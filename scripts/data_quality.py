# scripts/data_quality.py
import os
import traceback
import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# Mongo connection (expects MONGO_URI in env)
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI missing in environment")

client = MongoClient(MONGO_URI)
db = client.get_database("shelf_sense_db")

# CONFIG
DATASOURCE_NAME = "my_pandas_runtime_datasource"
DATA_CONNECTOR_NAME = "default_runtime_data_connector_name"
SUITE_NAME = "fact_suite"
DATA_ASSET_NAME = "fact_inventory_runtime_asset"

def ensure_pandas_runtime_datasource(context, datasource_name=DATASOURCE_NAME):
    """
    If the DataContext has no matching datasource, register a minimal
    pandas RuntimeDataConnector datasource programmatically.
    """
    try:
        existing = [ds["name"] for ds in context.list_datasources()]
        print("GE datasources seen by context:", existing)
    except Exception as e:
        # If listing fails, print and continue - will attempt to register anyway
        print("Warning: failed to list datasources:", e)
        existing = []

    if datasource_name in existing:
        print(f"Datasource '{datasource_name}' already exists — skipping registration.")
        return

    # minimal YAML config for registering a pandas runtime datasource
    yaml_config = f"""
name: {datasource_name}
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  {DATA_CONNECTOR_NAME}:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
"""

    try:
        print("Registering pandas runtime datasource via context.test_yaml_config(...)")
        test_result = context.test_yaml_config(yaml_config)
        print("test_yaml_config result:", test_result)
    except Exception as e:
        # Some GE versions may behave differently; show full error for debugging
        print("Failed to register datasource programmatically. Error:")
        traceback.print_exc()
        raise

def validate_data():
    """
    Loads fact_inventory from MongoDB, ensures GE datasource exists,
    validates df using RuntimeBatchRequest -> validator.
    Returns True/False.
    """
    try:
        fact_pd = pd.DataFrame(list(db["fact_inventory"].find()))
        print(f"Loaded fact_inventory rows: {len(fact_pd)}")
    except Exception:
        print("Failed to read from MongoDB 'fact_inventory' collection:")
        traceback.print_exc()
        return False

    if fact_pd.empty:
        print("No data for validation—fact_inventory is empty. (Run transform/load step first.)")
        return True  # Not an error for pipeline; adjust if you want to fail instead

    try:
        # Get context (will be ephemeral if no repo present)
        context = ge.get_context()
        print("GE context root_directory:", getattr(context, "root_directory", "N/A"))
        # Debug: show datasources before ensuring
        try:
            print("Datasources before ensure:", [ds["name"] for ds in context.list_datasources()])
        except Exception as e:
            print("Could not list datasources before ensure:", e)

        # Ensure the runtime datasource exists (register if not)
        ensure_pandas_runtime_datasource(context, datasource_name=DATASOURCE_NAME)

        # Ensure expectation suite exists (overwrite to be idempotent)
        try:
            context.create_expectation_suite(SUITE_NAME, overwrite_existing=True)
            print(f"Created/overwrote expectation suite: {SUITE_NAME}")
        except Exception as e:
            print("Warning: could not create expectation suite via context; continuing. Error:", e)

        # Build runtime batch request for in-memory DF
        batch_request = RuntimeBatchRequest(
            datasource_name=DATASOURCE_NAME,
            data_connector_name=DATA_CONNECTOR_NAME,
            data_asset_name=DATA_ASSET_NAME,
            runtime_parameters={"batch_data": fact_pd},
            batch_identifiers={"default_identifier_name": "fact_inventory_1"},
        )

        # Get validator and add expectations
        validator = context.get_validator(batch_request=batch_request, expectation_suite_name=SUITE_NAME)
        print("Validator acquired. Running expectations...")

        # Add/ensure expectations (idempotent)
        validator.expect_column_to_exist("Product_ID")
        validator.expect_column_values_to_not_be_null("Stock_Quantity")

        # Run validation
        results = validator.validate()
        success = results.get("success", False)
        print("Validation success:", success)

        if not success:
            # Log details (optional) then raise to fail the Airflow task
            print("DATA QUALITY: FAIL — raising RuntimeError to fail Airflow task")
            raise RuntimeError("Data quality checks failed: expectation suite did not pass.")
        else:
            print("DATA QUALITY: PASS")
        # Optionally save the suite
        try:
            validator.save_expectation_suite(discard_failed_expectations=False)
        except Exception as e:
            print("Could not save expectation suite (non-fatal):", e)
        # inside validate_data(), after running validation and obtaining `success` (bool)

        return bool(success)

    except AttributeError as ae:
        # Handle older/newer GE API differences; attempt fallback to ge.from_pandas
        print("AttributeError while using RuntimeBatchRequest/validator path:", ae)
        print("Attempting fallback using ge.from_pandas(...) for older GE versions.")
        try:
            df_ge = ge.from_pandas(fact_pd)
            r1 = df_ge.expect_column_to_exist("Product_ID")
            r2 = df_ge.expect_column_values_to_not_be_null("Stock_Quantity")
            ok = bool(r1.get("success", False) and r2.get("success", False))
            print("Fallback validation success:", ok)
            return ok
        except Exception:
            print("Fallback also failed. Full traceback:")
            traceback.print_exc()
            return False
    except Exception:
        print("Unexpected error during validation. Full traceback:")
        traceback.print_exc()
        return False

# If run as script (local debug)
if __name__ == "__main__":
    ok = validate_data()
    if ok:
        print("DATA QUALITY: PASS")
    else:
        print("DATA QUALITY: FAIL")
