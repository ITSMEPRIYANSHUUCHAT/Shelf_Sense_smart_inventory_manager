import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from urllib.parse import quote_plus

load_dotenv()
# Escape username/password
user = os.getenv('MONGO_USER')  # e.g., mongouser
password = os.getenv('MONGO_PASS')  # e.g., mongopass
cluster = os.getenv('MONGO_CLUSTER')  # e.g., cluster0.abcde.mongodb.net

if not all([user, password, cluster]):
    raise ValueError("Missing MongoDB credentials in .env")

username = quote_plus(user)
pwd = quote_plus(password)
MONGO_URI = f"mongodb+srv://{username}:{pwd}@{cluster}?retryWrites=true&w=majority"

client = MongoClient(MONGO_URI)
def validate_data():
    client = MongoClient(MONGO_URI)
    db = client['shelf_sense_db']
    fact_pd = pd.DataFrame(list(db['fact_inventory'].find()))

    context = ge.get_context()
    datasource = context.add_or_update_pandas_datasource(name="mongo_data")
    asset = datasource.add_dataframe_asset(name="fact_asset", dataframe=fact_pd)

    suite = asset.add_expectation_suite("fact_suite")
    suite.add_expectation(ge.expectations.ExpectColumnToExist(column="Product_ID"))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToNotBeNull(column="Stock_Quantity"))

    checkpoint = SimpleCheckpoint(name="fact_checkpoint", data_context=context, validations=[{"batch_request": asset.build_batch_request(), "expectation_suite_name": "fact_suite"}])
    results = context.run_checkpoint(checkpoint_name="fact_checkpoint")

    print("Validation success:", results.success)

if __name__ == "__main__":
    validate_data()