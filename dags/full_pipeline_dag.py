# file: dags/full_pipeline_dag.py
import os
import json
import base64
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from pymongo import MongoClient
from dotenv import load_dotenv

# load env if using a .env on the worker (optional)
load_dotenv()

# Use Airflow Variable for secrets (recommended)
# Set this in the Airflow UI: Admin -> Variables -> key=MONGO_URI, value="mongodb://user:pass@host:port"
MONGO_URI = Variable.get("MONGO_URI", default_var=os.getenv("MONGO_URI", ""))

# Raw notebook URL (must be raw.githubusercontent.com)
COLAB_RAW_NOTEBOOK = (
    "https://raw.githubusercontent.com/"
    "ITSMEPRIYANSHUUCHAT/Shelf_Sense_smart_inventory_manager/"
    "main/notebooks/ShelfTransform.ipynb"
)

# Mongo client for PythonOperators
client = MongoClient(MONGO_URI) if MONGO_URI else None
db = client['shelf_sense_db'] if client else None

default_args = {
    "owner": "shelf_sense",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 11, 1),
}

dag = DAG(
    "full_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)

# --- Tasks ---

batch_task = PythonOperator(
    task_id="batch_ingest",
    python_callable=lambda: print("batch_ingest placeholder"),
    dag=dag,
)

skip_stream = DummyOperator(
    task_id="skip_stream_ingest",
    dag=dag,
)

# Hardened bash operator that:
# - downloads the raw .ipynb
# - checks JSON validity
# - runs papermill with MONGO_URI from env
# - verifies result.json exists
# - encodes result.json -> result.b64
run_colab = BashOperator(
    task_id="run_colab_transform",
    bash_command=f"""
set -euo pipefail
cd /tmp

echo "Downloading notebook from raw URL..."
curl --fail -L -o input.ipynb "{COLAB_RAW_NOTEBOOK}"

echo "Sanity-checking notebook JSON..."
python - <<'PY'
import sys, json
p = "input.ipynb"
try:
    with open(p, 'r', encoding='utf-8') as f:
        json.load(f)
except Exception as e:
    print("Notebook is not valid JSON or couldn't be loaded:", e, file=sys.stderr)
    sys.exit(2)
print("Notebook JSON OK")
PY

# Ensure MONGO_URI is provided at runtime
if [ -z "${{MONGO_URI:-}}" ]; then
  echo "ERROR: MONGO_URI environment variable is empty. Provide it via Airflow Variable or worker env." >&2
  exit 3
fi

echo "Running papermill..."
papermill input.ipynb output.ipynb -p MONGO_URI "$MONGO_URI"

# Expect the notebook to write result.json (your notebook must produce this file)
if [ ! -f result.json ]; then
  echo "ERROR: result.json not found after papermill run. Check notebook cells that write result.json" >&2
  ls -al
  exit 4
fi

echo "Encoding result.json to result.b64..."
python - <<'PY'
import sys, json, base64
try:
    with open('result.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
except Exception as e:
    print("Failed to read/parse result.json:", e, file=sys.stderr)
    sys.exit(5)
b64 = base64.b64encode(json.dumps(data).encode()).decode()
with open('result.b64','w', encoding='utf-8') as f:
    f.write(b64)
print("Wrote result.b64")
PY
""",
    env={"MONGO_URI": MONGO_URI},
    dag=dag,
    retries=1,
)

def load_colab_result(**context):
    import os, json, base64
    b64_path = "/tmp/result.b64"
    if not os.path.exists(b64_path):
        raise FileNotFoundError(f"{b64_path} not found — papermill step likely failed.")
    with open(b64_path, "r", encoding="utf-8") as f:
        b64 = f.read().strip()
    try:
        decoded = base64.b64decode(b64)
        data = json.loads(decoded)
    except Exception as e:
        raise RuntimeError("Failed to decode/parse result.b64: " + str(e))

    # Basic schema checks
    if not isinstance(data, dict) or 'fact' not in data or 'optimized' not in data:
        raise ValueError("Decoded data missing expected keys ('fact', 'optimized')")

    # Insert into MongoDB with safe checks
    if db is None:
        raise RuntimeError("MongoDB client not configured (MONGO_URI missing).")

    try:
        db['fact_inventory'].insert_many(data['fact'])
    except Exception as e:
        raise RuntimeError("Failed to insert fact_inventory: " + str(e))

    try:
        db['optimized_reorders'].insert_many(data['optimized'])
    except Exception as e:
        raise RuntimeError("Failed to insert optimized_reorders: " + str(e))

    print("Colab results loaded to MongoDB")

load_task = PythonOperator(
    task_id="load_colab_to_mongo",
    python_callable=load_colab_result,
    provide_context=True,
    dag=dag,
)

optimize_task = PythonOperator(
    task_id="optimize_with_prediction",
    python_callable=lambda: print("optimize placeholder"),
    dag=dag,
)

query_task = PythonOperator(
    task_id="query_insights",
    python_callable=lambda: print("query placeholder"),
    dag=dag,
)

quality_task = PythonOperator(
    task_id="data_quality",
    python_callable=lambda: print("quality placeholder"),
    dag=dag,
)

# Flow
batch_task >> skip_stream >> run_colab >> load_task >> optimize_task >> query_task >> quality_task
