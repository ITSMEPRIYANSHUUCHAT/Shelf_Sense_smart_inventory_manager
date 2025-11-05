from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from scripts.batch_ingest import batch_ingest_to_lake
from scripts.stream_ingest import stream_ingest_to_hub
from scripts.optimize import optimize_with_prediction
from scripts.mongo_queries import query_insights
from scripts.data_quality import validate_data
from urllib.parse import quote_plus
import os, json, base64
from dotenv import load_dotenv
from pymongo import MongoClient

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
COLAB_ID = os.getenv('COLAB_NOTEBOOK_ID')

db = client['shelf_sense_db']

dag = DAG(
    'full_pipeline_dag',
    default_args={'owner': 'shelf_sense', 'start_date': datetime(2025, 11, 1)},
    schedule_interval='@daily',
    catchup=False
)

batch_task = PythonOperator(task_id='batch_ingest', python_callable=batch_ingest_to_lake, dag=dag)
stream_task = PythonOperator(task_id='stream_ingest', python_callable=stream_ingest_to_hub, dag=dag)

# Automate Colab: Download notebook, run headless with papermill, get result.b64
run_colab = BashOperator(
    task_id='run_colab_transform',
    bash_command=f"""
    curl -L -o input.ipynb "https://colab.research.google.com/drive/{COLAB_ID}?authuser=0#offline=true&kernel=python3" --silent
    papermill input.ipynb output.ipynb -p MONGO_URI "{MONGO_URI}"
    python -c 'import base64, json; open("result.b64","w").write(base64.b64encode(open("result.json","rb").read()).decode())'
    """,
    dag=dag
)

# Load Colab Result to MongoDB
def load_colab_result():
    with open("result.b64") as f:
        b64 = f.read().strip()
    data = json.loads(base64.b64decode(b64))
    db['fact_inventory'].insert_many(data['fact'])
    print("Colab transforms loaded to MongoDB")

load_task = PythonOperator(task_id='load_colab_to_mongo', python_callable=load_colab_result, dag=dag)

optimize_task = PythonOperator(task_id='optimize_with_prediction', python_callable=optimize_with_prediction, dag=dag)
query_task = PythonOperator(task_id='query_insights', python_callable=query_insights, dag=dag)
quality_task = PythonOperator(task_id='data_quality', python_callable=validate_data, dag=dag)

batch_task >> stream_task >> run_colab >> load_task >> optimize_task >> query_task >> quality_task