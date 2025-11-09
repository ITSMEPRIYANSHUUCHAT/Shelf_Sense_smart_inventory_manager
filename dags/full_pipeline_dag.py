from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from scripts.batch_ingest import batch_ingest_to_lake
from scripts.stream_ingest import stream_ingest_to_hub
from scripts.upload_to_drive import upload_to_drive
from scripts.download_from_drive import download_from_drive
from scripts.load_from_drive import load_from_drive
from scripts.optimize import optimize_with_prediction
from scripts.mongo_queries import query_insights
from scripts.data_quality import validate_data
from dotenv import load_dotenv
from pymongo import MongoClient
import os
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
# FIXED: Raw GitHub URL (not blob)
COLAB_NOTEBOOK_URL = 'https://raw.githubusercontent.com/ITSMEPRIYANSHUUCHAT/Shelf_Sense_smart_inventory_manager/main/notebooks/ShelfTransform.ipynb'
client = MongoClient(MONGO_URI)
db = client['shelf_sense_db']

dag = DAG(
    'full_pipeline_dag',
    default_args={'owner': 'shelf_sense', 'start_date': datetime(2025, 11, 1)},
    schedule_interval='@daily',
    catchup=False
)

batch_task = PythonOperator(task_id='batch_ingest', python_callable=batch_ingest_to_lake, dag=dag)
stream_task = PythonOperator(task_id='stream_ingest', python_callable=stream_ingest_to_hub, dag=dag)

upload_task = PythonOperator(task_id='upload_to_drive', python_callable=upload_to_drive, dag=dag)

# FIXED: Raw URL + better error handling in bash
run_colab = BashOperator(
    task_id='run_colab_transform',
    bash_command=f"""
    cd /tmp
    export MONGO_URI="{MONGO_URI}"
    curl -L -o input.ipynb "{COLAB_NOTEBOOK_URL}"
    if [ ! -s input.ipynb ]; then
        echo "Error: Notebook download failed or empty—check URL"
        exit 1
    fi
    papermill input.ipynb output.ipynb -p MONGO_URI "$MONGO_URI"
    if [ -f result.json ]; then
        echo "result.json created—encoding to b64"
        python -c "import json, base64; with open('result.json', 'r') as f: data = json.load(f); b64 = base64.b64encode(json.dumps(data).encode()).decode(); open('result.b64', 'w').write(b64)"
    else
        echo "Error: result.json not created by notebook—check output cell"
        ls -la /tmp | grep json
        exit 1
    fi
    echo "Colab run complete—result.json ready"
    """,
    dag=dag
)

download_task = PythonOperator(task_id='download_from_drive', python_callable=download_from_drive, dag=dag)

load_task = PythonOperator(
    task_id='load_to_mongo',
    python_callable=load_from_drive,
    dag=dag
)

optimize_task = PythonOperator(task_id='optimize_with_prediction', python_callable=optimize_with_prediction, dag=dag)
query_task = PythonOperator(task_id='query_insights', python_callable=query_insights, dag=dag)
quality_task = PythonOperator(task_id='data_quality', python_callable=validate_data, dag=dag)

batch_task >> stream_task >> upload_task >> run_colab >> download_task >> load_task >> optimize_task >> query_task >> quality_task