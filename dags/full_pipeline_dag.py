from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator  # For skipping ingest
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
COLAB_NOTEBOOK_URL = 'https://raw.githubusercontent.com/ITSMEPRIYANSHUUCHAT/Shelf_Sense_smart_inventory_manager/main/notebooks/ShelfTransform.ipynb'
client = MongoClient(MONGO_URI)
db = client['shelf_sense_db']

dag = DAG(
    'full_pipeline_dag',
    default_args={'owner': 'shelf_sense', 'start_date': datetime(2025, 11, 1)},
    schedule_interval='@daily',
    catchup=False
)

# Skip Batch and Stream: Dummy tasks
skip_batch = DummyOperator(task_id='skip_batch_ingest', dag=dag)
skip_stream = DummyOperator(task_id='skip_stream_ingest', dag=dag)

upload_task = PythonOperator(task_id='upload_to_drive', python_callable=upload_to_drive, dag=dag)

run_colab = BashOperator(
    task_id='run_colab_transform',
    bash_command=f"""
    cd /tmp
    export MONGO_URI="{MONGO_URI}"
    curl -L -o input.ipynb "{COLAB_NOTEBOOK_URL}"
    papermill input.ipynb output.ipynb -p MONGO_URI "$MONGO_URI"
    echo "Colab run complete—result.json in shelfstorage"
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

# Flow: Skip Batch/Stream → Upload → Run Colab → Download → Load → Optimize → Query → Quality
skip_batch >> skip_stream >> upload_task >> run_colab >> download_task >> load_task >> optimize_task >> query_task >> quality_task