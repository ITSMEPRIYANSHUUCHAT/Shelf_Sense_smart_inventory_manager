from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from scripts.preprocess_data import preprocess_data  # NEW: Preprocess task
from scripts.batch_ingest import batch_ingest_to_lake
from scripts.stream_ingest import stream_ingest_to_hub
from scripts.upload_to_drive import upload_to_drive
from scripts.optimize import optimize_with_prediction
from scripts.mongo_queries import query_insights
from scripts.data_quality import validate_data
from dotenv import load_dotenv
from pymongo import MongoClient
import os
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
client = MongoClient(MONGO_URI)
db = client['shelf_sense_db']

dag = DAG(
    'full_pipeline_dag',
    default_args={'owner': 'shelf_sense', 'start_date': datetime(2025, 11, 1)},
    schedule_interval='@daily',
    catchup=False
)

# NEW: Preprocess Task (Kicks Off with Raw to Processed)
preprocess_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,  # Your preprocess function
    dag=dag
)

batch_task = PythonOperator(task_id='batch_ingest', python_callable=batch_ingest_to_lake, dag=dag)
stream_task = PythonOperator(task_id='stream_ingest', python_callable=stream_ingest_to_hub, dag=dag)

upload_task = PythonOperator(task_id='upload_to_drive', python_callable=upload_to_drive, dag=dag)

# Poll for Flag: Waits for Manual Colab Run
poll_flag = BashOperator(
    task_id='poll_for_manual_notebook',
    bash_command="""
    cd /tmp
    for i in {1..10}; do
        if curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://www.googleapis.com/drive/v3/files?q='$(DRIVE_FOLDER_ID)' in parents and name='ready.txt'" | grep -q "ready.txt"; then
            echo "Manual Colab complete—flag detected, proceeding"
            break
        else
            echo "Waiting for manual Colab run... ($i/10, ~30s each)"
            sleep 30
        fi
    done
    """,
    dag=dag
)

optimize_task = PythonOperator(task_id='optimize_with_prediction', python_callable=optimize_with_prediction, dag=dag)
query_task = PythonOperator(task_id='query_insights', python_callable=query_insights, dag=dag)
quality_task = PythonOperator(task_id='data_quality', python_callable=validate_data, dag=dag)

# Flow: Preprocess → Batch → Stream → Upload → Poll for Manual Colab → Optimize → Query → Quality
preprocess_task >> batch_task >> stream_task >> upload_task >> poll_flag >> optimize_task >> query_task >> quality_task