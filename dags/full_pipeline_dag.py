# full_pipeline_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# existing imports
from scripts.batch_ingest import batch_ingest_to_lake
from scripts.stream_ingest import stream_ingest_to_hub
from scripts.upload_to_drive import upload_to_drive
from scripts.optimize import optimize_with_prediction
from scripts.mongo_queries import query_insights
from scripts.data_quality import validate_data
from scripts.check_flag import check_flag

# ✅ import your check_flag() from the file you already have

from dotenv import load_dotenv
load_dotenv()

# ------------------ DAG Definition ------------------
default_args = {
    'owner': 'shelf_sense',
    'start_date': datetime(2025, 11, 1)
}

dag = DAG(
    'full_pipeline_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# ------------------ Tasks ------------------

batch_task = PythonOperator(
    task_id='batch_ingest',
    python_callable=batch_ingest_to_lake,
    dag=dag
)

stream_task = PythonOperator(
    task_id='stream_ingest',
    python_callable=stream_ingest_to_hub,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_drive',
    python_callable=upload_to_drive,
    dag=dag
)

# ✅ New: use check_flag directly as PythonOperator
poll_flag = PythonOperator(
    task_id='poll_for_manual_notebook',
    python_callable=check_flag,
    dag=dag
)

optimize_task = PythonOperator(
    task_id='optimize_with_prediction',
    python_callable=optimize_with_prediction,
    dag=dag
)

query_task = PythonOperator(
    task_id='query_insights',
    python_callable=query_insights,
    dag=dag
)

quality_task = PythonOperator(
    task_id='data_quality',
    python_callable=validate_data,
    dag=dag
)

# ------------------ Task Flow ------------------
batch_task >> stream_task >> upload_task >> poll_flag >> optimize_task >> query_task >> quality_task
