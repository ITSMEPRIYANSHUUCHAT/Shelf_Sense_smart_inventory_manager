from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.batch_ingest import batch_ingest_to_lake
from scripts.stream_ingest import stream_ingest_to_hub

dag = DAG(
    'ingestion_dag',
    default_args={'owner': 'shelf_sense', 'start_date': datetime(2025, 11, 1)},
    schedule_interval='@daily',  # Or None for manual
    catchup=False
)

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

optimize_task = PythonOperator(
    task_id='optimize_with_prediction',
    python_callable=optimize_with_prediction,
    dag=dag
)

stream_task >> optimize_task

batch_task >> stream_task  # Run batch first