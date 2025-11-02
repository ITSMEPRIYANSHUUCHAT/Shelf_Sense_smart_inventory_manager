from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello from Airflow DAG!")

dag = DAG(
    'test_dag',
    default_args={'owner': 'shelf_sense', 'start_date': datetime(2025, 11, 1)},
    schedule_interval=None,  # Manual trigger
    catchup=False
)

task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag
)

task