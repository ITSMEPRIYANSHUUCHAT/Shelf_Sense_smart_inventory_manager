from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from scripts.batch_ingest import batch_ingest_to_lake
from scripts.stream_ingest import stream_ingest_to_hub
import os, json, base64, pymongo
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
client = pymongo.MongoClient(MONGO_URI)
db = client['shelf_sense_db']

dag = DAG(
    'full_pipeline_dag',
    default_args={'owner': 'shelf_sense'},
    start_date=datetime(2025, 11, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['shelf-sense']
)

# 1. Batch & Stream
batch = PythonOperator(task_id='batch', python_callable=batch_ingest_to_lake, dag=dag)
stream = PythonOperator(task_id='stream', python_callable=stream_ingest_to_hub, dag=dag)

# 2. Run Colab (headless) → returns result.b64
run_colab = BashOperator(
    task_id='run_colab',
    bash_command="""
    cd /tmp
    curl -L -o notebook.ipynb "https://colab.research.google.com/drive/1-25BzDFnP2mgEAiEAPr-R-CEIL0rCrOI?authuser=0#offline=true&kernel=python3" --silent
    pip install papermill nbconvert pymongo python-dotenv pyspark -q
    papermill notebook.ipynb executed.ipynb -p MONGO_URI "{{ var.value.MONGO_URI }}"
    python -c "import base64, json, os; \
        open('result.b64','wb').write(base64.b64encode(open('result.json','rb').read()))"
    """,
    dag=dag
)

# 3. Download result & push to MongoDB
def load_result(**kwargs):
    b64 = open("/tmp/result.b64").read().strip()
    data = json.loads(base64.b64decode(b64))
    db.fact_inventory.delete_many({})
    db.fact_inventory.insert_many(data['fact'])
    db.optimized_reorders.delete_many({})
    db.optimized_reorders.insert_many(data['optimized'])
    print("Colab results → MongoDB")

push_to_mongo = PythonOperator(
    task_id='push_to_mongo',
    python_callable=load_result,
    dag=dag
)

# 4. Refresh Streamlit (optional)
refresh_ui = BashOperator(
    task_id='refresh_ui',
    bash_command="curl -X POST http://localhost:8501/_stcore/refresh || true",
    dag=dag
)

# ORDER
batch >> stream >> run_colab >> push_to_mongo >> refresh_ui