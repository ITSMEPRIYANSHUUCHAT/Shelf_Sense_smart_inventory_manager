import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

DATA_LAKE_CONN_STR = os.getenv('DATA_LAKE_CONN_STR')
LOCAL_PROCESSED_DIR = 'data/processed'  # Your local folder

def batch_ingest_to_lake():
    blob_service = BlobServiceClient.from_connection_string(DATA_LAKE_CONN_STR)
    container = blob_service.get_container_client('processed-data')

    # Upload specific batch files
    batch_files = ['sales_perishables.csv', 'weather_daily_clean.csv', 'promotions_weekly.csv']
    for file in batch_files:
        local_path = os.path.join(LOCAL_PROCESSED_DIR, file)
        if os.path.exists(local_path):
            blob = container.get_blob_client(file)
            with open(local_path, 'rb') as f:
                blob.upload_blob(f, overwrite=True)
            print(f"Uploaded {file} to Data Lake")
        else:
            print(f"File not found: {local_path}")

# For testing: batch_ingest_to_lake()