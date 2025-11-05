import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load .env file
load_dotenv()

# Get keys from env vars (fallback to empty if not set)
DATA_LAKE_CONN_STR = os.getenv('DATA_LAKE_CONN_STR', '')
EVENT_HUB_CONN_STR = os.getenv('EVENT_HUB_CONN_STR', '')

# Rest of your script remains the same...
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
# Test 1: Data Lake Storage (Upload a dummy file to processed-data container)
try:
    blob_service_client = BlobServiceClient.from_connection_string(DATA_LAKE_CONN_STR)
    container_client = blob_service_client.get_container_client("processed-data")
    blob_client = container_client.get_blob_client("test_upload.txt")
    blob_client.upload_blob("This is a test upload from local script!", overwrite=True)
    print("Data Lake connected successfully!")
except Exception as e:
    print(f"Data Lake error: {e}")

# Test 2: Event Hubs (Send a dummy event to inventory-stream)
try:
    producer_client = EventHubProducerClient.from_connection_string(EVENT_HUB_CONN_STR, eventhub_name="inventory-stream")
    with producer_client:
        event_batch = producer_client.create_batch()
        event_batch.add(EventData('{"test_key": "test_value"}')) 
        producer_client.send_batch(event_batch)
    print("Event Hubs connected successfully!")
except EventHubError as e:
    print(f"Event Hubs error: {e}")
except Exception as e:
    print(f"General error: {e}")