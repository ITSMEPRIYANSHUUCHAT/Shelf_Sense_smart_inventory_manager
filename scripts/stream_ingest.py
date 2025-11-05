import os
import json
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
from urllib.parse import quote_plus

load_dotenv()

EVENT_HUB_CONN_STR = os.getenv('EVENT_HUB_CONN_STR')
LOCAL_PROCESSED_DIR = 'data/processed'

def stream_ingest_to_hub():
    producer = EventHubProducerClient.from_connection_string(EVENT_HUB_CONN_STR, eventhub_name="inventory-stream")

    # Find and send inventory chunks
    chunks = [f for f in os.listdir(LOCAL_PROCESSED_DIR) if f.startswith('inventory_chunk_')]
    for chunk in chunks:
        local_path = os.path.join(LOCAL_PROCESSED_DIR, chunk)
        with open(local_path, 'r') as f:
            data = f.read()  # Or load as JSON if converted
            event = EventData(json.dumps({"file": chunk, "data": data}))  # Send as JSON
            with producer:
                batch = producer.create_batch()
                batch.add(event)
                producer.send_batch(batch)
            print(f"Streamed {chunk} to Event Hubs")

# For testing: stream_ingest_to_hub()