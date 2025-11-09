import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')

def load_from_drive():
    client = MongoClient(MONGO_URI)
    db = client['shelf_sense_db']

    with open('/content/result.json', 'r') as f:
        data = json.load(f)

    db['fact_inventory'].insert_many(data['fact'])
    db['dim_products'].insert_many(data['dim_products'])
    db['dim_dates'].insert_many(data['dim_dates'])
    print("Loaded from Drive to MongoDB")

if __name__ == "__main__":
    load_from_drive()