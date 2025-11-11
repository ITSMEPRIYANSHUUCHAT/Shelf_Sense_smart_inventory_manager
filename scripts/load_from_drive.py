#!/usr/bin/env python3
import os
import json
import logging
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "shelf_sense_db")
INPUT_PATH = os.getenv("DRIVE_DATA_DIR", "/opt/airflow/data") + "/result.json"

def load_from_drive():
    if not MONGO_URI:
        log.error("MONGO_URI not set in environment.")
        return

    if not os.path.exists(INPUT_PATH):
        log.error("Result file not found at %s", INPUT_PATH)
        return

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    with open(INPUT_PATH, "r") as f:
        payload = json.load(f)

    # Validate keys exist
    fact = payload.get("fact", [])
    dim_products = payload.get("dim_products", [])
    dim_dates = payload.get("dim_dates", [])

    if fact:
        db["fact_inventory"].insert_many(fact)
        log.info("Inserted %d fact records", len(fact))
    else:
        log.info("No 'fact' records to insert")

    if dim_products:
        db["dim_products"].insert_many(dim_products)
        log.info("Inserted %d dim_products", len(dim_products))

    if dim_dates:
        db["dim_dates"].insert_many(dim_dates)
        log.info("Inserted %d dim_dates", len(dim_dates))

    log.info("Load complete.")

if __name__ == "__main__":
    load_from_drive()
