import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from pymongo import MongoClient
import pandas as pd

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
client = MongoClient(MONGO_URI)
db = client['shelf_sense_db']

def query_insights():
    fact_pd = pd.DataFrame(list(db['fact_inventory'].find()))

    if fact_pd.empty:
        print("No data for queries—run transform first.")
        return

    # Query: Low stock alerts with risk
    alerts = fact_pd[(fact_pd['Stock_Quantity'] < 50) & (fact_pd['predicted_waste_risk'] == 'High')]

    print("High-risk low stock alerts:")
    print(alerts[['Product_ID', 'Stock_Quantity', 'predicted_waste_risk']])

    # FIXED: Clear collection before insert (avoids duplicates)
    db['alerts'].delete_many({})

    if not alerts.empty:
        db['alerts'].insert_many(alerts.to_dict('records'))
        print(f"Inserted {len(alerts)} alerts to MongoDB")
    else:
        print("No alerts generated.")

if __name__ == "__main__":
    query_insights()