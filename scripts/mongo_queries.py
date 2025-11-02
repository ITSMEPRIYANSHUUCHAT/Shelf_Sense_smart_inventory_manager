import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')

def query_insights():
    client = MongoClient(MONGO_URI)
    db = client['shelf_sense_db']
    fact_pd = pd.DataFrame(list(db['fact_inventory'].find()))

    # Query: Low stock alerts with risk
    alerts = fact_pd[(fact_pd['Stock_Quantity'] < 50) & (fact_pd['predicted_waste_risk'] == 'High')]

    print("High-risk low stock alerts:")
    print(alerts[['Product_ID', 'Stock_Quantity', 'predicted_waste_risk']])

    # Save alerts to DB
    db['alerts'].insert_many(alerts.to_dict('records'))

if __name__ == "__main__":
    query_insights()