import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pulp
import pandas as pd
from urllib.parse import quote_plus

load_dotenv()
# Escape username/password
user = os.getenv('MONGO_USER')  # e.g., mongouser
password = os.getenv('MONGO_PASS')  # e.g., mongopass
cluster = os.getenv('MONGO_CLUSTER')  # e.g., cluster0.abcde.mongodb.net

if not all([user, password, cluster]):
    raise ValueError("Missing MongoDB credentials in .env")

username = quote_plus(user)
pwd = quote_plus(password)
MONGO_URI = f"mongodb+srv://{username}:{pwd}@{cluster}?retryWrites=true&w=majority"

client = MongoClient(MONGO_URI)

def optimize_with_prediction():
    client = MongoClient(MONGO_URI)
    db = client['shelf_sense_db']
    fact_pd = pd.DataFrame(list(db['fact_inventory'].find()))

    # Mock prediction column
    fact_pd['predicted_waste_risk'] = 'Low'
    fact_pd.loc[(fact_pd['Weather_Temp'] > 25) & (fact_pd['Weather_Humidity'] > 80), 'predicted_waste_risk'] = 'High'
    fact_pd.loc[(fact_pd['Weather_Temp'] > 20) & (fact_pd['Weather_Humidity'] > 70) & (fact_pd['predicted_waste_risk'] != 'High'), 'predicted_waste_risk'] = 'Medium'

    # Group and optimize (as above)
    grouped = fact_pd.groupby('Product_ID').agg({
        'Stock_Quantity': 'mean',
        'Units_Sold': 'mean',
        'Weather_Temp': 'mean'
    }).reset_index()

    prob = pulp.LpProblem("ReorderOptWithPrediction", pulp.LpMinimize)
    products = grouped['Product_ID'].tolist()
    reorder_qty = pulp.LpVariable.dicts("Reorder", products, lowBound=0, cat='Integer')

    prob += pulp.lpSum([reorder_qty[p] * grouped[grouped['Product_ID'] == p]['Weather_Temp'].values[0] for p in products])

    for p in products:
        row = grouped[grouped['Product_ID'] == p]
        current_stock = row['Stock_Quantity'].values[0]
        demand = row['Units_Sold'].values[0]
        prob += reorder_qty[p] + current_stock >= demand, f"Demand_{p}"

    prob.solve()

    reorders = {p: pulp.value(reorder_qty[p]) for p in products}
    grouped['recommended_reorder'] = grouped['Product_ID'].map(reorders)

    # Save back to MongoDB
    db['optimized_reorders'].insert_many(grouped.to_dict('records'))
    print("Optimized table with mock prediction saved to MongoDB")

if __name__ == "__main__":
    optimize_with_prediction()