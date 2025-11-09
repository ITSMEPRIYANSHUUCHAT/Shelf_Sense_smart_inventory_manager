import os
import glob
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
LOCAL_PROCESSED_DIR = '/opt/airflow/data/processed'  # Container path

def transform_data():
    # Load data with Pandas
    sales_df = pd.read_csv(os.path.join(LOCAL_PROCESSED_DIR, 'sales_perishables.csv'))
    inv_files = glob.glob(os.path.join(LOCAL_PROCESSED_DIR, 'inventory_chunk_*.csv'))
    inv_dfs = [pd.read_csv(f) for f in inv_files if f]
    inventory_df = pd.concat(inv_dfs, ignore_index=True) if inv_dfs else pd.DataFrame()
    weather_df = pd.read_csv(os.path.join(LOCAL_PROCESSED_DIR, 'weather_daily_clean.csv'))
    promo_df = pd.read_csv(os.path.join(LOCAL_PROCESSED_DIR, 'promotions_weekly.csv'))

    # Clean dates
    sales_df['Date_Received'] = pd.to_datetime(sales_df['Date_Received'], errors='coerce')
    inventory_df['Date'] = pd.to_datetime(inventory_df['Date'], errors='coerce')
    weather_df['date'] = pd.to_datetime(weather_df['date'], errors='coerce')

    # Join: Sales + Inventory on Product_ID, + Weather on date
    joined_df = pd.merge(sales_df, inventory_df, left_on='Product_ID', right_on='Product_ID', how='inner')
    joined_df = pd.merge(joined_df, weather_df, left_on='Date_Received', right_on='date', how='left')

    # Select key columns
    joined_df = joined_df[['Product_ID', 'Category', 'Stock_Quantity', 'Units_Sold', 'meantemp', 'humidity', 'Date_Received']]

    # Star Schema
    fact_inventory = joined_df[['Product_ID', 'Date_Received', 'Stock_Quantity', 'Units_Sold', 'meantemp', 'humidity']].copy()
    fact_inventory.rename(columns={'meantemp': 'Weather_Temp', 'humidity': 'Weather_Humidity'}, inplace=True)

    dim_products = sales_df[['Product_ID', 'Product_Name', 'Category', 'Unit_Price']].drop_duplicates()
    dim_dates = fact_inventory[['Date_Received']].drop_duplicates()
    dim_dates.rename(columns={'Date_Received': 'Date'}, inplace=True)

    # Direct to MongoDB
    client = MongoClient(MONGO_URI)
    db = client['shelf_sense_db']
    db['fact_inventory'].insert_many(fact_inventory.to_dict('records'))
    db['dim_products'].insert_many(dim_products.to_dict('records'))
    db['dim_dates'].insert_many(dim_dates.to_dict('records'))
    print("Transform complete: Star schema loaded directly to MongoDB")

if __name__ == "__main__":
    transform_data()