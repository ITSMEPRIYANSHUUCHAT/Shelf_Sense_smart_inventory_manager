import os
import glob
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pymongo import MongoClient
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
LOCAL_PROCESSED_DIR = '/opt/airflow/data/processed'

def transform_data():
    spark = SparkSession.builder.appName("ShelfTransform").getOrCreate()

    # Load
    sales_df = spark.read.csv(os.path.join(LOCAL_PROCESSED_DIR, 'sales_perishables.csv'), header=True, inferSchema=True)
    inv_files = glob.glob(os.path.join(LOCAL_PROCESSED_DIR, 'inventory_chunk_*.csv'))
    inventory_df = spark.read.csv(inv_files, header=True, inferSchema=True)
    weather_df = spark.read.csv(os.path.join(LOCAL_PROCESSED_DIR, 'weather_daily_clean.csv'), header=True, inferSchema=True)
    promo_df = spark.read_csv(os.path.join(LOCAL_PROCESSED_DIR, 'promotions_weekly.csv'), header=True, inferSchema=True)

    # Clean dates
    sales_df = sales_df.withColumn('Date_Received', to_date(col('Date_Received'), 'MM/dd/yyyy'))
    inventory_df = inventory_df.withColumn('Date', to_date(col('Date'), 'yyyy-MM-dd'))
    weather_df = weather_df.withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))

    # FIXED: Bracket notation for joins, check columns
    sales_df = sales_df.withColumnRenamed('Product ID', 'Product_ID') if 'Product ID' in [c.lower() for c in sales_df.columns] else sales_df
    inventory_df = inventory_df.withColumnRenamed('Product ID', 'Product_ID') if 'Product ID' in [c.lower() for c in inventory_df.columns] else inventory_df

    joined_df = sales_df.join(inventory_df, sales_df["Product_ID"] == inventory_df["Product_ID"], 'inner') \
                        .join(weather_df, sales_df["Date_Received"] == weather_df["date"], 'left') \
                        .select('Product_ID', 'Category', 'Stock_Quantity', 'Units_Sold', 'meantemp', 'humidity', 'Date_Received')

    # Star Schema
    fact_inventory = joined_df.select('Product_ID', 'Date_Received', 'Stock_Quantity', 'Units_Sold', col('meantemp').alias('Weather_Temp'), col('humidity').alias('Weather_Humidity'))
    dim_products = sales_df.select('Product_ID', 'Product_Name', 'Category', 'Unit_Price').distinct()
    dim_dates = fact_inventory.select('Date_Received').distinct().withColumnRenamed('Date_Received', 'Date')

    # Output to MongoDB
    client = MongoClient(MONGO_URI)
    db = client['shelf_sense_db']
    db['fact_inventory'].insert_many(fact_inventory.toPandas().to_dict('records'))
    db['dim_products'].insert_many(dim_products.toPandas().to_dict('records'))
    db['dim_dates'].insert_many(dim_dates.toPandas().to_dict('records'))
    print("Transform complete - Star schema loaded to MongoDB")

    spark.stop()

if __name__ == "__main__":
    transform_data()