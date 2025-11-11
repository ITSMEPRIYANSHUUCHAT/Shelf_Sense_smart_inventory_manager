

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# -----------------------
# CLI args & logging
# -----------------------
parser = argparse.ArgumentParser(description="Run ShelfTransform pipeline (Spark or Pandas fallback).")
parser.add_argument("--data-dir", default="data/processed", help="Path to processed data directory (default: data/processed)")
parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", ""), help="MongoDB URI (optional). Can also be set via MONGO_URI env var.")
parser.add_argument("--force-pandas", action="store_true", help="Force using pandas fallback even if pyspark is available.")
parser.add_argument("--result-file", default="result.json", help="Output result JSON file (default: result.json)")
args = parser.parse_args()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("run_pipeline")

DATA_DIR = Path(args.data_dir).resolve()
RESULT_FILE = Path(args.result_file).resolve()
MONGO_URI = args.mongo_uri

# -----------------------
# Utils
# -----------------------
def find_files_by_glob(d: Path, pattern: str):
    return list(d.glob(pattern))

# -----------------------
# Pandas fallback pipeline
# -----------------------
def run_pandas_pipeline(data_dir: Path):
    log.info("Running pandas fallback pipeline")
    try:
        import pandas as pd
    except Exception as e:
        log.exception("pandas not available")
        raise

    # find files
    sales_file = next(data_dir.glob("sales*.csv"), None)
    inventory_file = next(data_dir.glob("inventory*.csv"), None)
    weather_file = next(data_dir.glob("weather*.csv"), None)

    if not any([sales_file, inventory_file, weather_file]):
        log.warning("No CSV files found in %s", data_dir)
        # write minimal result.json so Airflow doesn't fail
        payload = {"status": "fallback", "fact": [], "dim_products": [], "dim_dates": [], "optimized": []}
        RESULT_FILE.write_text(json.dumps(payload, default=str))
        return payload

    log.info("Pandas loading files: sales=%s inventory=%s weather=%s", sales_file, inventory_file, weather_file)

    sales_df = pd.read_csv(sales_file) if sales_file else pd.DataFrame()
    inv_df = pd.read_csv(inventory_file) if inventory_file else pd.DataFrame()
    weather_df = pd.read_csv(weather_file) if weather_file else pd.DataFrame()

    # Normalize columns as best-effort
    if "Date" in inv_df.columns and "Date_Received" not in inv_df.columns:
        inv_df = inv_df.rename(columns={"Date": "Date_Received"})
    if "Product ID" in inv_df.columns and "Product_ID" not in inv_df.columns:
        inv_df = inv_df.rename(columns={"Product ID": "Product_ID"})
    if "Stock Quantity" in inv_df.columns and "Stock_Quantity" not in inv_df.columns:
        inv_df = inv_df.rename(columns={"Stock Quantity": "Stock_Quantity"})
    if "Units Sold" in sales_df.columns and "Units_Sold" not in sales_df.columns:
        sales_df = sales_df.rename(columns={"Units Sold": "Units_Sold"})

    # Merge strategy
    merged = None
    if "Product_ID" in sales_df.columns and "Product_ID" in inv_df.columns:
        log.info("Merging on Product_ID")
        merged = sales_df.merge(inv_df, on="Product_ID", how="inner")
    else:
        log.info("Product_ID missing — using index alignment fallback")
        n = min(len(sales_df), len(inv_df))
        if n > 0:
            merged = pd.concat([sales_df.head(n).reset_index(drop=True), inv_df.head(n).reset_index(drop=True)], axis=1)
        else:
            merged = pd.DataFrame({"status": ["fallback"], "message": ["no usable data found for merging"]})

    # merge weather if it has a date column
    if not weather_df.empty:
        # attempt to normalize date fields and merge where possible
        if "Date_Received" in merged.columns and "date" in weather_df.columns:
            try:
                merged["Date_Received"] = pd.to_datetime(merged["Date_Received"], errors="coerce")
                weather_df["date"] = pd.to_datetime(weather_df["date"], errors="coerce")
                merged = merged.merge(weather_df, left_on="Date_Received", right_on="date", how="left", suffixes=("","_weather"))
            except Exception:
                log.warning("Could not merge weather by date in pandas fallback")

    # predicted_waste_risk
    if "meantemp" in merged.columns and "humidity" in merged.columns:
        def risk(r):
            try:
                t = float(r.get("meantemp") or 0)
                h = float(r.get("humidity") or 0)
                if t > 25 and h > 80:
                    return "High"
                if t > 20 and h > 70:
                    return "Medium"
                return "Low"
            except Exception:
                return "Low"
        merged["predicted_waste_risk"] = merged.apply(risk, axis=1)
    else:
        merged["predicted_waste_risk"] = merged.apply(lambda r: "High" if (r.get("Stock_Quantity") is not None and float(r.get("Stock_Quantity") or 999) < 5) else "Low", axis=1)

    # grouped
    grouped = []
    if "Product_ID" in merged.columns and ("Units_Sold" in merged.columns or "Units Sold" in merged.columns):
        units_col = "Units_Sold" if "Units_Sold" in merged.columns else ("Units Sold" if "Units Sold" in merged.columns else None)
        if units_col:
            grouped_df = merged.groupby("Product_ID")[units_col].sum().reset_index().rename(columns={units_col: "Total_Units_Sold"})
            grouped = grouped_df.to_dict("records")

    # dims
    dim_products = []
    if "Product_ID" in sales_df.columns:
        cols = [c for c in ("Product_ID", "Product_Name", "Category", "Unit_Price") if c in sales_df.columns]
        if cols:
            dim_products = sales_df[cols].drop_duplicates().to_dict("records")

    dim_dates = []
    if "Date_Received" in merged.columns:
        merged["Date_Received"] = pd.to_datetime(merged["Date_Received"], errors="coerce")
        dim_dates = [{"Date": d.strftime("%Y-%m-%d")} for d in merged["Date_Received"].dropna().unique()]

    payload = {
        "status": "fallback",
        "fact": merged.fillna("").to_dict("records"),
        "dim_products": dim_products,
        "dim_dates": dim_dates,
        "optimized": grouped
    }

    RESULT_FILE.write_text(json.dumps(payload, default=str))
    log.info("WROTE %s (pandas fallback). Rows: %d", RESULT_FILE, len(payload["fact"]))
    return payload

# -----------------------
# Spark pipeline
# -----------------------
def run_spark_pipeline(data_dir: Path):
    log.info("Attempting Spark pipeline")
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, to_date, date_format, row_number, when
        from pyspark.sql.window import Window
    except Exception as e:
        log.exception("pyspark import failed")
        raise

    # start Spark session (assumes pyspark wheel or proper SPARK_HOME environment)
    try:
        spark = SparkSession.builder.master("local[*]").appName("ShelfTransform").config("spark.sql.legacy.timeParserPolicy","LEGACY").getOrCreate()
        log.info("Spark version: %s", spark.version)
    except Exception:
        log.exception("Failed starting SparkSession")
        raise

    sales_file = next(data_dir.glob("sales*.csv"), None)
    inventory_file = next(data_dir.glob("inventory*.csv"), None)
    weather_file = next(data_dir.glob("weather*.csv"), None)

    if not all([sales_file, inventory_file, weather_file]):
        raise FileNotFoundError(f"Missing one of sales/inventory/weather files in {data_dir}")

    log.info("Spark loading files: %s %s %s", sales_file.name, inventory_file.name, weather_file.name)
    sales_df = spark.read.option("header", "true").option("inferSchema","true").csv(str(sales_file))
    inventory_df = spark.read.option("header", "true").option("inferSchema","true").csv(str(inventory_file))
    weather_df = spark.read.option("header", "true").option("inferSchema","true").csv(str(weather_file))

    # Normalize
    sales_df = sales_df.withColumn("Date_Received", to_date(col("Date_Received"), "MM/dd/yyyy"))
    inventory_df = inventory_df.withColumn("Date_Received", to_date(col("Date"), "yyyy-MM-dd")).drop("Date")
    if "Product ID" in inventory_df.columns:
        inventory_df = inventory_df.withColumnRenamed("Product ID", "Product_ID")
    if "Stock Quantity" in inventory_df.columns:
        inventory_df = inventory_df.withColumnRenamed("Stock Quantity", "Stock_Quantity")
    weather_df = weather_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    need_row_id = False
    if "Product_ID" not in sales_df.columns or "Product_ID" not in inventory_df.columns:
        need_row_id = True

    if need_row_id:
        log.info("Product_ID missing — using row_id fallback for joins.")
        sales_df = sales_df.withColumn("row_id", row_number().over(Window.orderBy(sales_df.columns[0])))
        inventory_df = inventory_df.withColumn("row_id", row_number().over(Window.orderBy(inventory_df.columns[0])))
        weather_df = weather_df.withColumn("row_id", row_number().over(Window.orderBy(weather_df.columns[0])))
        joined_df = sales_df.join(inventory_df, on="row_id", how="inner").join(weather_df, on="row_id", how="left")
    else:
        joined_df = sales_df.join(inventory_df, on="Product_ID", how="inner")
        if "date" in weather_df.columns and "Date_Received" in joined_df.columns:
            joined_df = joined_df.join(weather_df.withColumnRenamed("date", "Date_Received_weather"),
                                       joined_df.Date_Received == weather_df.date, how="left")

    # Select safe columns
    sel_cols = []
    if "Product_ID" in joined_df.columns:
        sel_cols.append("Product_ID")
    if "Category" in joined_df.columns:
        sel_cols.append("Category")
    if "Stock_Quantity" in joined_df.columns:
        sel_cols.append("Stock_Quantity")
    if "Units Sold" in joined_df.columns:
        sel_cols.append(col("Units Sold").alias("Units_Sold"))
    elif "Units_Sold" in joined_df.columns:
        sel_cols.append("Units_Sold")
    if "meantemp" in joined_df.columns:
        sel_cols.append(col("meantemp").alias("meantemp"))
    if "humidity" in joined_df.columns:
        sel_cols.append(col("humidity").alias("humidity"))
    if "Date_Received" in joined_df.columns:
        sel_cols.append("Date_Received")

    final = joined_df.select(*[c if isinstance(c, str) else c for c in sel_cols])

    # grouped summary
    grouped_pd = []
    if "Product_ID" in final.columns and "Units_Sold" in final.columns:
        grouped = final.groupBy("Product_ID").sum("Units_Sold").withColumnRenamed("sum(Units_Sold)", "Total_Units_Sold")
        grouped_pd = grouped.toPandas().to_dict("records")

    # predicted_waste_risk
    if "meantemp" in final.columns and "humidity" in final.columns:
        final = final.withColumn(
            "predicted_waste_risk",
            when((col("meantemp") > 25) & (col("humidity") > 80), "High")
            .when((col("meantemp") > 20) & (col("humidity") > 70), "Medium")
            .otherwise("Low")
        )
    else:
        final = final.withColumn("predicted_waste_risk", when(col("Stock_Quantity") < 5, "High").otherwise("Low"))

    # to JSON-safe structures
    if "Date_Received" in final.columns:
        final_json = final.withColumn("Date_Received", date_format("Date_Received", "yyyy-MM-dd")).toPandas().to_dict("records")
    else:
        final_json = final.toPandas().to_dict("records")

    dim_products_pd = []
    if "Product_ID" in sales_df.columns:
        cols = [c for c in ("Product_ID", "Product_Name", "Category", "Unit_Price") if c in sales_df.columns]
        if cols:
            dim_products_pd = sales_df.select(*cols).distinct().toPandas().to_dict("records")

    dim_dates_pd = []
    if "Date_Received" in final.columns:
        dim_dates_pd = final.select("Date_Received").distinct().withColumnRenamed("Date_Received", "Date") \
                        .withColumn("Date", date_format("Date", "yyyy-MM-dd")).toPandas().to_dict("records")

    payload = {
        "status": "ok",
        "fact": final_json,
        "dim_products": dim_products_pd,
        "dim_dates": dim_dates_pd,
        "optimized": grouped_pd
    }

    RESULT_FILE.write_text(json.dumps(payload, default=str))
    log.info("WROTE %s (spark). Rows: %d", RESULT_FILE, len(payload["fact"]))
    return payload

# -----------------------
# Main control
# -----------------------
def main():
    if not DATA_DIR.exists():
        log.error("Data dir not found: %s", DATA_DIR)
        return 2

    # Try Spark unless forced pandas
    if not args.force_pandas:
        try:
            payload = run_spark_pipeline(DATA_DIR)
        except Exception as e:
            log.warning("Spark pipeline failed or not available: %s", e)
            log.info("Falling back to pandas pipeline")
            try:
                payload = run_pandas_pipeline(DATA_DIR)
            except Exception as e2:
                log.exception("Pandas fallback also failed")
                return 3
    else:
        payload = run_pandas_pipeline(DATA_DIR)

    # Optionally push to MongoDB
    if MONGO_URI:
        try:
            from pymongo import MongoClient
            log.info("Connecting to MongoDB")
            client = MongoClient(MONGO_URI)
            db = client.get_database("shelfsense")
            docs = payload.get("fact", [])
            if docs:
                db["fact_inventory"].insert_many(docs)
                log.info("Inserted %d documents to MongoDB", len(docs))
            else:
                log.info("No fact documents to insert in MongoDB")
        except Exception as e:
            log.exception("MongoDB insert failed (continuing). Error: %s", e)

    log.info("Pipeline finished successfully")
    return 0

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
