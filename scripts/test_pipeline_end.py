import os
from dotenv import load_dotenv
from download_from_drive import download_from_drive
from load_from_drive import load_from_drive
from optimize import optimize_with_prediction
from mongo_queries import query_insights
from data_quality import validate_data

load_dotenv()

print("=== Testing Pipeline End (Download → Load → Optimize → Query → Quality) ===\n")

# 1. Download from Drive
print("1. Downloading from Drive...")
download_from_drive()
print("Download complete.\n")

# 2. Load to MongoDB
print("2. Loading to MongoDB...")
load_from_drive()
print("Load complete.\n")

# 3. Optimize
print("3. Optimizing...")
optimize_with_prediction()
print("Optimization complete.\n")

# 4. Query/Alerts
print("4. Generating alerts...")
query_insights()
print("Queries complete.\n")

# 5. Quality Check
print("5. Validating quality...")
validate_data()
print("Validation complete.\n")

print("\n=== Success! Refresh Streamlit for updates. ===")