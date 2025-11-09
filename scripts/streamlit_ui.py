import streamlit as st
from pymongo import MongoClient
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os
import pandas as pd
import plotly.express as px
from urllib.parse import quote_plus

# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------
load_dotenv()

user = os.getenv("MONGO_USER")
password = os.getenv("MONGO_PASS")
cluster = os.getenv("MONGO_CLUSTER")

if not all([user, password, cluster]):
    st.error("Missing MongoDB credentials. Check your .env file.")
    st.stop()

username = quote_plus(user)
pwd = quote_plus(password)
MONGO_URI = os.getenv('MONGO_URI', f"mongodb+srv://{username}:{pwd}@{cluster}/shelfsensestorage?retryWrites=true&w=majority")

# ---------------------------------------------------------
# Streamlit page setup
# ---------------------------------------------------------
st.set_page_config(page_title="Shelf Sense Dashboard", layout="wide")
st.title("🚀 Shelf Sense: Smart Inventory Dashboard")

# ---------------------------------------------------------
# MongoDB connection (cached)
# ---------------------------------------------------------
@st.cache_resource
def get_db():
    client = MongoClient(MONGO_URI)
    return client['shelf_sense_db']

db = get_db()

# ---------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------
st.sidebar.header("Filters")
risk_filter = st.sidebar.multiselect(
    "Waste Risk Level",
    ['Low', 'Medium', 'High'],
    default=['Low', 'Medium', 'High']
)

# ---------------------------------------------------------
# Main Dashboard Layout
# ---------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.header("📊 Inventory Facts (From PySpark Transform)")

    fact_df = pd.DataFrame(list(db['fact_inventory'].find()))
    if not fact_df.empty:
        fact_df = fact_df[fact_df['predicted_waste_risk'].isin(risk_filter)]
        st.dataframe(fact_df, use_container_width=True)

        # Key Metrics
        st.subheader("Key Metrics")
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Total Stock", fact_df['Stock_Quantity'].sum())
        col_b.metric("Avg Demand", round(fact_df['Units_Sold'].mean(), 2))
        col_c.metric("High Risk Items", (fact_df['predicted_waste_risk'] == 'High').sum())
    else:
        st.warning("No inventory data found. Run the ETL DAG to populate MongoDB.")

with col2:
    st.header("📈 Reorder Recommendations (PuLP Optimization)")

    opt_df = pd.DataFrame(list(db['optimized_reorders'].find()))
    if not opt_df.empty:
        st.dataframe(opt_df, use_container_width=True)

        # Interactive Chart
        fig = px.bar(
            opt_df,
            x='Product_ID',
            y=['Stock_Quantity', 'recommended_reorder'],
            barmode='group',
            title="Stock vs Recommended Reorder"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No optimized reorder data available yet.")

# ---------------------------------------------------------
# Alerts Section
# ---------------------------------------------------------
st.header("⚠️ High-Risk Alerts (From Queries)")
alerts_df = pd.DataFrame(list(db['alerts'].find()))
if not alerts_df.empty:
    st.table(alerts_df[['Product_ID', 'Stock_Quantity', 'predicted_waste_risk']])
else:
    st.info("No alerts—data is healthy! Run DAG for updates.")

# ---------------------------------------------------------
# Realtime Refresh / Simulated Airflow Trigger
# ---------------------------------------------------------
st.header("🔄 Realtime Refresh")
if st.button("Run Full Pipeline (Airflow DAG)"):
    st.write("Triggering Airflow DAG... (Simulation)")
    # Example placeholder for real API trigger:
    # import requests
    # requests.post(
    #     'http://localhost:8080/api/v1/dags/full_pipeline_dag/dagRuns',
    #     auth=('shelf_admin', 'shelf_pass123'),
    #     json={"conf": {}}
    # )
    st.success("Pipeline triggered! Refresh page in 1 min for updates.")

# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------
st.markdown("---")
st.caption("Data from MongoDB (Updated after DAG run). Built with Streamlit for realtime views!")
