# 🚀 Shelf Sense: The Ultimate Inventory Beast Mode Pipeline 🔥

**Conquer waste. Crush reorders. Rule perishable inventory.**

Shelf Sense is an end-to-end **data engineering powerhouse** that transforms raw data into actionable insights — **forecasting demand, predicting spoilage, and optimizing reorders** like a boss.  
Built for **speed, scale, and zero bullshit**, it’s your prototype to **simulate a real supply chain** with **automation, analytics, and dashboards — all on free-tier tools**.  

---

## 🧠 Project Overview

Shelf Sense is designed to **optimize inventory and reduce waste** for perishable goods (think fruits, dairy, seafood — the stuff that spoils fast).

It ingests **batch and streaming data**, cleans and transforms it into a **star schema**, **predicts spoilage risks**, **optimizes reorder quantities**, validates data quality, and visualizes results in **real time** — all orchestrated by Airflow.  
No paid APIs. No cloud overkill. Just clean, efficient data flow.

---

## 🎯 Objectives

✅ **Scalable Ingestion:** Batch + Streaming data from sales, weather, inventory, and promotions.  
✅ **Data Transformation:** Model data into a **star schema** for deep analytical queries.  
✅ **Spoilage Prediction:** Mock model flags risk when **Temp > 25°C + Humidity > 80%**.  
✅ **Optimization:** Use **PuLP** for reorder quantities that **minimize waste and stockouts**.  
✅ **Quality Control:** Validate every dataset using **Great Expectations**.  
✅ **Visualization:** Streamlit for real-time dashboards, Power BI for analytical reports.  
✅ **Automation:** Apache Airflow to run it all hands-free.  

---

## 🧩 Architecture Overview

**From raw CSVs to optimized reorders — here’s how the beast moves:**

```mermaid
flowchart TD
    A[Kaggle CSVs (data/raw)] --> B[Preprocess (scripts/preprocess_data.py)]
    B --> C[Processed Files (data/processed)]
    C --> D[Azure Data Lake (Batch Upload)]
    C --> E[Azure Event Hubs (Stream Send)]
    D & E --> F[Airflow DAG (dags/full_pipeline_dag.py)]
    F --> G[Colab PySpark Transform -> Star Schema]
    G --> H[MongoDB Atlas (fact_inventory, dim_products, dim_dates)]
    H --> I[PuLP Optimize (scripts/optimize.py)]
    I --> J[Validation (Great Expectations)]
    J --> K[Streamlit UI (scripts/streamlit_ui.py)]
    J --> L[Power BI Dashboard (dashboards/inventory_dashboard.pbix)]
