# 🚀 Shelf Sense: The Ultimate Inventory Beast Mode Pipeline 🔥

**Conquer waste. Crush reorders. Rule perishable inventory.**  
Shelf Sense is an end-to-end **data engineering powerhouse** that transforms raw data into gold — **forecasting demand, predicting spoilage, and optimizing reorders** like a boss.  
Built for **speed, scale, and zero bullshit**, it’s your prototype to **simulate a real supply chain** with **automation, analytics, and dashboards — all on free-tier tools.**

---

## 🧠 Project Overview

Shelf Sense is designed to **optimize inventory and reduce waste** for perishable goods (think fruits, dairy, seafood — the stuff that spoils fast).  
It ingests **batch and streaming data**, cleans and transforms it into a **star schema**, predicts spoilage risks, optimizes reorder quantities, validates data quality, and visualizes results in real time — all orchestrated by Airflow.  
No paid APIs. No cloud overkill. Just clean, efficient data flow.

---

## 🎯 Objectives

✅ **Scalable Ingestion:** Batch + Streaming data from sales, weather, inventory, and promotions.  
✅ **Data Transformation:** Model data into a **star schema** for deep analytical queries.  
✅ **Spoilage Prediction:** Mock model flags risk when **Temp > 25°C + Humidity > 80%.**  
✅ **Optimization:** Use **PuLP** for reorder quantities that **minimize waste and stockouts.**  
✅ **Quality Control:** Validate every dataset using **Great Expectations.**  
✅ **Visualization:** Streamlit for real-time dashboards, Power BI for analytical reports.  
✅ **Automation:** Apache Airflow to run it all hands-free.

---

## 🧩 Architecture Overview

**From raw CSVs to optimized reorders — here’s how the beast moves:**

1. Kaggle CSVs → Preprocess → Processed files  
2. Batch upload to Azure Data Lake + Streaming to Event Hubs  
3. Airflow DAG orchestrates the flow  
4. Colab PySpark for transformation into star schema  
5. Load to MongoDB Atlas  
6. PuLP optimization, queries, and quality checks  
7. Streamlit UI and Power BI dashboard pull from MongoDB

---

## 🛠 Tech Stack Badassery

| Layer | Tools / Services | Purpose |
|-------|------------------|----------|
| **Data Ingestion** | Azure Data Factory (batch), Event Hubs (streaming), Python SDKs | Load sales/weather/promotions + live inventory |
| **Orchestration** | Apache Airflow (Docker local) | Automate the whole flow like a pro |
| **Storage** | Azure Data Lake Gen2 (free tier), MongoDB Atlas (free M0) | Raw storage + NoSQL DB for schema |
| **Transformation** | PySpark (Colab free), Pandas fallback | Clean, join, star schema magic |
| **Optimization** | PuLP | Linear programming to crush waste |
| **Validation** | Great Expectations | Schema, nulls, anomalies — zero tolerance for bad data |
| **Visualization** | Streamlit (realtime), Power BI Desktop (static) | Interactive charts, alerts, metrics |
| **Deployment** | Docker Compose, GitHub Actions (CI/CD) | Containerized for easy runs |

---

## ⚙️ Setup & Run the Beast

### 🧩 Clone & Configure
```bash
git clone https://github.com/yourusername/shelf-sense.git
cd shelf-sense
```

Create `.env` with your keys:
```ini
DATA_LAKE_CONN_STR=your_azure_datalake_key
EVENT_HUB_CONN_STR=your_eventhub_key
MONGO_USER=your_user
MONGO_PASS=your_pass
MONGO_CLUSTER=your_cluster_url
```

---

### ⚙️ Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

### 🐳 Launch Airflow
```bash
docker compose up -d
```
UI: [http://localhost:8080](http://localhost:8080)  
Login: `shelf_admin / shelf_pass123`

---

### 🔄 Run the Pipeline
- UI → `full_pipeline_dag` → Trigger  
- Watch ingestion → transformation → optimization flow live!

---

### 📊 Visualization

**Streamlit:**
```bash
streamlit run scripts/streamlit_ui.py
```
Visit: [http://localhost:8501](http://localhost:8501)

**Power BI:**
- Open `dashboards/inventory_dashboard.pbix`  
- Click **Refresh** to load latest data from MongoDB

💡 **Pro Tip:** Run Colab notebook manually if needed for PySpark — DAG handles the rest.

---

## 🎥 Demo Like a Pro

| Time | Step |
|------|------|
| 1 min | GitHub repo tour (README, folders) |
| 3 mins | Airflow UI (trigger DAG, show realtime logs/graph) |
| 2 mins | MongoDB Atlas (refresh collections — data updated) |
| 3 mins | Streamlit UI (charts, filters, refresh for realtime) |
| 1 min | Power BI (interactive dashboard) |

**Badass Outcomes:** Automated pipeline, waste reduction insights, scalable setup. Fork and conquer! 🌟

---

## 💥 Conclusion

Shelf Sense isn’t just a prototype — it’s a **data-driven retail intelligence engine.**  
It brings together **engineering discipline**, **optimization brilliance**, and **visual storytelling** — all in one lean, scalable system.

> “One Airflow trigger. Full pipeline execution. Actionable insights in minutes.” ⚙️

---

## 🧑‍💻 Built for Engineers Who Mean Business

If you love automation, optimization, and coffee-fueled debugging marathons — this project’s your playground.  
**Fork it. Run it. Break it. Make it better.**  

**Let’s dominate the supply chain. 💥**
