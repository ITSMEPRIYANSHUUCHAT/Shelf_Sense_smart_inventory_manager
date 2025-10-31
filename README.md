# Shelf_Sense_smart_inventory_manager
End-to-end data pipeline for perishable goods inventory optimization using Azure, Databricks, and Airflow
Project: Smart Inventory Forecasting & Waste Reduction Pipeline for Perishable Goods ⚙️ Overview This project builds an end-to-end data engineering pipeline designed to optimize inventory and minimize waste for perishable goods. It integrates batch and streaming data, performs transformations and forecasting, and generates actionable insights to recommend reorder quantities and detect potential spoilage risks. The solution simulates a real-world supply chain scenario where multiple data streams—such as sales, weather, promotions, and inventory—need to be ingested, processed, analyzed, and visualized efficiently. 🎯 Objectives

Build a scalable data pipeline combining batch and real-time ingestion.
Use Azure and Databricks services with a focus on low-cost and free-tier tools.
Implement data transformations and model data in a star schema within Azure Synapse.
Forecast demand and predict waste using Prophet/XGBoost models.
Optimize reorder quantities using linear programming.
Visualize key insights (forecast, waste, and inventory health) in Power BI or Metabase.
Enforce data quality and orchestrate all workflows using Apache Airflow.
