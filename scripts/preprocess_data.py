import pandas as pd
import os

# Paths (adjust if needed)
data_dir = 'data/raw'
output_dir = 'data/processed'
os.makedirs(output_dir, exist_ok=True)

# 1. Sales Data (filter for perishables like 'Produce', 'Dairy')
sales_file = os.path.join(data_dir, 'Grocery_Inventory_and_Sales.csv')  # Adjust name
sales_df = pd.read_csv(sales_file)
# Assuming columns like 'Category', 'Product', 'Date', 'Quantity_Sold', 'Price'
perishables = ['Produce', 'Dairy', 'Fruits', 'Vegetables']  # Adapt based on your data
sales_df = sales_df[sales_df['Category'].isin(perishables)]  # Filter
sales_df.to_csv(os.path.join(output_dir, 'sales_perishables.csv'), index=False)
print(f"Processed sales: {len(sales_df)} rows")

# 2. Inventory Data (simulate streaming by splitting into chunks)
inv_file = os.path.join(data_dir, 'inventory_data.csv')  # Adjust
inv_df = pd.read_csv(inv_file)
# Filter similarly if category column exists
inv_df = inv_df[inv_df['Category'].isin(perishables)] if 'Category' in inv_df.columns else inv_df
# Split for simulation: e.g., daily chunks
for i, chunk in enumerate(pd.date_range(inv_df['Date'].min(), inv_df['Date'].max(), freq='D')):
    chunk_df = inv_df[inv_df['Date'] == chunk]  # Assuming 'Date' column
    chunk_df.to_csv(os.path.join(output_dir, f'inventory_chunk_{i}.csv'), index=False)
print(f"Processed inventory: {len(inv_df)} rows, split into chunks")

# 3. Weather Data (daily batch, no filter needed)
weather_file = os.path.join(data_dir, 'DailyDelhiClimateTrain.csv')
weather_df = pd.read_csv(weather_file)
# Clean: Rename columns if needed, e.g., 'date' to 'Date'
weather_df.rename(columns={'date': 'Date'}, inplace=True)
weather_df.to_csv(os.path.join(output_dir, 'weather_daily.csv'), index=False)
print(f"Processed weather: {len(weather_df)} rows")

# 4. Promotions Data (weekly batch)
promo_file = os.path.join(data_dir, 'promotion_responses.csv')  # Adjust
promo_df = pd.read_csv(promo_file)
# Aggregate to weekly if needed (assuming 'Date' or 'Week')
if 'Date' in promo_df.columns:
    promo_df['Week'] = pd.to_datetime(promo_df['Date']).dt.isocalendar().week
    promo_df.groupby('Week').agg({'Promotion_Type': 'first', 'Response': 'sum'}).to_csv(os.path.join(output_dir, 'promotions_weekly.csv'))
print(f"Processed promotions: {len(promo_df)} rows")