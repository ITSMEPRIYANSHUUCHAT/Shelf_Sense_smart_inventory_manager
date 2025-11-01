import pandas as pd
import os

# Paths
data_dir = 'data/raw'
output_dir = 'data/processed'
os.makedirs(output_dir, exist_ok=True)

# Perishable categories
perishables = ['Fruits & Vegetables', 'Dairy', 'Seafood', 'Groceries']  # Adapt as needed

# 1. Sales Data (Grocery_Inventory_and_Sales)
sales_file = os.path.join(data_dir, 'Grocery_Inventory_and_Sales_Dataset.csv')
sales_df = pd.read_csv(sales_file)
sales_df.rename(columns={'Catagory': 'Category'}, inplace=True)  # Fix typo
sales_df = sales_df[sales_df['Category'].isin(perishables)]
sales_df.to_csv(os.path.join(output_dir, 'sales_perishables.csv'), index=False)
print(f"Processed sales: {len(sales_df)} rows")

# 2. Inventory Data (retail_store_inventory) - Split for streaming sim
inv_file = os.path.join(data_dir, 'retail_store_inventory.csv')
inv_df = pd.read_csv(inv_file)
inv_df['Date'] = pd.to_datetime(inv_df['Date'])  # Ensure date format
inv_df = inv_df[inv_df['Category'].isin(perishables)]
# Split into daily chunks for simulation
dates = inv_df['Date'].unique()
for i, date in enumerate(dates):
    chunk_df = inv_df[inv_df['Date'] == date]
    chunk_df.to_csv(os.path.join(output_dir, f'inventory_chunk_{i}_{date.date()}.csv'), index=False)
print(f"Processed inventory: {len(inv_df)} rows, split into {len(dates)} chunks")

# 3. Weather Data (already combined)
weather_file = os.path.join(output_dir, 'weather_daily.csv')  # From combine script
weather_df = pd.read_csv(weather_file)
weather_df['date'] = pd.to_datetime(weather_df['date'])
# No filter needed; clean if desired (e.g., fill NaNs)
weather_df = weather_df.ffill()
weather_df.to_csv(os.path.join(output_dir, 'weather_daily_clean.csv'), index=False)
print(f"Processed weather: {len(weather_df)} rows")

# 4. Promotions Data (promoted.csv as main; aggregate to weekly)
promo_file = os.path.join(data_dir, 'promoted.csv')
promo_df = pd.read_csv(promo_file)
# No date column—add synthetic weeks based on index or card_tenure (proxy for time)
promo_df['Synthetic_Week'] = pd.cut(promo_df['card_tenure'], bins=10, labels=range(1, 11))  # Group into 10 "weeks"
weekly_promo = promo_df.groupby('Synthetic_Week',observed=True).agg({
    'resp': 'sum',  # Total responses
    'num_promoted': 'sum',
    'avg_bal': 'mean'  # Avg balance
}).reset_index()
weekly_promo.to_csv(os.path.join(output_dir, 'promotions_weekly.csv'), index=False)
print(f"Processed promotions: {len(weekly_promo)} rows (aggregated)")

# Optional: Use target.csv if needed (similar structure; merge if responses match)
# target_df = pd.read_csv(os.path.join(data_dir, 'target.csv'))
# ... (e.g., merge on customer_id)