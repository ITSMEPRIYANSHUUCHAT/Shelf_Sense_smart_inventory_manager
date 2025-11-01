import pandas as pd
import os

data_dir = 'data/raw'
output_dir = 'data/processed'
os.makedirs(output_dir, exist_ok=True)

# Combine weather
train = pd.read_csv(os.path.join(data_dir, 'DailyDelhiClimateTrain.csv'))
test = pd.read_csv(os.path.join(data_dir, 'DailyDelhiClimateTest.csv'))
weather = pd.concat([train, test], ignore_index=True)
weather.to_csv(os.path.join(output_dir, 'weather_daily.csv'), index=False)
print("Combined weather:", len(weather), "rows")