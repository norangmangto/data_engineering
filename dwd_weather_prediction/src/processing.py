import pandas as pd
import os

INPUT_FILE = "data/raw/dwd_daily_01078.csv"
OUTPUT_FILE = "data/processed/dwd_daily_01078_processed.csv"

def process_data():
    print("Loading data...")
    df = pd.read_csv(INPUT_FILE)

    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Pivot table to wide format
    # We want 'date' as index, 'parameter' as columns, and 'value' as values
    print("Pivoting data...")
    df_wide = df.pivot(index='date', columns='parameter', values='value')

    # Select relevant columns
    # temperature_air_mean_2m: Daily mean temperature
    # precipitation_height: Daily precipitation
    # wind_speed: Daily mean wind speed
    # sunshine_duration: Daily sunshine duration
    relevant_cols = [
        'temperature_air_mean_2m',
        'temperature_air_max_2m',
        'temperature_air_min_2m',
        'precipitation_height',
        'wind_speed',
        'sunshine_duration'
    ]

    # Filter columns that actually exist
    available_cols = [c for c in relevant_cols if c in df_wide.columns]
    df_wide = df_wide[available_cols]

    print(f"Selected columns: {available_cols}")

    # Handle missing values
    # Interpolate for temperature/pressure (continuous)
    # Fill 0 for precipitation (if missing often means 0, but let's check)
    # For simplicity, we'll interpolate everything linearly for now, but limit the direction
    print("Handling missing values...")
    df_wide = df_wide.interpolate(method='time', limit_direction='both')

    # Feature Engineering: Lag features
    # Predict next 7 days temperature based on past 7 days
    target_col = 'temperature_air_mean_2m'

    # Create lag features (past 7 days)
    for i in range(1, 8):
        df_wide[f'{target_col}_lag_{i}'] = df_wide[target_col].shift(i)

    # Create target features (next 7 days)
    for i in range(1, 8):
        df_wide[f'target_day_{i}'] = df_wide[target_col].shift(-i)

    # Drop rows with NaNs (due to shifting)
    df_wide = df_wide.dropna()

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df_wide.to_csv(OUTPUT_FILE)
    print(f"Processed data saved to {OUTPUT_FILE}")
    print(f"Shape: {df_wide.shape}")
    print(df_wide.head())

if __name__ == "__main__":
    process_data()
