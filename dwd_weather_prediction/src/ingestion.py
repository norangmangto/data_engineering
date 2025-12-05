import os
from wetterdienst.provider.dwd.observation import DwdObservationRequest
from wetterdienst import Settings
import pandas as pd

STATION_ID = "01078" # Düsseldorf
DATA_DIR = "data/raw"
OUTPUT_FILE = os.path.join(DATA_DIR, f"dwd_daily_{STATION_ID}.csv")

def fetch_data():
    """
    Fetch historical and recent weather data for Düsseldorf.
    Merges with existing data if available.
    """
    print(f"Fetching data for Station {STATION_ID} (Düsseldorf)...")

    settings = Settings(ts_shape="long")

    # Check if we already have data
    if os.path.exists(OUTPUT_FILE):
        print("Existing data found. Fetching 'recent' data to append...")
        periods = ["recent"]
    else:
        print("No existing data. Fetching 'historical' and 'recent' data...")
        periods = ["historical", "recent"]

    request = DwdObservationRequest(
        parameters=[("daily", "climate_summary")],
        periods=periods,
        settings=settings
    )

    stations = request.filter_by_station_id(station_id=[STATION_ID])

    try:
        new_df = stations.values.all().df
    except:
        new_df = stations.values.all().to_pandas()

    # Convert to pandas if it's polars
    if not isinstance(new_df, pd.DataFrame):
        new_df = new_df.to_pandas()

    # Ensure date is datetime
    new_df['date'] = pd.to_datetime(new_df['date'], utc=True)

    if os.path.exists(OUTPUT_FILE):
        print("Merging with existing data...")
        old_df = pd.read_csv(OUTPUT_FILE)
        old_df['date'] = pd.to_datetime(old_df['date'], utc=True)

        # Concatenate and drop duplicates
        # We keep the 'last' (newest) value if there's an overlap, assuming newer data might be corrected
        combined_df = pd.concat([old_df, new_df])
        combined_df = combined_df.drop_duplicates(subset=['station_id', 'date', 'parameter'], keep='last')
        combined_df = combined_df.sort_values(by=['date', 'parameter'])
    else:
        combined_df = new_df

    os.makedirs(DATA_DIR, exist_ok=True)
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Data saved to {OUTPUT_FILE}")
    print(f"Total rows: {len(combined_df)}")
    print(combined_df.tail())

if __name__ == "__main__":
    fetch_data()
