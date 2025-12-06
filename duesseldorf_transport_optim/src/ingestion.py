import os
import requests
import zipfile
import io
import pandas as pd
from pathlib import Path

# Configuration
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
GTFS_URL = "https://www.vrr.de/opencms/export/sites/vrr/modules/openservice/data/google_transit.zip" # This is a common stable link for VRR GTFS
UNFALLATLAS_BASE_URL = "https://unfallatlas.statistikportal.de/App/Download/Unfallorte2022_LinRef.zip" # Example for 2022 data
TRAFFIC_DATA_URL = "https://opendata.duesseldorf.de/sites/default/files/Verkehrszaehlung_5J_Zaehlstellen_2015_2019.csv" # Example URL, might need check

def download_file(url: str, dest_path: Path):
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Saved to {dest_path}")

def extract_zip(zip_path: Path, extract_to: Path):
    print(f"Extracting {zip_path}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Extracted to {extract_to}")

def ingest_gtfs():
    gtfs_dir = RAW_DIR / "gtfs"
    gtfs_dir.mkdir(parents=True, exist_ok=True)
    zip_path = gtfs_dir / "vrr_gtfs.zip"

    try:
        download_file(GTFS_URL, zip_path)
        extract_zip(zip_path, gtfs_dir)
    except Exception as e:
        print(f"Failed to fetch GTFS data: {e}. Please manually place 'google_transit.zip' contents in {gtfs_dir}")

def ingest_accidents():
    accidents_dir = RAW_DIR / "accidents"
    accidents_dir.mkdir(parents=True, exist_ok=True)
    zip_path = accidents_dir / "unfallorte.zip"

    try:
        # Note: In a real app we might scrape for the latest year. Here we use a fixed 2022 link or placeholder
        download_file(UNFALLATLAS_BASE_URL, zip_path)
        extract_zip(zip_path, accidents_dir)

        # We only really care about the CSV inside that covers NRW or Dusseldorf.
        # Often the zip contains a Shapefile and CSV.
        print("Accident data downloaded. Check extraction folder for CSVs.")
    except Exception as e:
        print(f"Failed to fetch Accident data: {e}")

def ingest_traffic():
    traffic_dir = RAW_DIR / "traffic"
    traffic_dir.mkdir(parents=True, exist_ok=True)
    csv_path = traffic_dir / "traffic_counts.csv"

    try:
        download_file(TRAFFIC_DATA_URL, csv_path)

        # Quick validation
        df = pd.read_csv(csv_path, sep=';', encoding='latin1') # German CSVs often latin1 and semicolon
        print(f"Traffic data loaded. Shape: {df.shape}")
    except Exception as e:
        print(f"Failed to fetch Traffic data: {e}")

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting data ingestion...")
    ingest_gtfs()
    ingest_accidents()
    ingest_traffic()
    print("Ingestion complete!")

if __name__ == "__main__":
    main()
