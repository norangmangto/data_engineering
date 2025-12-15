# Data Engineering Workspace

This repository contains data engineering projects focusing on data pipelines, geospatial analysis, time-series forecasting, and machine learning integration.

## Projects

### 1. [OSM Data Pipeline](./osm_data)
A comprehensive data pipeline that downloads and processes **OpenStreetMap** cities and countries data. It extracts geographic boundaries, ISO codes, and metadata, storing them in **DuckDB** with spatial extensions. Uses **dbt** for data transformation and enrichment, providing ready-to-use datasets for geographic analysis.

**Key Features:**
- Downloads global cities and countries from OpenStreetMap
- Smart caching (skips re-download if data is < 7 days old)
- Spatial geometry support (polygons, points)
- ISO 3166-1 codes for countries
- dbt models with data quality tests

### 2. [DWD Weather Prediction](./dwd_weather_prediction)
A continuously updating data pipeline that fetches weather data from the German Meteorological Service (DWD) for Düsseldorf. It processes data using **DuckDB** and **dbt**, and trains **XGBoost**, **LSTM**, and **TensorFlow** models to provide a 7-day forecast for temperature, wind, and rain via a **FastAPI** endpoint.

**Key Features:**
- Real-time weather data ingestion from DWD
- Time-series transformation with dbt
- Multi-model ML predictions (LSTM, XGBoost)
- REST API for forecast queries

### 3. [Düsseldorf Transport Optimizer](./duesseldorf_transport_optim)
A routing application that finds the safest and fastest public transport routes in Düsseldorf. It combines **GTFS** (Timetables), **Accident Data** (Unfallatlas), and **Traffic Data** to calculate "Risk Scores" for transport stops. It uses a custom **NetworkX** graph router to penalize high-risk areas.

**Key Features:**
- Multi-source data integration (GTFS, accidents, traffic)
- Network-based route optimization
- Risk scoring for public transport stops
- Graph-based routing with NetworkX

## Technologies

**Core Stack:**
- **Language**: Python 3.13 (managed by `uv`)
- **Database**: DuckDB (with spatial extension)
- **Transformation**: dbt (data build tool)
- **Machine Learning**: PyTorch, TensorFlow/Keras, XGBoost, Scikit-Learn
- **API**: FastAPI
- **Orchestration**: Custom Scripts + Shell

**Data Sources:**
- OpenStreetMap (Overpass API)
- German Meteorological Service (DWD)
- GTFS (Public Transport)
- Unfallatlas (Accident Data)

## Project Structure

```
data_engineering/
├── osm_data/                    # OpenStreetMap cities & countries pipeline
│   ├── src/ingestion.py        # Download and load OSM data
│   ├── transform/              # dbt models
│   └── data/                   # DuckDB database
├── dwd_weather_prediction/      # Weather forecasting pipeline
│   ├── src/                    # Ingestion and ML training
│   ├── models/                 # Trained LSTM/XGBoost models
│   └── transform/              # dbt weather transformations
├── duesseldorf_transport_optim/ # Transport routing optimizer
│   ├── src/                    # Routing and risk calculation
│   └── transform/              # dbt transport models
└── tools/                       # Shared utilities and examples
    ├── dbt/                    # dbt learning projects
    └── apache_beam/            # Beam examples
```

## Getting Started

Each project has its own README with detailed setup instructions. General workflow:

1. **Install uv** (Python package manager):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Navigate to a project**:
   ```bash
   cd osm_data  # or dwd_weather_prediction, duesseldorf_transport_optim
   ```

3. **Install dependencies**:
   ```bash
   uv pip install -e .
   ```

4. **Run the pipeline**:
   ```bash
   ./run_pipeline.sh  # Most projects have this orchestration script
   ```

## Common Commands

```bash
# Run ingestion
uv run python src/ingestion.py

# Run dbt transformations
cd transform && uv run python -m dbt.cli.main run

# Run dbt tests
uv run python -m dbt.cli.main test

# Query DuckDB
duckdb data/processed/*.duckdb
```

## Requirements

- Python 3.9+
- uv (Python package manager)
- Git

## License

MIT
