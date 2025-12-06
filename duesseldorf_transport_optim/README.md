# Düsseldorf Public Transport Optimizer

A safety-aware routing application for public transport in Düsseldorf. It finds routes that avoid accident hotspots by penalizing risky stops.

## Features
- **Multimodal Routing**: Supports Transfers, Trams, U-Bahn (based on VRR GTFS).
- **Risk Analysis**: Integrates "Unfallatlas" (Accident Atlas) data to calculate a **Risk Score** for every stop.
- **Traffic Awareness**: Uses historical traffic counts to weight route duration.
- **Custom Router**: Python-based **NetworkX** implementation for flexible weight calculation.

## Architecture
1. **Ingestion**: Downloads Open Data (GTFS, Accidents, Traffic).
2. **Transformation (dbt)**: processing raw data into `transport.duckdb`.
   - `int_network_risk.sql`: Geospatial join of stops and accidents.
3. **Serving (FastAPI)**: Endpoint to calculate shortest path on the weighted graph.

## Setup

1. **Install Dependencies**
   ```bash
   uv sync
   ```

2. **Generate Data**
   *(Note: Using mock data generator as live Open Data URLs are unstable)*
   ```bash
   uv run python src/generate_mock_data.py
   ```

3. **Build Data Warehouse**
   Runs dbt models to materialize tables in DuckDB.
   ```bash
   cd transform
   uv run dbt run
   cd ..
   ```

4. **Start API**
   Starts the routing server on port 8002.
   ```bash
   uv run python src/app.py
   ```

## Usage

**Request a Safe Route:**

```bash
curl -X POST http://localhost:8002/route \
  -H "Content-Type: application/json" \
  -d '{
    "start": {"lat": 51.22, "lon": 6.79},
    "destination": {"lat": 51.19, "lon": 6.8021}
  }'
```

**Response:**
Returns a list of segments with travel time and risk scores.
```json
{
  "total_duration_minutes": 22.0,
  "total_accumulated_risk": 3.0,
  "segments": [...]
}
```
