#!/bin/bash
# Run the OSM Data pipeline (cities and countries)

uv pip install -e .

echo "=== OSM Data Pipeline (Cities & Countries) ==="
echo ""

# Step 1: Run ingestion
echo "Step 1: Downloading and ingesting OSM data..."
cd "$(dirname "$0")"
uv run python src/ingestion.py

# Step 2: Run dbt
echo ""
echo "Step 2: Running dbt transformations..."
cd transform
uv run python -m dbt.cli.main run

echo ""
echo "Step 3: Running dbt tests..."
uv run python -m dbt.cli.main test

echo ""
echo "=== Pipeline completed successfully! ==="
echo ""
echo "Query the data:"
echo "  duckdb data/processed/osm_data.duckdb"
echo ""
echo "View dbt docs:"
echo "  cd transform && uv run python -m dbt.cli.main docs generate && uv run python -m dbt.cli.main docs serve"
