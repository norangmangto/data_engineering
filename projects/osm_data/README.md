# OSM Data Pipeline (Cities & Countries)

A data engineering pipeline that downloads OpenStreetMap city and country data, processes it, and loads it into DuckDB with dbt transformations.

## Overview

This pipeline:
1. Downloads city and country data from OpenStreetMap using the Overpass API
2. Extracts city and country names, locations, and polygon geometries
3. Loads raw data into DuckDB
4. Transforms data using dbt models with spatial extensions

## Project Structure

```
osm_data/
├── src/
│   ├── ingestion.py          # OSM data download and ingestion script
│   └── __pycache__/
├── data/
│   ├── raw/                  # Raw OSM JSON files (generated)
│   │   ├── cities.json
│   │   └── countries.json
│   └── processed/            # DuckDB database (generated)
│       └── osm_data.duckdb
├── transform/                # dbt project
│   ├── dbt_project.yml       # dbt configuration
│   ├── profiles.yml          # DuckDB connection profile
│   ├── models/
│   │   ├── staging/          # Staging models (cleaned, typed data)
│   │   │   ├── stg_cities.sql
│   │   │   ├── stg_countries.sql
│   │   │   └── schema.yml
│   │   └── intermediate/     # Enriched models (calculated fields)
│   │       ├── int_cities_enriched.sql
│   │       ├── int_countries_enriched.sql
│   │       └── schema.yml
│   ├── logs/                 # dbt logs (generated)
│   └── target/               # dbt artifacts (generated)
├── pyproject.toml            # Python dependencies
├── run_pipeline.sh           # Complete pipeline orchestration script
├── .gitignore
└── README.md
```

## Setup

### Prerequisites

- Python 3.9+
- pip or uv package manager
- Git (for version control)

### Installation

1. Clone the repository and navigate to the project:
```bash
cd osm_data
```

2. Install Python dependencies:
```bash
pip install -e .
# or with uv:
uv pip install -e .
```

3. Install dbt and dependencies:
```bash
cd transform
pip install dbt-duckdb
cd ..
```

4. (Optional) Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

## Usage

### Quick Start

Run the complete pipeline with a single command:
```bash
./run_pipeline.sh
```

This executes all steps: download cities, download countries, transform with dbt, and run tests.

### Step-by-Step Usage

#### 1. Download and Ingest OSM Data

Run the ingestion script to download city and country data from OpenStreetMap:

```bash
python src/ingestion.py
```

**Options:**
- `--country <name>`: Filter cities by specific country (optional)
- `--data-dir <path>`: Specify data directory (default: 'data')
- `--force`: Force re-download even if recent data exists
- `--max-age-days <days>`: Maximum age in days before re-downloading (default: 7)

**Examples:**
```bash
# Download all cities and countries (uses cached data if < 7 days old)
python src/ingestion.py

# Force re-download even if recent data exists
python src/ingestion.py --force

# Use cached data if < 30 days old
python src/ingestion.py --max-age-days 30

# Download only cities from Germany
python src/ingestion.py --country Germany

# Use custom data directory
python src/ingestion.py --data-dir /path/to/data
```

**Smart Caching:**
By default, the ingestion script checks if OSM data already exists and is recent (less than 7 days old). If so, it uses the cached data instead of re-downloading:
- Saves time (Overpass API queries can take 10+ minutes)
- Reduces load on OSM servers
- Prevents rate limiting issues
- Configurable via `--max-age-days` parameter

To force a fresh download, use the `--force` flag.

**What it downloads:**
- **Cities**: All cities tagged in OpenStreetMap globally, with names, locations, and geometries
- **Countries**: All country administrative boundaries (admin_level=2) with ISO codes and polygons

**Data Freshness:**
- Downloaded data is cached for 7 days by default
- Subsequent runs use cached data if available and recent
- Logs show data age and cache status
- Override with `--force` or adjust with `--max-age-days`

**Note**: Initial Overpass API queries can take several minutes and return large datasets:
- Cities: Typically thousands of cities worldwide (~10-15 minutes)
- Countries: Usually 180-250 country boundaries (~5-10 minutes)

For testing with smaller datasets, modify the Overpass queries in `src/ingestion.py` to add bounding box filters.

#### 2. Run dbt Transformations

Transform the raw data using dbt:

```bash
cd transform
uv run python -m dbt.cli.main run
```

**Other dbt commands:**
```bash
# Run tests
uv run python -m dbt.cli.main test

# Generate and view documentation
uv run python -m dbt.cli.main docs generate
uv run python -m dbt.cli.main docs serve

# Clean artifacts
uv run python -m dbt.cli.main clean

# Parse project (validate syntax)
uv run python -m dbt.cli.main parse

# Debug connection
uv run python -m dbt.cli.main debug
```

## Data Models

### Staging Layer

**stg_cities**: Cleaned and typed city data from raw OSM
- Converts WKT strings to geometry objects
- Parses population as integer
- Maintains original OSM metadata

**stg_countries**: Cleaned and typed country data from raw OSM
- Converts WKT strings to polygon geometry objects
- Parses population as integer
- Includes ISO 3166-1 alpha-2 and alpha-3 codes
- Preserves country metadata (capital, official name, etc.)

### Intermediate Layer

**int_cities_enriched**: Enriched city data with calculated fields
- City size categories (Megacity, Large, Medium, Small, Town)
- Boolean flags for data completeness
- Ready for analytics and visualization

**int_countries_enriched**: Enriched country data with calculated fields
- Population size categories
- Boolean flags for ISO codes and geometry availability
- Ready for geographic analysis and mapping

## Data Schema

### raw_cities (source table)
- `osm_id`: OpenStreetMap ID
- `osm_type`: Element type (node/way/relation)
- `name`: City name
- `country`: Country name
- `population`: Population (string)
- `wikidata`: Wikidata identifier
- `wikipedia`: Wikipedia reference
- `latitude`, `longitude`: Coordinates
- `geometry_wkt`: Geometry in WKT format
- `loaded_at`: Load timestamp

### raw_countries (source table)
- `osm_id`: OpenStreetMap ID
- `osm_type`: Element type (relation)
- `name`: Country name
- `iso3166_1_alpha2`: ISO 3166-1 alpha-2 code (e.g., "DE" for Germany)
- `iso3166_1_alpha3`: ISO 3166-1 alpha-3 code (e.g., "DEU" for Germany)
- `population`: Population (string)
- `capital`: Capital city name
- `wikidata`: Wikidata identifier
- `wikipedia`: Wikipedia reference
- `official_name`: Official country name
- `latitude`, `longitude`: Center coordinates
- `geometry_wkt`: Country boundary polygon in WKT format
- `loaded_at`: Load timestamp

### Transformed Models
See dbt model schema files in `transform/models/*/schema.yml`

## DuckDB Spatial Features

The pipeline uses DuckDB's spatial extension to:
- Store geometries efficiently (points for cities, polygons for countries)
- Convert WKT strings to native geometry types
- Enable spatial queries and geographic analysis
- Support distance calculations between locations

### Example Spatial Queries

```sql
-- Find cities within a bounding box
SELECT name, country, population
FROM int_cities_enriched
WHERE ST_Within(
    geometry, 
    ST_MakeEnvelope(6.0, 50.0, 7.0, 51.0)  -- Bounding box: minx, miny, maxx, maxy
)
ORDER BY population DESC;

-- Calculate distance between cities
SELECT 
    a.name as city1, 
    b.name as city2,
    a.country,
    ST_Distance(a.geometry, b.geometry) * 111 as distance_km  -- Approximate conversion
FROM int_cities_enriched a
CROSS JOIN int_cities_enriched b
WHERE a.osm_id < b.osm_id
ORDER BY distance_km
LIMIT 10;

-- Find which country a city is in (spatial join)
SELECT 
    c.name as city,
    co.name as country,
    co.iso3166_1_alpha2 as country_code
FROM int_cities_enriched c
JOIN int_countries_enriched co
    ON ST_Within(c.geometry, co.geometry)
LIMIT 20;

-- Get country statistics
SELECT 
    name,
    iso3166_1_alpha2,
    iso3166_1_alpha3,
    population,
    population_category,
    capital,
    has_geometry
FROM int_countries_enriched
WHERE population IS NOT NULL
ORDER BY population DESC
LIMIT 20;
```

### Query from DuckDB CLI

```bash
# Open DuckDB interactive shell
duckdb data/processed/osm_data.duckdb

# In DuckDB:
SELECT COUNT(*) as num_cities FROM int_cities_enriched;
SELECT COUNT(*) as num_countries FROM int_countries_enriched;
SELECT * FROM int_countries_enriched LIMIT 5;
```

## Troubleshooting

### Overpass API Timeout or Connection Issues
If the query times out or fails:
- Try running at a different time (Overpass is rate-limited and shared)
- The script will show progress and wait times
- For regional testing, add bounding box constraints to the Overpass queries

### DuckDB Spatial Extension Issues
If you encounter spatial extension errors:
```bash
# Manually install and load the extension
duckdb data/processed/osm_cities.duckdb -c "INSTALL spatial; LOAD spatial;"

# Or in Python:
import duckdb
conn = duckdb.connect('data/processed/osm_cities.duckdb')
conn.execute("INSTALL spatial; LOAD spatial;")
```

### Memory Issues with Large Datasets
If Python runs out of memory during ingestion:
- The data is processed in chunks
- Consider splitting by country or region
- Increase available RAM or reduce the query scope

### dbt Errors
```bash
# Validate dbt project syntax
cd transform && dbt parse

# Check dbt model dependencies
dbt deps

# Debug with verbose output
dbt run -d
```

## Contributing

1. Fork/clone the repository
2. Create a feature branch
3. Make changes and test locally:
   ```bash
   python src/ingestion.py
   cd transform && dbt run && dbt test
   ```
4. Commit with clear messages
5. Push and create a pull request

## Future Enhancements

- [ ] Add bounding box parameter for regional downloads
- [ ] Implement incremental/delta updates for existing data
- [ ] Extract additional place types (towns, villages, suburbs)
- [ ] Include detailed administrative boundaries (states, provinces)
- [ ] Add data quality metrics and validation tests
- [ ] Create visualization dashboard (e.g., with DuckDB + Observable)
- [ ] Support for other geospatial data sources (Natural Earth, etc.)
- [ ] Performance optimizations for very large datasets
- [ ] Automated scheduled pipeline runs (Airflow, Prefect, etc.)

## License

MIT

## Resources

- [OpenStreetMap](https://www.openstreetmap.org/)
- [Overpass API Documentation](https://wiki.openstreetmap.org/wiki/Overpass_API)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial)

