"""
OSM Cities Data Ingestion
Downloads OpenStreetMap data and extracts city information including names and polygons.
"""

import requests
import os
import json
import duckdb
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OSMCitiesIngestion:
    """Download and process OpenStreetMap city data."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.db_path = self.processed_dir / "osm_data.duckdb"
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def is_data_recent(self, file_path: Path, max_age_days: int = 7) -> bool:
        """
        Check if a data file exists and is recent.
        
        Args:
            file_path: Path to the data file
            max_age_days: Maximum age in days before considering data stale
        
        Returns:
            True if file exists and is recent, False otherwise
        """
        if not file_path.exists():
            return False
        
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        age = datetime.now() - file_mtime
        
        return age < timedelta(days=max_age_days)
    
    def download_osm_data(self, data_type: str = "cities", country: str = None, force: bool = False, max_age_days: int = 7) -> Path:
        """
        Download OSM data using Overpass API.
        
        Args:
            data_type: Type of data to download ('cities' or 'countries')
            country: Specific country name to filter (optional, for cities)
            force: Force re-download even if recent data exists
            max_age_days: Maximum age in days before re-downloading
        
        Returns:
            Path to downloaded data file
        """
        logger.info(f"Downloading OSM {data_type} data{' for ' + country if country else ''}...")
        
        if data_type == "cities":
            # Overpass API query to get cities (places tagged as city)
            query = """
            [out:json][timeout:300];
            (
              node["place"="city"];
              way["place"="city"];
              relation["place"="city"];
            );
            out geom;
            """
            output_file = self.raw_dir / "cities.json"
        elif data_type == "countries":
            # Overpass API query to get countries
            query = """
            [out:json][timeout:300];
            (
              relation["boundary"="administrative"]["admin_level"="2"];
            );
            out geom;
            """
            output_file = self.raw_dir / "countries.json"
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        # Check if recent data already exists
        if not force and self.is_data_recent(output_file, max_age_days):
            file_age = datetime.now() - datetime.fromtimestamp(output_file.stat().st_mtime)
            logger.info(f"Recent {data_type} data found (age: {file_age.days} days, {file_age.seconds // 3600} hours). Skipping download.")
            logger.info(f"Using cached file: {output_file}")
            logger.info(f"To force re-download, use --force flag or delete the file.")
            return output_file
        
        logger.info(f"Downloading OSM {data_type} data{' for ' + country if country else ''}...")
        overpass_url = "https://overpass-api.de/api/interpreter"
        
        try:
            response = requests.post(
                overpass_url,
                data={"data": query},
                timeout=600
            )
            response.raise_for_status()
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=2)
            
            logger.info(f"Downloaded data to {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Error downloading OSM data: {e}")
            raise
    
    def parse_geometry(self, element: Dict[str, Any]) -> str:
        """
        Parse OSM element geometry into WKT format.
        
        Args:
            element: OSM element dictionary
        
        Returns:
            WKT string representation of geometry
        """
        element_type = element.get('type')
        
        if element_type == 'node':
            # Point geometry
            lat = element.get('lat')
            lon = element.get('lon')
            if lat and lon:
                return f"POINT({lon} {lat})"
        
        elif element_type == 'way':
            # LineString or Polygon
            if 'geometry' in element:
                coords = [(node['lon'], node['lat']) for node in element['geometry']]
                if len(coords) > 2:
                    # Check if it's closed (polygon)
                    if coords[0] == coords[-1]:
                        coord_str = ', '.join([f"{lon} {lat}" for lon, lat in coords])
                        return f"POLYGON(({coord_str}))"
                    else:
                        # For cities, we'll create a simple point from the first coordinate
                        return f"POINT({coords[0][0]} {coords[0][1]})"
        
        elif element_type == 'relation':
            # For relations (complex polygons), extract outer members
            if 'members' in element:
                # For simplicity, take the center point if available
                if 'center' in element:
                    lon = element['center'].get('lon')
                    lat = element['center'].get('lat')
                    if lon and lat:
                        return f"POINT({lon} {lat})"
        
        return None
    
    def process_osm_data(self, input_file: Path, data_type: str = "cities") -> List[Dict[str, Any]]:
        """
        Process downloaded OSM data to extract city or country information.
        
        Args:
            input_file: Path to the downloaded JSON file
            data_type: Type of data to process ('cities' or 'countries')
        
        Returns:
            List of city or country records
        """
        logger.info(f"Processing OSM {data_type} data from {input_file}...")
        
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        records = []
        elements = data.get('elements', [])
        
        logger.info(f"Found {len(elements)} {data_type} elements")
        
        if data_type == "cities":
            records = self._process_cities(elements)
        elif data_type == "countries":
            records = self._process_countries(elements)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        logger.info(f"Processed {len(records)} {data_type}")
        return records
    
    def _process_cities(self, elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process city elements from OSM data."""
        cities = []
        
        for element in elements:
            tags = element.get('tags', {})
            name = tags.get('name')
            
            if not name:
                continue
            
            city_record = {
                'osm_id': element.get('id'),
                'osm_type': element.get('type'),
                'name': name,
                'country': tags.get('addr:country') or tags.get('is_in:country') or 'Unknown',
                'population': tags.get('population'),
                'wikidata': tags.get('wikidata'),
                'wikipedia': tags.get('wikipedia'),
                'geometry_wkt': self.parse_geometry(element)
            }
            
            # Add coordinates for point features
            if element.get('type') == 'node':
                city_record['latitude'] = element.get('lat')
                city_record['longitude'] = element.get('lon')
            elif 'center' in element:
                city_record['latitude'] = element['center'].get('lat')
                city_record['longitude'] = element['center'].get('lon')
            
            cities.append(city_record)
        
        return cities
    
    def _process_countries(self, elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process country elements from OSM data."""
        countries = []
        
        for element in elements:
            tags = element.get('tags', {})
            name = tags.get('name')
            
            if not name:
                continue
            
            country_record = {
                'osm_id': element.get('id'),
                'osm_type': element.get('type'),
                'name': name,
                'iso3166_1_alpha2': tags.get('ISO3166-1:alpha2'),
                'iso3166_1_alpha3': tags.get('ISO3166-1:alpha3'),
                'population': tags.get('population'),
                'capital': tags.get('capital'),
                'wikidata': tags.get('wikidata'),
                'wikipedia': tags.get('wikipedia'),
                'official_name': tags.get('official_name'),
                'geometry_wkt': self.parse_geometry(element)
            }
            
            # Add coordinates for point features
            if element.get('type') == 'node':
                country_record['latitude'] = element.get('lat')
                country_record['longitude'] = element.get('lon')
            elif 'center' in element:
                country_record['latitude'] = element['center'].get('lat')
                country_record['longitude'] = element['center'].get('lon')
            
            countries.append(country_record)
        
        return countries
    
    def load_to_duckdb(self, records: List[Dict[str, Any]], data_type: str = "cities") -> None:
        """
        Load city or country data into DuckDB.
        
        Args:
            records: List of city or country records
            data_type: Type of data to load ('cities' or 'countries')
        """
        logger.info(f"Loading {len(records)} {data_type} to DuckDB...")
        
        conn = duckdb.connect(str(self.db_path))
        
        try:
            # Install and load spatial extension
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            
            if data_type == "cities":
                self._load_cities_to_duckdb(conn, records)
            elif data_type == "countries":
                self._load_countries_to_duckdb(conn, records)
            else:
                raise ValueError(f"Unknown data type: {data_type}")
            
        finally:
            conn.close()
    
    def _load_cities_to_duckdb(self, conn: duckdb.DuckDBPyConnection, cities: List[Dict[str, Any]]) -> None:
        """Load city data into DuckDB."""
        # Create raw cities table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_cities (
                osm_id BIGINT,
                osm_type VARCHAR,
                name VARCHAR,
                country VARCHAR,
                population VARCHAR,
                wikidata VARCHAR,
                wikipedia VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                geometry_wkt VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert data
        if cities:
            conn.executemany("""
                INSERT INTO raw_cities (
                    osm_id, osm_type, name, country, population,
                    wikidata, wikipedia, latitude, longitude, geometry_wkt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    c['osm_id'], c['osm_type'], c['name'], c['country'],
                    c['population'], c['wikidata'], c['wikipedia'],
                    c.get('latitude'), c.get('longitude'), c['geometry_wkt']
                ) for c in cities
            ])
        
        # Get count
        count = conn.execute("SELECT COUNT(*) FROM raw_cities").fetchone()[0]
        logger.info(f"Successfully loaded {count} cities to DuckDB")
    
    def _load_countries_to_duckdb(self, conn: duckdb.DuckDBPyConnection, countries: List[Dict[str, Any]]) -> None:
        """Load country data into DuckDB."""
        # Create raw countries table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_countries (
                osm_id BIGINT,
                osm_type VARCHAR,
                name VARCHAR,
                iso3166_1_alpha2 VARCHAR,
                iso3166_1_alpha3 VARCHAR,
                population VARCHAR,
                capital VARCHAR,
                wikidata VARCHAR,
                wikipedia VARCHAR,
                official_name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                geometry_wkt VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert data
        if countries:
            conn.executemany("""
                INSERT INTO raw_countries (
                    osm_id, osm_type, name, iso3166_1_alpha2, iso3166_1_alpha3,
                    population, capital, wikidata, wikipedia, official_name,
                    latitude, longitude, geometry_wkt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    c['osm_id'], c['osm_type'], c['name'], c['iso3166_1_alpha2'],
                    c['iso3166_1_alpha3'], c['population'], c['capital'],
                    c['wikidata'], c['wikipedia'], c['official_name'],
                    c.get('latitude'), c.get('longitude'), c['geometry_wkt']
                ) for c in countries
            ])
        
        # Get count
        count = conn.execute("SELECT COUNT(*) FROM raw_countries").fetchone()[0]
        logger.info(f"Successfully loaded {count} countries to DuckDB")
    
    def run(self, country: str = None, force: bool = False, max_age_days: int = 7) -> None:
        """
        Run the complete ingestion pipeline.
        
        Args:
            country: Optional country filter (for cities)
            force: Force re-download even if recent data exists
            max_age_days: Maximum age in days before re-downloading data
        """
        logger.info("Starting OSM cities and countries ingestion pipeline...")
        
        # Download and process cities
        logger.info("\n=== Processing Cities ===")
        osm_cities_file = self.download_osm_data(data_type="cities", country=country, force=force, max_age_days=max_age_days)
        cities = self.process_osm_data(osm_cities_file, data_type="cities")
        self.load_to_duckdb(cities, data_type="cities")
        
        # Download and process countries
        logger.info("\n=== Processing Countries ===")
        osm_countries_file = self.download_osm_data(data_type="countries", force=force, max_age_days=max_age_days)
        countries = self.process_osm_data(osm_countries_file, data_type="countries")
        self.load_to_duckdb(countries, data_type="countries")
        
        logger.info("\nIngestion pipeline completed successfully!")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest OSM cities data')
    parser.add_argument(
        '--country',
        type=str,
        help='Filter by specific country',
        default=None
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        help='Data directory path',
        default='data'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-download even if recent data exists'
    )
    parser.add_argument(
        '--max-age-days',
        type=int,
        help='Maximum age in days before re-downloading data (default: 7)',
        default=7
    )
    
    args = parser.parse_args()
    
    ingestion = OSMCitiesIngestion(data_dir=args.data_dir)
    ingestion.run(country=args.country, force=args.force, max_age_days=args.max_age_days)


if __name__ == "__main__":
    main()
