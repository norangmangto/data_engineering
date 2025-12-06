import pandas as pd
import os
from pathlib import Path
import random
from datetime import datetime, timedelta

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"

def generate_gtfs():
    gtfs_dir = RAW_DIR / "gtfs"
    gtfs_dir.mkdir(parents=True, exist_ok=True)

    # helper to write csv
    def write_csv(name, data):
        pd.DataFrame(data).to_csv(gtfs_dir / name, index=False)

    # 1. agency.txt
    write_csv("agency.txt", [{
        "agency_id": "VRR",
        "agency_name": "Verkehrsverbund Rhein-Ruhr",
        "agency_url": "http://www.vrr.de",
        "agency_timezone": "Europe/Berlin"
    }])

    # 2. stops.txt (Düsseldorf center approx coords)
    stops = [
        {"stop_id": "S1", "stop_name": "Düsseldorf Hbf", "stop_lat": 51.2198, "stop_lon": 6.7943},
        {"stop_id": "S2", "stop_name": "Heinrich-Heine-Allee", "stop_lat": 51.2255, "stop_lon": 6.7776},
        {"stop_id": "S3", "stop_name": "Bilk S", "stop_lat": 51.2064, "stop_lon": 6.7725},
        {"stop_id": "S4", "stop_name": "Uni Ost/Botanischer Garten", "stop_lat": 51.1895, "stop_lon": 6.8021}
    ]
    write_csv("stops.txt", stops)

    # 3. routes.txt
    routes = [
        {"route_id": "R1", "route_short_name": "U79", "route_long_name": "Uni - Hbf - Duisburg", "route_type": 1}, # Subway
        {"route_id": "R2", "route_short_name": "707", "route_long_name": "Unterrath - Hbf - Uni", "route_type": 0}  # Tram
    ]
    write_csv("routes.txt", routes)

    # 4. calendar.txt
    write_csv("calendar.txt", [{
        "service_id": "daily",
        "monday": 1, "tuesday": 1, "wednesday": 1, "thursday": 1, "friday": 1, "saturday": 1, "sunday": 1,
        "start_date": "20250101", "end_date": "20251231"
    }])

    # 5. trips.txt
    trips = [
        {"route_id": "R1", "service_id": "daily", "trip_id": "T1", "trip_headsign": "Duisburg"},
        {"route_id": "R2", "service_id": "daily", "trip_id": "T2", "trip_headsign": "Uni"}
    ]
    write_csv("trips.txt", trips)

    # 6. stop_times.txt
    stop_times = [
        # Trip 1: Hbf -> Heine -> Bilk (Imaginary route for mock)
        {"trip_id": "T1", "arrival_time": "08:00:00", "departure_time": "08:00:00", "stop_id": "S1", "stop_sequence": 1},
        {"trip_id": "T1", "arrival_time": "08:05:00", "departure_time": "08:05:00", "stop_id": "S2", "stop_sequence": 2},
        {"trip_id": "T1", "arrival_time": "08:12:00", "departure_time": "08:12:00", "stop_id": "S3", "stop_sequence": 3},

        # Trip 2: Bilk -> Uni
        {"trip_id": "T2", "arrival_time": "08:15:00", "departure_time": "08:15:00", "stop_id": "S3", "stop_sequence": 1},
        {"trip_id": "T2", "arrival_time": "08:25:00", "departure_time": "08:25:00", "stop_id": "S4", "stop_sequence": 2}
    ]
    write_csv("stop_times.txt", stop_times)

    print("Mock GTFS generated.")

def generate_accidents():
    accidents_dir = RAW_DIR / "accidents"
    accidents_dir.mkdir(parents=True, exist_ok=True)

    # Mock accidents near Hbf and Bilk
    data = []
    for _ in range(50):
        # Cluster around Hbf
        lat = 51.2198 + random.uniform(-0.005, 0.005)
        lon = 6.7943 + random.uniform(-0.005, 0.005)
        data.append({
            "UJAHR": 2024,
            "UMONAT": random.randint(1, 12),
            "USTUNDE": random.randint(0, 23),
            "WOCHENTAG": random.randint(1, 7),
            "UKATEGORIE": random.choice([1, 2, 3]), # 1=Fatal, 2=Serious, 3=Minor
            "XGCSWGS84": lon,
            "YGCSWGS84": lat
        })

    pd.DataFrame(data).to_csv(accidents_dir / "Unfallorte.csv", index=False)
    print("Mock Accidents generated.")

def generate_traffic():
    traffic_dir = RAW_DIR / "traffic"
    traffic_dir.mkdir(parents=True, exist_ok=True)

    # Mock traffic counts
    data = [
        {"strassenname": "Friedrich-Ebert-Straße", "dtv_kfz": 25000, "lat": 51.2200, "lon": 6.7900},
        {"strassenname": "Heinrich-Heine-Allee", "dtv_kfz": 15000, "lat": 51.2255, "lon": 6.7776},
        {"strassenname": "Universitätsstraße", "dtv_kfz": 12000, "lat": 51.1900, "lon": 6.7950}
    ]
    pd.DataFrame(data).to_csv(traffic_dir / "traffic_counts.csv", index=False)
    print("Mock Traffic generated.")

if __name__ == "__main__":
    generate_gtfs()
    generate_accidents()
    generate_traffic()
