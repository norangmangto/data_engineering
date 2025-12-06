import duckdb
import networkx as nx
import pandas as pd
from datetime import datetime
import math
from typing import List, Dict, Optional, Tuple

DB_PATH = "data/processed/transport.duckdb"

class TransportRouter:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.stops = {}  # stop_id -> {lat, lon, name, risk_score}
        self.loaded = False

    def load_graph(self):
        print("Loading transport graph...")
        con = duckdb.connect(DB_PATH)

        # 1. Load Stops and Risk Scores
        print("Fetching stops and risk scores...")
        stops_df = con.sql("""
            SELECT
                s.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                coalesce(r.risk_score, 0) as risk_score
            FROM stg_gtfs_stops s
            LEFT JOIN int_network_risk r ON s.stop_id = r.stop_id
        """).df()

        for _, row in stops_df.iterrows():
            self.stops[row['stop_id']] = {
                'name': row['stop_name'],
                'lat': row['stop_lat'],
                'lon': row['stop_lon'],
                'risk_score': row['risk_score']
            }
            self.graph.add_node(row['stop_id'], **self.stops[row['stop_id']])

        # 2. Load Connections (Edges)
        # We aggregate multiple trips into a single "average" edge for static routing
        # In a full system, we'd use time-dependent graphs (RAPTOR/CSA)
        print("Building edges from stop_times...")
        edges_query = """
            WITH segments AS (
                SELECT
                    t.route_id,
                    st1.stop_id as from_stop,
                    st2.stop_id as to_stop,
                    -- Parse time 'HH:MM:SS' roughly to seconds (ignoring >24h for now)
                    date_part('hour', CAST(st2.arrival_time AS TIME)) * 3600 +
                    date_part('minute', CAST(st2.arrival_time AS TIME)) * 60 +
                    date_part('second', CAST(st2.arrival_time AS TIME))
                    -
                    (date_part('hour', CAST(st1.departure_time AS TIME)) * 3600 +
                    date_part('minute', CAST(st1.departure_time AS TIME)) * 60 +
                    date_part('second', CAST(st1.departure_time AS TIME))) as duration_sec
                FROM stg_gtfs_stop_times st1
                JOIN stg_gtfs_stop_times st2 ON st1.trip_id = st2.trip_id AND st1.stop_sequence + 1 = st2.stop_sequence
                JOIN stg_gtfs_trips t ON st1.trip_id = t.trip_id
            )
            SELECT
                from_stop,
                to_stop,
                avg(duration_sec) as avg_duration,
                mode(route_id) as route_id
            FROM segments
            GROUP BY 1, 2
        """
        segments_df = con.sql(edges_query).df()

        count = 0
        for _, row in segments_df.iterrows():
            u, v = row['from_stop'], row['to_stop']
            duration = row['avg_duration']

            # Risk Penalty:
            # We want to penalize arriving at a risky stop.
            # Weight = Duration * (1 + RiskFactor)
            # RiskFactor = (risk_score of TARGET node) * 0.1 (arbitrary scaling)
            target_risk = self.stops[v]['risk_score']
            weight = duration * (1 + (target_risk * 0.1))

            self.graph.add_edge(
                u, v,
                weight=weight,
                duration=duration,
                route_id=row['route_id'],
                raw_risk=target_risk
            )
            count += 1

        con.close()
        self.loaded = True
        print(f"Graph loaded: {self.graph.number_of_nodes()} stops, {count} segments.")

    def find_nearest_stop(self, lat: float, lon: float) -> str:
        # Simple Euclidean approximation for speed
        best_stop = None
        min_dist = float('inf')

        for stop_id, data in self.stops.items():
            dist = (data['lat'] - lat)**2 + (data['lon'] - lon)**2
            if dist < min_dist:
                min_dist = dist
                best_stop = stop_id

        return best_stop

    def get_route(self, start_lat: float, start_lon: float, end_lat: float, end_lon: float):
        if not self.loaded:
            self.load_graph()

        start_node = self.find_nearest_stop(start_lat, start_lon)
        end_node = self.find_nearest_stop(end_lat, end_lon)

        if not start_node or not end_node:
            return None

        try:
            path = nx.shortest_path(self.graph, start_node, end_node, weight='weight')

            # Reconstruct details
            route_details = []
            total_duration = 0
            total_risk = 0

            for i in range(len(path) - 1):
                u, v = path[i], path[i+1]
                edge = self.graph[u][v]
                stop_info = self.stops[v]

                segment = {
                    "from_stop": self.stops[u]['name'],
                    "to_stop": stop_info['name'],
                    "route_id": edge['route_id'],
                    "duration_sec": edge['duration'],
                    "stop_risk_score": stop_info['risk_score']
                }
                route_details.append(segment)
                total_duration += edge['duration']
                total_risk += stop_info['risk_score']

            return {
                "start_stop": self.stops[start_node]['name'],
                "end_stop": self.stops[end_node]['name'],
                "segments": route_details,
                "total_duration_minutes": total_duration / 60,
                "total_accumulated_risk": total_risk
            }

        except nx.NetworkXNoPath:
            return None

router = TransportRouter()
