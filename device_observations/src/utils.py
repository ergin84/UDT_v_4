import datetime
import os
import pickle
import urllib.parse
from dotenv import load_dotenv
import redis
from shapely import wkt, Polygon
from shapely.geometry import Point
import geopandas as gpd
import pandas as pd
import osmnx as ox
import queries as queries
from triple_store import TripleStoreHandler
import networkx as nx
from shapely.wkt import loads
from pyproj import CRS

# Redis configuration
REDIS_HOST = os.getenv("REDIS_SERVICE_SERVICE_HOST", "localhost")  
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379 ))
REDIS_DB = 0  # DB number

load_dotenv()
# Set up the triple store
if os.getenv("VIRTUOSO_SERVICE_SERVICE_HOST"):
    virtuoso_service_host = os.getenv("VIRTUOSO_SERVICE_SERVICE_HOST")
    virtuoso_service_port = os.getenv("VIRTUOSO_SERVICE_SERVICE_PORT", "8890")  # Assuming default port if not specified
    virtuoso_url = f"http://{virtuoso_service_host}:{virtuoso_service_port}/sparql-auth"
else:
    # Fallback to localhost settings
    virtuoso_url = "http://localhost:5556/sparql-auth"

# Environment variables in Kubernetes deployment
virtuoso_user = "dba"  # Default to 'dba' if not set
virtuoso_pwd = os.getenv("DBA_PASSWORD", "ergin")  # Default password

# Initialize TripleStoreHandler
triple_store = TripleStoreHandler(
    virtuoso_url=virtuoso_url,
    virtuoso_user=virtuoso_user,
    virtuoso_pwd="ergin")

def get_redis_connection():
    return redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)

def load_graph_from_redis(redis_key='projected_graph'):
    """Load a graph from Redis using the specified key."""
    r = get_redis_connection()
    serialized_graph = r.get(redis_key)
    if serialized_graph:
        print("Loading graph from Redis.")
        return pickle.loads(serialized_graph)
    else:
        print("Graph not found in Redis.")
        return None

G_projected = load_graph_from_redis()

if G_projected:
    # Save the graph as a GraphML file if it was loaded successfully
    ox.save_graphml(G_projected, filepath="./road_graph3.graphml")
    print("Graph saved successfully.")
else:
    print("No graph loaded from Redis, skipping save.")

# Execute the road query
road_data = triple_store.execute_sparql_query(queries.road_query4)
node_data = triple_store.execute_sparql_query(queries.node_query4)

# Parse the road data into a DataFrame
road_df = pd.DataFrame({
    "road_iri": [result["road"]["value"] for result in road_data["results"]["bindings"]],
    "start_node_id": [result["startNodeId"]["value"] for result in road_data["results"]["bindings"]],
    "start_node_iri": [result["startNode"]["value"] for result in road_data["results"]["bindings"]],
    "end_node_id": [result["endNodeId"]["value"] for result in road_data["results"]["bindings"]],
    "end_node_iri": [result["endNode"]["value"] for result in road_data["results"]["bindings"]],
    "geometry": [loads(result["geometry"]["value"]) for result in road_data["results"]["bindings"]]
})

# Convert DataFrame to GeoDataFrame and set CRS
road_gdf = gpd.GeoDataFrame(road_df, geometry='geometry', crs="EPSG:32619")

# Parse the road data into a DataFrame
node_df = pd.DataFrame({
    "node_iri": [result["node"]["value"] for result in node_data["results"]["bindings"]],
    "geometry": [loads(result["geometry"]["value"]) for result in node_data["results"]["bindings"]]
})

# Convert DataFrame to GeoDataFrame and set CRS
node_gdf = gpd.GeoDataFrame(node_df, geometry='geometry', crs="EPSG:32619")

# Project to a suitable CRS for distance calculation (e.g., a local UTM zone)
projected_crs = CRS.from_user_input("EPSG:32619")  # Example: UTM zone for Italy
road_gdf_projected = road_gdf.to_crs(projected_crs)
node_gdf_projected = node_gdf.to_crs(projected_crs)


# Constants for distance thresholds
NODE_THRESHOLD = 5  # meters
ROAD_THRESHOLD = 4  # meters


# Find nearest road for each point
def find_nearest_road_or_node(latitude, longitude):
    point = Point(longitude, latitude)
    # Project the point to the same CRS as the road and node GeoDataFrames
    point_gdf = gpd.GeoDataFrame([{'geometry': point}], crs=road_gdf.crs)
    
    # Calculate distance to all roads
    road_gdf['distance_to_point'] = road_gdf.distance(point_gdf.iloc[0].geometry)
    nearest_road = road_gdf.loc[road_gdf['distance_to_point'].idxmin()]
    
    # If the nearest road is within the ROAD_THRESHOLD, check distance to start and end nodes
    if nearest_road['distance_to_point'] <= ROAD_THRESHOLD:
        # Retrieve start and end nodes' geometries
        start_node_geom = node_gdf.loc[node_gdf['node_iri'] == nearest_road['start_node_iri']].geometry.iloc[0]
        end_node_geom = node_gdf.loc[node_gdf['node_iri'] == nearest_road['end_node_iri']].geometry.iloc[0]
        
        # Calculate distance from the point to start and end nodes
        distance_to_start_node = point_gdf.distance(start_node_geom).iloc[0]
        distance_to_end_node = point_gdf.distance(end_node_geom).iloc[0]
        
        # Check if either node is within NODE_THRESHOLD
        if distance_to_start_node <= NODE_THRESHOLD:
            return nearest_road['start_node_iri']
        elif distance_to_end_node <= NODE_THRESHOLD:
            return nearest_road['end_node_iri']
        else:
            return nearest_road['road_iri']
    else:
        # If no road is within ROAD_THRESHOLD, return None or an appropriate value indicating no nearby road
        return None

def categorize_traffic(self, density, speed):
    if density >= 1 and density <= 2 and speed >= 0.8 and speed <= 1.2:
        return "Fluid Movement Effect"
    elif density >= 3 and density <= 4 and speed >= 0.4 and speed <= 0.8:
        return "Stop and Go Movement"
    elif density >= 5 and density <= 6 and speed >= 0.4 and speed <= 0.5:
         return "Congested Effect"
    elif density > 6 and speed <= 0.4:
        return "Turbulence Effect"
    else:
        return "Unknown"
    
def timestamp_to_url_string(timestamp):
    # Convert timestamp to a string in ISO format, including milliseconds
    # Replace colons with nothing and the period before ms with an underscore
    timestamp_str = timestamp.replace(':', '').replace('.', '_')
    
    # URL-encode the string to ensure it's fully compatible for use in a URL
    url_encoded_timestamp = urllib.parse.quote_plus(timestamp_str)
    
    return url_encoded_timestamp


# Other utility functions...
