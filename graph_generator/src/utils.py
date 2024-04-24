import os
import networkx as nx
from rdflib import Graph, URIRef, Literal, Namespace, XSD
from rdflib.namespace import RDF, RDFS, OWL
from shapely import Polygon
from shapely.geometry import LineString, Point
from dotenv import load_dotenv
import redis
import pickle
import osmnx as ox

# Redis configuration
REDIS_HOST = os.getenv("REDIS_SERVICE_SERVICE_HOST", "localhost")  
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379 ))

# Define your polygon coordinates for the OSM area
cords = [(12.3047, 45.4455), (12.2995, 45.4389),
         (12.3052, 45.4368), (12.3084, 45.4411),
         (12.3140, 45.4418), (12.3061, 45.4463),
         (12.3047, 45.4455)]
polygon = Polygon(cords)

def generate_road_id(edge):
    """
    Generates a road ID based on the OSM IDs from the edge attributes.

    Args:
        edge (tuple): The graph edge representing the road element.

    Returns:
        str: A unique road ID generated from OSM IDs.
    """
    osm_ids = edge[2].get('osmid', [])

    # Ensure osm_ids is a list
    if isinstance(osm_ids, int):
        osm_ids = [osm_ids]  # Wrap a single integer ID in a list
    elif isinstance(osm_ids, str):
        # Assuming osm_ids are separated by commas in the string
        osm_ids = [osm_id.strip() for osm_id in osm_ids.split(',')]
    elif not isinstance(osm_ids, list):
        raise ValueError("osm_ids must be an int, str, or list.")

    # Convert each ID to string and join with underscore
    base_id = '_'.join(str(osm_id) for osm_id in osm_ids)

    return f"{base_id}"



def create_wkt_point(node_id, data):
    lat = data['y']
    lon = data['x']
    return Point(lon, lat)

def generate_road_element_id(edge, counter_dict):
    osm_ids = edge[2].get('osmid', [])

    # Ensure osm_ids is a list
    if isinstance(osm_ids, int):
        osm_ids = [osm_ids]  # Convert a single integer ID into a list
    elif isinstance(osm_ids, str):
        # Assuming osm_ids are separated by commas in the string
        osm_ids = [int(osm_id) for osm_id in osm_ids.split(',') if osm_id.isdigit()]

    # Normalize and concatenate OSM IDs to form the base_id
    base_id = '_'.join(str(osm_id).strip() for osm_id in osm_ids)

    # Increment the count for this base_id and append it to form the unique ID
    counter_dict[base_id] = counter_dict.get(base_id, 0) + 1
    return f"{base_id}_{counter_dict[base_id]}"



def create_geometry_from_edge_data(edge_data, source_node, target_node, graph):
    if 'geometry' in edge_data:  # Check if geometry data is present
        linestring = edge_data['geometry']
        start_index = linestring.find('(') + 1
        end_index = linestring.find(')')
        if start_index > 0 and end_index > 0:
            coords_string = linestring[start_index:end_index]
            coords_pairs = coords_string.split(', ')
            coords = [(float(lon), float(lat)) for lon, lat in (pair.split(' ') for pair in coords_pairs)]
            return LineString(coords).wkt
    else:
        # Check if source and target nodes have valid coordinates
        if 'x' in graph.nodes[source_node] and 'y' in graph.nodes[source_node] and \
                'x' in graph.nodes[target_node] and 'y' in graph.nodes[target_node]:
            source_coords = (float(graph.nodes[source_node]['x']), float(graph.nodes[source_node]['y']))
            target_coords = (float(graph.nodes[target_node]['x']), float(graph.nodes[target_node]['y']))
            return LineString([source_coords, target_coords]).wkt
    return None

def create_wkt_point(node_id, data):
    lat = data['y']
    lon = data['x']
    return Point(lon, lat)

def estimate_width(attributes):
    average_lane_width = 3.1  # meters
    default_footway_width = 2  # meters
    default_service_road_width = 3  # meters
    default_one_way_width = 3.1  # meters
    default_two_way_width = 6  # meters

    # Use the width if available
    if 'width' in attributes:
        try:
            return float(attributes['width'])
        except ValueError:
            pass

    # If lanes are specified, use lanes to estimate width
    if 'lanes' in attributes:
        lanes_value = attributes['lanes']
        try:
            if isinstance(lanes_value, list):
                # If lanes_value is a list, calculate the average
                lanes_list = [int(lane) for lane in lanes_value]
                average_lanes = sum(lanes_list) / len(lanes_list)
                return average_lanes * average_lane_width
            elif isinstance(lanes_value, str):
                # Handle string values (single number, comma-separated list, or range)
                if ',' in lanes_value:
                    lanes_list = [int(lane.strip()) for lane in lanes_value.split(',')]
                    average_lanes = sum(lanes_list) / len(lanes_list)
                    return average_lanes * average_lane_width
                elif '-' in lanes_value:
                    lanes_range = [int(lane.strip()) for lane in lanes_value.split('-')]
                    average_lanes = sum(range(lanes_range[0], lanes_range[1] + 1)) / len(range(lanes_range[0], lanes_range[1] + 1))
                    return average_lanes * average_lane_width
                else:
                    return float(lanes_value) * average_lane_width
            else:
                # Directly handle numeric values
                return float(lanes_value) * average_lane_width
        except ValueError:
            pass

    # Use defaults based on highway type
    highway_type = attributes.get('highway', '')
    if highway_type == 'footway':
        return default_footway_width
    elif highway_type == 'service':
        return default_service_road_width

    # Use one-way or two-way defaults
    if attributes.get('oneway', 'False') == 'True':
        return default_one_way_width
    else:
        return default_two_way_width
    
def length_cal(attributes):
    if 'length' in attributes:
        return attributes['length']
    else:
        return 0
    
def load_graphml(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"GraphML file not found: {file_path}")
    try:
        return nx.read_graphml(file_path)
    except Exception as e:
        raise Exception(f"Error reading GraphML file: {e}")

def create_road_graph():
    G = ox.graph_from_polygon(polygon, network_type='all', retain_all=True, simplify=True, truncate_by_edge=True)
    #G_projected = ox.project_graph(G)
    return G
    
def graphs_are_equivalent(graph1, graph2):
    # This function checks if two graphs are structurally identical.
    return nx.is_isomorphic(graph1, graph2)

def save_graph_if_different(graph, filepath="graph_generator/road_graph.graphml"):
    if os.path.exists(filepath):
        existing_graph = load_graphml(filepath)
        if graphs_are_equivalent(graph, existing_graph):
            print("Graphs are identical. No update needed.")
            return False
    # If the graph is new or different, save it.
    ox.save_graphml(graph, filepath)
    print("Graph saved/updated.")
    return True
    
def store_graph_in_redis(graph, redis_host=REDIS_HOST, redis_port=6379, redis_db=0):
    r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    serialized_graph = pickle.dumps(graph)
    r.set('projected_graph', serialized_graph)