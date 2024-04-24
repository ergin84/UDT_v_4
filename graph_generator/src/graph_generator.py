import filecmp
import os
import shutil
import osmnx as ox
from shapely.geometry import Polygon
import networkx as nx
import redis
import pickle
import geopandas as gpd
from rdflib import XSD, Graph, URIRef, Literal, Namespace, RDF, RDFS, OWL
import threading
from src.utils import create_geometry_from_edge_data, create_wkt_point, estimate_width, generate_road_element_id, generate_road_id, length_cal, load_graphml, save_graph_if_different, store_graph_in_redis, graphs_are_equivalent, create_road_graph
from src.triple_store import TripleStoreHandler
from dotenv import load_dotenv

# Define Namespaces
OSM = Namespace("http://openstreetmap.org/")
EXT = Namespace("https://www.extract-project.eu/ontology#")
SF = Namespace("http://www.opengis.net/ont/sf#")
OTN = Namespace("http://www.pms.ifi.uni-muenchen.de/OTN#")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
counter_dict = {}


class GraphGeneratorHandler:
    def __init__(self, triple_store):
        self.triple_store = triple_store


    
    def process_graph_nodes(self, graph):
            #global data, geom_uri, wkt_literal
            for node_id, data in graph.nodes(data=True):
                node_id = str(node_id)
                node_uri = URIRef(EXT['Node/' + node_id])
                self.triple_store.graph.add((node_uri, RDF.type, OTN.Node))
                self.triple_store.graph.add((node_uri, RDF.type, EXT.Node))
                self.triple_store.graph.add((EXT.Node, RDFS.subClassOf, OTN.Node))
                # Create a Geometry for the node
                geom_uri = URIRef(GEO['Geometry/' + node_id])
                # add node id as integer
                node_id_literal = Literal(node_id, datatype=XSD.integer)
                self.triple_store.graph.add((node_uri, EXT.hasOSMId, node_id_literal))
                # self.triple_store.graph.add((geom_uri, RDF.type, SF.Point))
                self.triple_store.graph.add((geom_uri, RDF.type, GEO.Geometry))
                # self.triple_store.graph.add((SF.Point, RDFS.subClassOf, GEO.Geometry))
                # Add the WKT literal to the Geometry
                wkt_literal = Literal(create_wkt_point(node_id, data), datatype=GEO.wktLiteral)
                self.triple_store.graph.add((geom_uri, GEO.asWKT, wkt_literal))
                # Link the node to the Geometry
                self.triple_store.graph.add((node_uri, GEO.hasGeometry, geom_uri))
                
                
    def process_graph_edges(self, graph):
        #global data, geom_uri, wkt_literal
        for source, target, data in graph.edges(data=True):
            edge = (source, target, data)
            road_element_id = generate_road_element_id(edge, counter_dict)
            road_id = generate_road_id(edge)
            geometry = create_geometry_from_edge_data(data, source, target, graph)

            # Estimate the width of the road segment
            estimated_width = estimate_width(data)
            width_literal = Literal(estimated_width, datatype=XSD.float)

            # Length of the road segment
            road_length = length_cal(data)
            length_literal = Literal(road_length, datatype=XSD.float)

            # Estimate area of the road segment
            estimated_road_element_area = round((float(estimated_width) * float(road_length)))
            area_literal = Literal(estimated_road_element_area, datatype=XSD.float)

            road_id_uri = URIRef(OTN['Road/'] + f"{road_id}")
            road_element_id_uri = URIRef(OTN['Road_Element/'] + f"{road_element_id}")

            # Create a Geometry for the Road element
            geom_uri = URIRef(GEO['Geometry/' + road_element_id])
            self.triple_store.graph.add((geom_uri, RDF.type, GEO.Geometry))
            # self.triple_store.graph.add((SF.LineString, RDFS.subClassOf, GEO.Geometry))
            # Add the WKT literal to the Geometry
            wkt_literal = Literal(create_geometry_from_edge_data(data, source, target, graph), datatype=GEO.wktLiteral)
            self.triple_store.graph.add((geom_uri, GEO.asWKT, wkt_literal))
            # Link the node to the Geometry
            self.triple_store.graph.add((road_element_id_uri, GEO.hasGeometry, geom_uri))
            self.triple_store.graph.add((GEO.hasGeometry, RDF.type, OWL.ObjectProperty))

            # Add the estimated width to the RDF graph as a data property
            self.triple_store.graph.add((road_element_id_uri, EXT.estimatedWidth, width_literal))

            # Add the length to the RDF graph as a data property
            self.triple_store.graph.add((road_element_id_uri, EXT.length, length_literal))

            # Add the area to the RDF graph as a data property
            self.triple_store.graph.add((road_element_id_uri, EXT.area, area_literal))

            self.triple_store.graph.add((road_id_uri, RDF.type, OTN.Road))
            self.triple_store.graph.add((road_element_id_uri, RDF.type, OTN.Road_Element))
            self.triple_store.graph.add((road_id_uri, OTN.contains, road_element_id_uri))
            self.triple_store.graph.add((road_element_id_uri, OTN.starts_at, URIRef(EXT['Node/'] + str(source))))
            self.triple_store.graph.add((road_element_id_uri, OTN.ends_at, URIRef(EXT['Node/'] + str(target))))

            # add road element id
            road_element_id_literal = Literal(road_element_id, datatype=XSD.string)
            staring_node_literal = Literal(source, datatype=XSD.integer)
            ending_node_literal = Literal(target, datatype=XSD.integer)
            self.triple_store.graph.add((road_element_id_uri, EXT.hasRoadElementId, road_element_id_literal))
            self.triple_store.graph.add((road_element_id_uri, EXT.startinNode, staring_node_literal))
            self.triple_store.graph.add((road_element_id_uri, EXT.endingNode, ending_node_literal))
    
#def transform_graph_to_rdf(graph):
#    self.triple_store.graph = Graph()
#    process_graph_nodes(graph, self.triple_store.graph) 
#    process_graph_edges(graph, self.triple_store.graph)
#    return self.triple_store.graph

def main():
    # Set up the triple store
    if os.getenv("VIRTUOSO_SERVICE_SERVICE_HOST"):
        virtuoso_service_host = os.getenv("VIRTUOSO_SERVICE_SERVICE_HOST")
        virtuoso_service_port = os.getenv("VIRTUOSO_SERVICE_SERVICE_PORT", "8890")  # Assuming default port if not specified
        virtuoso_url = f"http://{virtuoso_service_host}:{virtuoso_service_port}/sparql-auth"
    else:
        # Fallback to localhost settings
        virtuoso_url = "http://localhost:5556/sparql-auth"

    # Use default values for user and password when developing locally
    # Make sure to set these environment variables in your Kubernetes deployment
    virtuoso_user = "dba"  # Default to 'dba' if not set
    virtuoso_pwd = os.getenv("DBA_PASSWORD", "ergin")  # Default password, change it as needed

    # Now use these variables to initialize your TripleStoreHandler
    triple_store = TripleStoreHandler(
        virtuoso_url=virtuoso_url,
        virtuoso_user=virtuoso_user,
        virtuoso_pwd=virtuoso_pwd)
    
    graph_generator = GraphGeneratorHandler(triple_store)
    
    # Create the road graph
    graph = create_road_graph()
    
    # Save the graph to Virtuoso and Redis
    if save_graph_if_different(graph):
        store_graph_in_redis(graph)
        # Load the GraphML file
        graphml_file_path = 'graph_generator/road_graph.graphml'
        graph_loaded = load_graphml(graphml_file_path)
        #self.triple_store.graph = transform_graph_to_rdf(graph_loaded)
        triple_store.connect_virtuoso()
        graph_generator.process_graph_nodes(graph) 
        graph_generator.process_graph_edges(graph) 
        graph.serialize(destination="road_graph.ttl", format="turtle")
        triple_store.save_to_virtuoso()
    
    
if __name__ == "__main__":
    main()
