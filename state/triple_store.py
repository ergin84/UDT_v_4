from datetime import datetime
import os
import re
import pytz
from shapely import wkt
from shapely.geometry import LineString, Point
import geopandas as gpd
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, JSON, BASIC
from rdflib import Graph, Literal, Namespace, RDF, RDFS
from rdflib.namespace import OWL, XSD, URIRef
from dotenv import load_dotenv

# Define namespaces
OSM = Namespace("http://openstreetmap.org/")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
EXT = Namespace("https://www.extract-project.eu/ontology#")
SF = Namespace("http://www.opengis.net/ont/sf#")
OTN = Namespace("http://www.pms.ifi.uni-muenchen.de/OTN#")
SOSA = Namespace("http://www.w3.org/ns/sosa#")
TIME = Namespace("http://www.w3.org/2006/time#")


class TripleStoreHandler:
    def __init__(self, virtuoso_url=None, virtuoso_user=None, virtuoso_pwd=None):
        self.graph = Graph()
        self.namespace = Namespace("https://www.extract-project.eu/ontology#")
        self.bind_namespaces()
        self.virtuoso_url = virtuoso_url
        self.virtuoso_user = virtuoso_user
        self.virtuoso_pwd = virtuoso_pwd

    def connect_virtuoso(self):
        if not (self.virtuoso_url and self.virtuoso_user and self.virtuoso_pwd):
            print("Missing Virtuoso credentials!")
            return None

        sparql = SPARQLWrapper(self.virtuoso_url)
        sparql.setHTTPAuth(DIGEST)
        sparql.setCredentials(self.virtuoso_user, self.virtuoso_pwd)
        sparql.method = POST
        return sparql

    # Function to execute a SPARQL query and return results
    def execute_sparql_query(self, query):
        sparql = self.connect_virtuoso()
        if sparql is None:
            raise ConnectionError(
                "Unable to connect to Virtuoso. Please check your credentials and connection details.")

        try:
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            return sparql.query().convert()
        except Exception as e:
            raise Exception(f"SPARQL query execution failed: {e}")

    # Bind namespaces
    def bind_namespaces(self):
        self.graph.bind("EXT", self.namespace)
        self.graph.bind("rdf", RDF)
        self.graph.bind("rdfs", RDFS)
        self.graph.bind("owl", OWL)
        self.graph.bind("osm", OSM)
        self.graph.bind("otn", OTN)
        self.graph.bind("sf", SF)
        self.graph.bind("geo", GEO)
        self.graph.bind("sosa", SOSA)
        self.graph.bind("time", TIME)

    def store_ontology(self, ontology_path):
        try:
            ontology_graph = Graph()
            ontology_graph.parse(ontology_path, format="turtle")
            self.graph += ontology_graph
            print("Ontology stored in the triple store.")
        except Exception as e:
            print(f"Failed to store ontology: {str(e)}")

    def save_to_virtuoso(self):
        sparql = self.connect_virtuoso()
        if sparql is None:
            return

        query = f"INSERT DATA {{ GRAPH <https://extract-project.eu/road_schema> {{ {self.graph.serialize(format='turtle')} }} }}"

        # print(query)
       # print(self.graph.serialize(format='turtle'))
        self.graph.serialize(destination="observation_ontology.ttl", format="turtle")

        sparql.setQuery(query)

        try:
            sparql.query()
            print("Device Observations - Data saved to Virtuoso successfully.")
        except Exception as e:
            print(f"Device Observations - Failed to save data to Virtuoso: {str(e)}")


if __name__ == "__main__":
    load_dotenv()
    triple_store = TripleStoreHandler(virtuoso_url="http://localhost:5556/sparql-auth", virtuoso_user=os.getenv("VIRTUOSO_USER"),
                                      virtuoso_pwd=os.getenv("VIRTUOSO_PWD"))

    device_data = [
        {
            "device_id": "device001",
            "latitude": 41.123,
            "longitude": -71.123,
            "time": "2024-01-12T10:00:00Z",
            "speed": 30,
            "direction": 270
        },
    ]

    triple_store.insert_device_observations(device_data)

    ontology_path = "my_onto.ttl"
    triple_store.store_ontology(ontology_path)

    triple_store.connect_virtuoso()

    triple_store.save_to_virtuoso()
