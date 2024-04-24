import logging
from rdflib import OWL, RDF, RDFS, Graph, Namespace
from SPARQLWrapper import SPARQLWrapper, POST, JSON, DIGEST

# Define namespaces
OSM = Namespace("http://openstreetmap.org/")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
EXT = Namespace("https://www.extract-project.eu/ontology#")
SF = Namespace("http://www.opengis.net/ont/sf#")
OTN = Namespace("http://www.pms.ifi.uni-muenchen.de/OTN#")
SOSA = Namespace("http://www.w3.org/ns/sosa#")
TIME = Namespace("http://www.w3.org/2006/time#")



class TripleStoreHandler:
    def __init__(self, virtuoso_url, virtuoso_user, virtuoso_pwd):
        self.virtuoso_url = virtuoso_url
        self.virtuoso_user = virtuoso_user
        self.virtuoso_pwd = virtuoso_pwd
        self.sparql = SPARQLWrapper(self.virtuoso_url)
        self.sparql.setHTTPAuth(DIGEST)
        self.sparql.setCredentials(self.virtuoso_user, self.virtuoso_pwd)
        self.sparql.setMethod(POST)
        self.sparql.setReturnFormat(JSON)
        self.bind_namespaces()
        
    # Bind namespaces
    def bind_namespaces(self):
        self.graph = Graph()
        self.graph.bind("ext", EXT)
        self.graph.bind("rdf", RDF)
        self.graph.bind("rdfs", RDFS)
        self.graph.bind("owl", OWL)
        self.graph.bind("osm", OSM)
        self.graph.bind("otn", OTN)
        self.graph.bind("sf", SF)
        self.graph.bind("geo", GEO)
        self.graph.bind("sosa", SOSA)
        self.graph.bind("time", TIME)

    # Function to execute a SPARQL query and return results
    def execute_sparql_query(self, query, query_type="select"):
        self.sparql.setQuery(query)
        if query_type == "select":
            results = self.sparql.query().convert()
            return results
        elif query_type == "update":
            self.sparql.query()

    def bulk_insert(self, graphs):
        combined_graph = Graph()
        for graph in graphs:
            combined_graph += graph  # Correctly combine all individual graphs

        data = combined_graph.serialize(format='nt')  # Serialize the combined graph
        query = f"""
        INSERT DATA {{
            GRAPH <http://yourgraph.com> {{
                {data}
            }}
        }}
        """
        self.sparql.setQuery(query)
        try:
            self.sparql.query()
            logging.info("Batch inserted successfully into the triple store.")
        except Exception as e:
            logging.error(f"Failed to insert batch into the triple store: {e}")

           
    
