import json
import os
import re
import numpy as np
from dotenv import load_dotenv
import triple_store
load_dotenv()
triple_store = triple_store.TripleStoreHandler(virtuoso_url="http://localhost:5556/sparql-auth",
                                               virtuoso_user=os.getenv("DBA_USER"),
                                               virtuoso_pwd=os.getenv("DBA_PASSWORD"))

global conversion_array  # Make sure conversion_array is defined in the global scope
#global tensor  # Make sure tensor is defined in the global scope


def road_graph_array():
    node_query = '''
        PREFIX ext: <https://www.extract-project.eu/ontology#>
        PREFIX otn: <http://www.pms.ifi.uni-muenchen.de/OTN#>

        SELECT ?node ?osmId
            WHERE {
                GRAPH <https://extract-project.eu/road_schema> {
                    ?node a otn:Node ;
                        ext:hasOSMId ?osmId .
                }
            }
        '''

    road_query = '''
        PREFIX ext: <https://www.extract-project.eu/ontology#>
        PREFIX otn: <http://www.pms.ifi.uni-muenchen.de/OTN#>

        SELECT ?road ?startNode ?endNode
            WHERE {
                GRAPH <https://extract-project.eu/road_schema> {
                    ?road a otn:Road_Element ;
                        ext:startinNode ?startNode ;
                        ext:endingNode ?endNode .
                }
            }
        '''

    # Execute queries and get results
    node_results = triple_store.execute_sparql_query(node_query)
    road_results = triple_store.execute_sparql_query(road_query)

    nodes = set()
    roads = []

    # Process node results
    for result in node_results["results"]["bindings"]:
        node_id = result["osmId"]["value"]
        node_id = int(node_id)
        nodes.add(node_id)

    # Process road results
    for result in road_results["results"]["bindings"]:
        start_node = result["startNode"]["value"]
        end_node = result["endNode"]["value"]
        start_id = int(start_node)
        end_id = int(end_node)
        roads.append((start_id, end_id))
        roads.append((end_id, start_id))

    # Creating the conversion array
    conversion_array = np.array([(node, node) for node in nodes] + roads, dtype=object)

    # Save the conversion array to a JSON file
    with open('conversion_array.json', 'w') as json_file:
        json.dump(conversion_array.tolist(), json_file)

    return conversion_array

# Call the function
conversion_array = road_graph_array()
print("--- Num Nodes and Roads in Conversion Array ---")
print(len(conversion_array))
print("--- Conversion Array ---")
print(conversion_array)

def length_tensor():
    # Assuming conversion_array and triple_store are already defined and set up
    # Initialize the tensor with zeros
    length_tensor = np.zeros(len(conversion_array))

    # SPARQL query to retrieve road lengths
    query_for_road_length = '''
    PREFIX ext: <https://www.extract-project.eu/ontology#>
    PREFIX otn: <http://www.pms.ifi.uni-muenchen.de/OTN#>

    SELECT ?startNode ?endNode ?length
    WHERE {
    ?road a otn:Road_Element ;
          ext:startinNode ?startNode ;
          ext:endingNode ?endNode ;
          ext:length ?length .
    }
    '''

    # Execute the SPARQL query
    road_length_results = triple_store.execute_sparql_query(query_for_road_length)

    # Process the results
    for result in road_length_results["results"]["bindings"]:
        start_node = int(result["startNode"]["value"])
        end_node = int(result["endNode"]["value"])
        length = float(result["length"]["value"])

        # Construct tuples to match pairs in conversion_array
        start_end_tuple = (start_node, end_node)
        end_start_tuple = (end_node, start_node)

        # Search for these tuples in the conversion_array
        for i, pair in enumerate(conversion_array):
            if np.array_equal(pair, start_end_tuple) or np.array_equal(pair, end_start_tuple):
                # Update the tensor with the road length
                length_tensor[i] = length

    # Optionally, save the length tensor to a JSON file
    with open('length_tensor.json', 'w') as json_file:
        json.dump(length_tensor.tolist(), json_file)

    return length_tensor
#Call the function
length = length_tensor()

