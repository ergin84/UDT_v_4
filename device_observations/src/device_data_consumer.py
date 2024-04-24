import time
import json
import os
import time
import logging
from rdflib import Graph, URIRef, Literal, RDF, XSD
from dotenv import load_dotenv
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import timestamp_to_url_string, find_nearest_road_or_node
from triple_store import TripleStoreHandler, EXT, GEO, SOSA, TIME
from metrics import MESSAGES_RECEIVED, start_metrics_server

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ConsumerHandler:
    def __init__(self, kafka_server, kafka_topic, group_id, triple_store):
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_server],
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        self.triple_store = triple_store

    def process_data(self, record):
        try:
            graph = Graph()
            # Extract fields from the record
            device_id = record['device_id']
            record_time = (record['time'])
            latitude = record['latitude']
            longitude = record['longitude']
            speed = record['speed']
            direction = record['direction']

            # Generate URI references
            time_to_uri = timestamp_to_url_string(record_time)
            device_uri = URIRef(EXT[f"MobileDevice/{device_id}"])
            observation_uri = URIRef(EXT[f"DeviceObservation/{device_id}_{time_to_uri}"])
            geometry_uri = URIRef(EXT[f"Geometry/{device_id}_{time_to_uri}"])
            instant_uri = URIRef(TIME[f"Instant/{time_to_uri}"])

            # Construct RDF triples
            graph.add((device_uri, RDF.type, EXT.MobileDevice))
            graph.add((device_uri, RDF.type, SOSA.Sensor))
            graph.add((observation_uri, RDF.type, EXT.DeviceObservation))
            graph.add((observation_uri, RDF.type, SOSA.Observation))
            graph.add((geometry_uri, RDF.type, GEO.Geometry))
            graph.add((observation_uri, EXT.hasPosition, geometry_uri))
            graph.add((observation_uri, SOSA.madeBySensor, device_uri))
            graph.add((device_uri, SOSA.madeObservation, observation_uri))
            graph.add((geometry_uri, GEO.asWKT, Literal(f"POINT({longitude} {latitude})", datatype=GEO.wktLiteral)))
            graph.add((instant_uri, RDF.type, TIME.Instant))
            graph.add((instant_uri, TIME.inXSDDateTimeStamp, Literal(record_time, datatype=XSD.dateTimeStamp)))
            graph.add((observation_uri, EXT.observationTime, instant_uri))
            graph.add((observation_uri, EXT.hasSpeed, Literal(speed, datatype=XSD.float)))
            graph.add((observation_uri, EXT.hasDirection, Literal(direction, datatype=XSD.float)))
            
            nearest_road_or_node_id = find_nearest_road_or_node(latitude, longitude)
            if nearest_road_or_node_id is not None:
                if nearest_road_or_node_id.__contains__("Road"):
                    nearest_road_or_node_uri = URIRef(nearest_road_or_node_id)
                    graph.add((observation_uri, EXT.isLocatedIn, nearest_road_or_node_uri))
                elif nearest_road_or_node_id.__contains__("Node"):
                    nearest_road_or_node_uri = URIRef(nearest_road_or_node_id)
                    graph.add((observation_uri, EXT.isLocatedIn, nearest_road_or_node_uri))

            return graph
        except Exception as e:
            logging.error(f"Error processing data: {e}")
            return None


    def run(self):
        batch_size = 100
        while True:  # Ensures the consumer runs indefinitely
            messages_batch = []
            try:
                # Poll for messages
                raw_messages = self.consumer.poll(timeout_ms=10000)  # Poll for 10 seconds
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if message is not None and message.value is not None:
                            messages_batch.append(message.value)
                        if len(messages_batch) >= batch_size:
                            self.process_and_insert_batch(messages_batch)
                            messages_batch = []

                # Process any remaining messages after polling
                if messages_batch:
                    self.process_and_insert_batch(messages_batch)

                # If no messages were found during the poll, sleep to reduce load
                if not raw_messages:
                    logging.info("No new messages received. Sleeping for 10 seconds...")
                    time.sleep(10)

            except Exception as e:
                logging.error(f"Error while consuming messages: {e}")
                time.sleep(10)  # Sleep to avoid tight loop on error



    def process_and_insert_batch(self, messages_batch):
        graphs = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(self.process_data, message) for message in messages_batch]
            for future in as_completed(futures):
                graph = future.result()
                if graph:
                    graphs.append(graph)

        if graphs:
            try:
                self.triple_store.bulk_insert(graphs)
                MESSAGES_RECEIVED.inc(len(graphs))  # Assuming a metric that counts processed messages
            except Exception as e:
                logging.error(f"Exception in bulk inserting graphs: {e}")

def main():
    load_dotenv()
    #start_metrics_server()  # Start Prometheus metrics server
    kafka_server = os.getenv("KAFKA_SERVICE_SERVICE_HOST", "localhost") + ":" + os.getenv("KAFKA_SERVICE_SERVICE_PORT", "9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "device_data")
    group_id = os.getenv("KAFKA_GROUP_ID", "device_consumer_group")
    triple_store = TripleStoreHandler(virtuoso_url="http://localhost:5556/sparql-auth",
                                      virtuoso_user=os.getenv("VIRTUOSO_USER"),
                                      virtuoso_pwd=os.getenv("VIRTUOSO_PWD"))

    consumer = ConsumerHandler(kafka_server, kafka_topic, group_id, triple_store)
    while True:
        consumer.run()
          
if __name__ == "__main__":
    main()
