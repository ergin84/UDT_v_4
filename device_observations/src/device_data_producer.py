from datetime import datetime, timedelta
import json
import time
from influx_client import InfluxDBHandler
from kafka import KafkaProducer
import pandas as pd
from queries import create_query
import os
from dotenv import load_dotenv
import logging
from metrics import MESSAGES_SENT, start_metrics_server

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ProducerHandler:
    def __init__(self, kafka_server, kafka_topic,kafka_producer, influxdb_client):
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.producer = kafka_producer
        self.influxdb_client = influxdb_client
        
    def query_influxdb(self):
        try:
            # Define the time bounds for the query
            if not hasattr(self, 'last_query_time'):
                self.last_query_time = datetime.utcnow() - timedelta(seconds=10)  # Default to last 10 seconds on the first run
            current_time = datetime.utcnow()

            # Create query from the queries module
            query = create_query(self.last_query_time.isoformat() + 'Z', current_time.isoformat() + 'Z')
            result = self.influxdb_client.query_data(query)
            
            # Update the last query time
            self.last_query_time = current_time

            # Process results into DataFrame
            df = pd.DataFrame([{
                'device_id': record.values['device_id'],
                'time': record.get_time(),
                'latitude': record.values['latitude'],
                'longitude': record.values['longitude'],
                'speed': record.values['speed'],
                'direction': record.values['direction']
            } for table in result for record in table.records])

            return df if not df.empty else None
        except Exception as e:
            logging.error(f"Error querying InfluxDB: {e}")
            return None

    def publish_to_kafka(self, df):
        try:
            for row in df.itertuples(index=False):
                serializable_row = {
                    'device_id': row.device_id,
                    'time': row.time.isoformat(),
                    'latitude': row.latitude,
                    'longitude': row.longitude,
                    'speed': row.speed,
                    'direction': row.direction
                }
                serialized_data = json.dumps(serializable_row).encode('utf-8')
                self.producer.send(self.kafka_topic, value=serialized_data)
                MESSAGES_SENT.inc()  # Increment the metric counter after successful send
            self.producer.flush()
        except Exception as e:
            logging.error(f"Error publishing to Kafka: {e}")


    def run(self):
        df = self.query_influxdb()
        if df is not None and not df.empty:
            self.publish_to_kafka(df)
            logging.info("Data published to Kafka.")
        else:
            logging.info("No new data to publish.")
            
def main():
    #start_metrics_server()
    load_dotenv()
    sleep_duration = int(os.getenv("SLEEP_DURATION", 1))
    kafka_server = os.getenv("KAFKA_SERVICE_SERVICE_HOST", "localhost") + ":" + os.getenv("KAFKA_SERVICE_SERVICE_PORT", "9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "device_data")
    kafka_producer = KafkaProducer(bootstrap_servers=[kafka_server])
    influxdb_client = InfluxDBHandler(
                host=os.getenv("INFLUXDB_SERVICE_SERVICE_HOST", "localhost"),
                port=os.getenv("INFLUXDB_SERVICE_SERVICE_PORT", "8086"),
                token=os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"),
                org=os.getenv("DOCKER_INFLUXDB_INIT_ORG"),
                bucket=os.getenv("DOCKER_INFLUXDB_INIT_BUCKET")
    )
    
    producer = ProducerHandler(kafka_server, kafka_topic, kafka_producer, influxdb_client)
    while True:
        producer.run()
        time.sleep(sleep_duration)
    
    
if __name__ == "__main__":
    main()