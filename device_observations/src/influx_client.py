from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

class InfluxDBHandler:
    def __init__(self, host, port, token, org, bucket):
        self.client = InfluxDBClient(url=f"http://{host}:{port}", token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        self.logger = logging.getLogger(self.__class__.__name__)

    def write_data(self, data):
        try:
            self.write_api.write(self.bucket, record=data)
            self.logger.info(f"Data writen to InfluxDB: {data}")
        except Exception as e:
            self.logger.error(f"Failed to write data to InfluxDB: {str(e)}")

    def query_data(self, query):
        try:
            result = self.query_api.query(org=self.client.org, query=query)
            return list(result)
        except Exception as e:
            self.logger.error(f"Failed to query data from InfluxDB: {str(e)}")
            return []


if __name__ == "__main__":
    # InfluxDB connection details
    host = "localhost"
    port = 8086
    token = "uZVwQnrC7x8JRSOF3HCQXiTeRkfFI9uK2mrHxqy3OE3kFyUSVURFhsohjbVl3s_Ca8wLJGD-Va96ZDDrJSElCA=="
    org = "test_org"
    bucket = "SmartCity"

    influxdb_client = InfluxDBHandler(host, port, token, org, bucket)

    # Example usage
    data = [
        Point("device_positions")
        .tag("device_id", "device001")
        .field("latitude", 40.7120)
        .field("longitude", -74.0063)
        .field("speed", 1.0)
        .field("direction", 139.0)
    ]

    influxdb_client.write_data(data)

    query = (f'from(bucket: "{bucket}") '
             f'|> range(start: -1h) '
             f'|> filter(fn: (r) => r["_measurement"] == "device_positions") '
             f'|> filter(fn: (r) => r["_field"] == "direction" or r["_field"] == "latitude" or r["_field"] == "longitude" or r["_field"] == "speed")'       
             f'|> pivot (rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") '
             f'|> max(column: "_time")'
             f'|> yield(name: "mean")')

    result = influxdb_client.query_data(query)

    #print(result)
    for table in result:
        for record in table.records:
            # Extracting fields from the record
            time = record.get_time()
            measurement = record.get_measurement()
            device_id = record.values.get('device_id', 'N/A')  # 'N/A' as default if device_id is not available
            latitude = record.values.get('latitude', 'N/A')
            longitude = record.values.get('longitude', 'N/A')
            speed = record.values.get('speed', 'N/A')
            direction = record.values.get('direction', 'N/A')

            # Printing the data
            print(f"Time: {time}, Measurement: {measurement}, Device ID: {device_id}, "
                  f"Latitude: {latitude}, Longitude: {longitude}, Speed: {speed}, Direction: {direction}")
