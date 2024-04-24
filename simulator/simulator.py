import os
import random
import time
from influxdb_client import Point
from influx_client import InfluxDBHandler
from dotenv import load_dotenv
import osmnx as ox
from shapely.geometry import Polygon
from pyproj import Transformer

# Define your polygon coordinates for the OSM area
cords = [(12.3047, 45.4455), (12.2995, 45.4389),
         (12.3052, 45.4368), (12.3084, 45.4411),
         (12.3140, 45.4418), (12.3061, 45.4463),
         (12.3047, 45.4455)]
polygon = Polygon(cords)

G = ox.graph_from_polygon(polygon, network_type='all', retain_all=True, simplify=True, truncate_by_edge=True)
G_projected = ox.project_graph(G)

# Get the CRS (Coordinate Reference System) of the projected and original graph
crs_projected = ox.projection.project_gdf(ox.graph_to_gdfs(G_projected, nodes=False, edges=True)).crs
crs_geographic = ox.graph_to_gdfs(G, nodes=False, edges=True).crs

transformer = Transformer.from_crs(crs_projected, crs_geographic, always_xy=True)

unprojected = ox.get_undirected(G_projected)

class DeviceSimulator:
    def __init__(self, influxdb_client, num_devices=5):
        self.influxdb_client = influxdb_client
        self.num_devices = num_devices

    def generate_device_data(self):

        device_data = []

        for device_id in range(1, self.num_devices + 1):
            points = ox.utils_geo.sample_points(unprojected, n=1)
            x_projected, y_projected = points.x.values[0], points.y.values[0]
            latitude, longitude = transformer.transform(x_projected, y_projected)
            speed = random.uniform(0.00, 10.00)  # Assuming speed is in m/s and devices can go up to 10 mm/s
            direction = random.uniform(0.00, 360.00)  # Direction in degrees

            data_point = [
                Point("device_positions")
                .tag("device_id", device_id)
                .field("latitude", latitude)
                .field("longitude", longitude)
                .field("speed", speed)
                .field("direction", direction)
            ]
            device_data.append(data_point)
        return device_data

    def send_data_to_influxdb(self, data):
        try:
            self.influxdb_client.write_data(data)
            print("Data sent to InfluxDB successfully.")
        except Exception as e:
            print(f"Failed to send data to InfluxDB: {str(e)}")

    def start_simulation(self, interval=1):

        while True:
            device_data = self.generate_device_data()
            self.send_data_to_influxdb(device_data)
            time.sleep(interval)


if __name__ == "__main__":
    # Replace with your InfluxDB connection details
    load_dotenv()
    host = "localhost"
    port = 8086
    token = os.getenv("INFLUXDB_TOKEN")
    org = "extract_org"
    bucket = "DeviceData"
    influxdb_client = InfluxDBHandler(host=os.getenv("INFLUXDB_SERVICE_SERVICE_HOST", "localhost"),
                                      port=os.getenv("INFLUXDB_SERVICE_SERVICE_PORT", "8086"),
                                      token=os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"),
                                      org=os.getenv("DOCKER_INFLUXDB_INIT_ORG"), bucket=os.getenv("DOCKER_INFLUXDB_INIT_BUCKET"))
    device_simulator = DeviceSimulator(influxdb_client, num_devices=100)
   
    device_simulator.start_simulation()
    
