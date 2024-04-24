############################### SPARQL query for Nodes as NodeIRI - geometry ########################
node_query4 = """
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX otn: <http://www.pms.ifi.uni-muenchen.de/OTN#>
    PREFIX ext: <https://www.extract-project.eu/ontology#>

    SELECT ?node ?geometry WHERE {
        GRAPH <https://extract-project.eu/road_schema> {
            ?node a otn:Node .
            ?node geo:hasGeometry ?geom .
            ?geom geo:asWKT ?geometry
            
        }
    }
"""

############################### SPARQL query for RoadsIRI, startNodes, EndNodes, geometry ###########
road_query4 = """
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX otn: <http://www.pms.ifi.uni-muenchen.de/OTN#>
    PREFIX ext: <https://www.extract-project.eu/ontology#>

    SELECT ?road ?startNodeId ?startNode ?endNodeId ?endNode ?geometry WHERE {
        GRAPH <https://extract-project.eu/road_schema> {
            ?road a otn:Road_Element .
            ?road otn:starts_at ?startNode .
            ?road otn:ends_at ?endNode .
            ?startNode ext:hasOSMId ?startNodeId .
            ?endNode ext:hasOSMId ?endNodeId .
            ?road geo:hasGeometry ?geom .
            ?geom geo:asWKT ?geometry .
        }
    }
"""

# Query data from InfluxDB
influxdb_query = (f'from(bucket: "device-data") '
                  f'|> range(start: -1m) '
                  f'|> filter(fn: (r) => r["_measurement"] == "device_positions") '
                  f'|> filter(fn: (r) => r["_field"] == "direction" or r["_field"] == "latitude" or r["_field"] == "longitude" '
                  f'or r["_field"] == "speed")'
                  f'|> pivot (rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") '
                  f'|> max(column: "_time")'
                  f'|> yield(name: "mean")')

def create_query(start_time, end_time):
    """
    Function to create a dynamic InfluxDB query with specified time bounds.
    """
    return (
        f'from(bucket: "device-data") '
        f'|> range(start: {start_time}, stop: {end_time}) '
        f'|> filter(fn: (r) => r["_measurement"] == "device_positions") '
        f'|> filter(fn: (r) => r["_field"] == "direction" or r["_field"] == "latitude" or r["_field"] == "longitude" or r["_field"] == "speed") '
        f'|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
        f'|> max(column: "_time")'
        f'|> yield(name: "mean")'
    )