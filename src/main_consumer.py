from flask import Flask, render_template, jsonify, request
import threading
import json
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
from confluent_kafka import Consumer, KafkaError
from gen import location_pb2
from datetime import datetime
import dateutil.parser as dp

app = Flask(__name__)

# initialize location data
data_to_plot = {'Latitude': [], 'Longitude': [], 'Timestamp':[]}

# Kafka Consumer Configuration
bootstrap_servers = 'hiwi-test-kafka-1:9092'
#bootstrap_servers = 'c2d90e66eef2:9092'
config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

def convert_to_decimal(degrees_minutes, direction):
    degrees = int(degrees_minutes[:-7])
    minutes = float(degrees_minutes[-7:])
    decimal_degrees = degrees + minutes / 60
    if direction in ['S', 'W']:
        decimal_degrees *= -1
    return decimal_degrees

def kafka_consumer():
    # The longitude is incremented by one for every loop, just for better visibility in the map.
    consumer = Consumer(config)
    consumer.subscribe(['location_topic'])
    i=0
    global locations
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            location = location_pb2.location()
            location.ParseFromString(msg.value())
            data_to_plot['Latitude'].append(convert_to_decimal(location.latitude, location.lat_direction))
            data_to_plot['Longitude'].append(convert_to_decimal(location.longitude, location.lon_direction)+i)
            dt_object = datetime.fromtimestamp(location.utc_time)
            data_to_plot['Timestamp'].append(dt_object.isoformat())
            i+=1
            print(f"Decoded message: {location}")
            
    finally:
        consumer.close()

# Start the Kafka consumer in a background thread
thread = threading.Thread(target=kafka_consumer)
thread.start()

@app.route('/')
def index():
    return render_template('map.html')

@app.route('/data')

def data():
    # Retrieve start and end parameters from the query string
    start = request.args.get('start')
    end = request.args.get('end')

    if start and end:
        try:
            # Now safe to parse because start and end are not None
            parsedStartDateTime = dp.parse(start)
            start_datetime = parsedStartDateTime.timestamp()
            parsedEndDateTime = dp.parse(end)
            end_datetime = parsedEndDateTime.timestamp()

            if type(start_datetime)!=None and type(end_datetime)!=None and len(data_to_plot['Timestamp'])>0:
            # Initialize an empty list for filtered data
                filtered_data = {'Latitude': [], 'Longitude': [], 'Timestamp': []}

                for i in range(len(data_to_plot['Timestamp'])):
                    # Parse each timestamp string to a datetime object and then to a timestamp
                    point_timestamp = dp.parse(data_to_plot['Timestamp'][i]).timestamp()
                    
                    # Check if the point's timestamp is within the start and end datetime range
                    if start_datetime <= point_timestamp <= end_datetime:
                        filtered_data['Latitude'].append(data_to_plot['Latitude'][i])
                        filtered_data['Longitude'].append(data_to_plot['Longitude'][i])
                        filtered_data['Timestamp'].append(data_to_plot['Timestamp'][i])
                
                # Iterate over the indices of the Timestamp list
                fig = px.scatter_geo(lat=filtered_data['Latitude'], lon=filtered_data['Longitude'])
                return json.dumps(fig, cls=PlotlyJSONEncoder)
            
        except ValueError as e:
            # Handle parsing error
            return jsonify({'error': 'Invalid date format'}), 400
    
    else:
        print("else")
        filtered_data = data_to_plot

        
        # Iterate over the indices of the Timestamp list
        fig = px.scatter_geo(filtered_data, lat='Latitude', lon='Longitude')
        return json.dumps(fig, cls=PlotlyJSONEncoder)#, jsonify({'error': 'Missing start or end date parameter'}), 400

    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
    app.run(debug=True)
