from flask import Flask, render_template, jsonify
import threading
import json
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
from confluent_kafka import Consumer, KafkaError
from gen import location_pb2

app = Flask(__name__)

# initialize location data
data_to_plot = {'Latitude': [], 'Longitude': []}

# Kafka Consumer Configuration
bootstrap_servers = 'hiwi-test-kafka-1:9092'
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
    # Plot the data to the map
    fig = px.scatter_geo(data_to_plot, lat='Latitude', lon='Longitude')
    return json.dumps(fig, cls=PlotlyJSONEncoder)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
    app.run(debug=True)
