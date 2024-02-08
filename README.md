# Simple GPS data simulator

## Description

This is a simple data generator mocking a GPGGA data source. The datasource is then proccessed to an kafka-server (`hiwi-test-kafka-1:29092`). 

## Task
Please provide a service that subscribes to the Kafka topic `location_topic` and visualize the data on a map. A containerized solution is preferred. You may extend the service to provide additional features like filtering, some additional visualization, etc. You may use any programming language, any framework, or any library. 

AC:
* [ ] The service needs to use the data from the kafka topic `location_topic`
* [ ] The service should decode the protobuf data messages 
* [ ] The service should be able to visualize the data on a map
* [ ] The service should be containerized

Question to be answered:
* What object can be expected to move with that trajectory? **--something like exploration robot, maybe a rover? (keyword from the message key)**

## What Have Been Done
The task is done by processing the data with a Flask application, and visualizing it on a live-updating map.

The components added to this project are as follows:
- A Flask application that consumes the GPS data from the Kafka server and processes it.
- A live map visualization built with Plotly, which displays the processed GPS data.

### Flask Application

The Flask application consumes data from the Kafka server, processes it, and serves a web interface for visualization. The application subscribes to the Kafka topic, decodes the messages, and updates the live map.

### Live Map Visualization

The live map is a web-based visualization that displays the GPS data in real-time. It is accessible via a web browser and updates automatically as new data is processed by the Flask application.

## How to Access the Live Map

To access the live map visualization:

1. Ensure that the Flask application is running. It should be set up to consume data from the Kafka server and serve the web interface on a specified port.
2. Open your web browser and navigate to the IP address and port where the Flask application is hosted at [http://localhost:5000/](http://localhost:5000/).
3. The map should be visible and updating in real-time as new GPS data is processed.


## Installation

### Building the project

* Dependencies
  * docker
  * protobuf (for decoding the data)

* Building the project:

```
docker compose up -d
```

### Usage

* For a better interaction with docker you can use the VSCode docker extension: https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-docker

* If the docker containers are running, you may find the kafka-UI (kafka-drop) at [http://localhost:9000/](http://localhost:9000/)


### Protobuf

* Install protobuf

```
sudo apt install -y kafkacat protobuf-compiler
```

* Generating python descriptors

```
protoc -I="." --python_out=src/gen ./location.proto
```

* Generating kafkadrop descriptors (Ubuntu)

```
protoc -o descriptors/location.desc location.proto
```

### Known issues
* Error while running the kafka broker (i.e. `unable to allocate file descriptor table - out of memory`):

-> Solution: as often can provide [stackoverflow](https://stackoverflow.com/questions/68776387/docker-library-initialization-failed-unable-to-allocate-file-descriptor-tabl)

(!) Please open an issue if you find any problems.

