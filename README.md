# Graph Processing Pipelines with Neo4j, GDS, Kafka, and Kubernetes

This project implements graph-processing pipelines for the March 2022 NYC Taxi dataset using Neo4j and the Neo4j Graph Data Science (GDS) library. The project explores two architectures: a localized Docker-based graph analytics pipeline and a distributed streaming pipeline using Kubernetes, Apache Kafka, Kafka Connect, and Neo4j.

The goal of the project is to transform taxi trip records into a graph structure, analyze location relationships using graph algorithms, and demonstrate how streaming trip events can be inserted into a Neo4j graph database in near real time.

## Project Overview

Taxi trip data can naturally be modeled as a graph. In this project, each taxi zone is represented as a `Location` node, and each taxi trip is represented as a directed `TRIP` relationship between pickup and dropoff locations.

The project is divided into two phases:

1. **Phase 1: Localized Docker-Based Graph Processing**
   - Loads and cleans the NYC Taxi dataset.
   - Converts selected trip records into graph nodes and relationships.
   - Stores the graph in Neo4j.
   - Runs graph analytics using Neo4j GDS, including PageRank and shortest-path traversal.

2. **Phase 2: Distributed Kubernetes-Based Streaming Pipeline**
   - Deploys Zookeeper, Kafka, Kafka Connect, and Neo4j inside Minikube.
   - Streams taxi trip records into Kafka.
   - Uses the Neo4j Kafka Connector to insert records into Neo4j.
   - Demonstrates near-real-time graph construction and updates.

## Tech Stack

- **Languages:** Python, Cypher
- **Graph Database:** Neo4j
- **Graph Analytics:** Neo4j Graph Data Science
- **Data Processing:** Pandas, PyArrow
- **Containerization:** Docker
- **Orchestration:** Kubernetes, Minikube, Helm
- **Streaming:** Apache Kafka, Zookeeper, Kafka Connect
- **Data Format:** Parquet, CSV

## Dataset

This project uses the March 2022 NYC Taxi dataset. The raw dataset contains timestamped pickup and dropoff records, taxi zone IDs, trip distance, fare amount, and other trip-related metadata.

For this implementation, the dataset is filtered to include selected Bronx taxi zone IDs. Only the key columns needed for graph construction are retained:

- Pickup timestamp
- Dropoff timestamp
- Pickup location ID
- Dropoff location ID
- Trip distance
- Fare amount

The cleaned data is exported into CSV format and then loaded into Neo4j.

## Graph Model

The graph contains two main components:

### Nodes

Each unique taxi zone is represented as a `Location` node.

~~~cypher
(:Location {
  location_id: <taxi_zone_id>
})
~~~

### Relationships

Each taxi trip is represented as a directed `TRIP` relationship from the pickup location to the dropoff location.

~~~cypher
(:Location)-[:TRIP {
  distance: <trip_distance>,
  fare: <fare_amount>,
  pickup_dt: <pickup_timestamp>,
  dropoff_dt: <dropoff_timestamp>
}]->(:Location)
~~~

The data loader uses `MERGE` to avoid duplicate `Location` nodes and `CREATE` to insert each taxi trip as a separate relationship.

## Phase 1: Docker-Based Graph Processing

Phase 1 builds a localized graph-processing environment using Docker, Neo4j, and the Neo4j Graph Data Science plugin.

The main objectives of this phase are:

- Build a reproducible local Neo4j environment.
- Load and clean raw taxi trip data.
- Transform taxi trip records into graph structures.
- Run graph analytics algorithms on the constructed graph.

### Main Components

- `data_loader.py`
  - Reads the raw Parquet dataset.
  - Converts the data into a Pandas DataFrame.
  - Filters the dataset by selected taxi zone IDs.
  - Exports the cleaned data to CSV.
  - Initializes the Neo4j graph with nodes and relationships.

- `interface.py`
  - Implements graph analytics functions using Neo4j GDS.
  - Runs PageRank to analyze centrality.
  - Runs shortest-path traversal between taxi locations.

- `tester.py`
  - Validates the implementation and confirms that the graph-processing pipeline works correctly.

## Graph Analytics

### PageRank

PageRank is used to estimate the importance or centrality of taxi zones in the graph. A location with a higher PageRank score may represent a more connected or frequently used zone in the taxi trip network.

The implementation projects the graph into Neo4j GDS and runs PageRank over the `Location` nodes and `TRIP` relationships.

### Shortest-Path Traversal

The project also implements a path traversal function between two location IDs. Although the project template refers to this function as BFS, the implementation uses Neo4j GDS shortest-path logic to return an ordered list of location IDs representing the computed route.

## Phase 2: Kubernetes-Based Streaming Pipeline

Phase 2 extends the localized Docker pipeline into a distributed streaming architecture using Kubernetes, Kafka, Kafka Connect, and Neo4j.

The objective of this phase is to simulate a more realistic real-time data pipeline where incoming taxi trip events are continuously processed and inserted into a graph database.

### Main Components

- `zookeeper-setup.yaml`
  - Defines the Zookeeper deployment and service.

- `kafka-setup.yaml`
  - Defines the Kafka deployment and service.
  - Configures Kafka listeners and connection settings.

- `neo4j-values.yaml`
  - Configures the Neo4j Helm deployment.
  - Enables the Neo4j Graph Data Science plugin.

- `neo4j-service.yaml`
  - Exposes Neo4j inside the Minikube cluster.

- `kafka-neo4j-connector.yaml`
  - Deploys Kafka Connect with the Neo4j Kafka Connector.
  - Configures Kafka Connect to transform records into Cypher operations.

- `data_producer.py`
  - Streams taxi trip records into Kafka.

## Streaming Pipeline Architecture

The distributed pipeline follows this flow:

1. `data_producer.py` reads taxi trip records.
2. Trip records are streamed into a Kafka topic.
3. Kafka Connect consumes records from the Kafka topic.
4. The Neo4j Kafka Connector converts incoming records into Cypher operations.
5. Neo4j receives the records and updates the graph database.
6. Graph analytics can be performed on the updated graph.
