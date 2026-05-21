# Graph Processing Pipelines with Neo4j and GDS

This project implements graph-processing pipelines for the March 2022 NYC Taxi dataset using Neo4j and the Neo4j Graph Data Science (GDS) library. The project explores two architectures: a localized Docker-based graph analytics pipeline and a distributed streaming pipeline using Kubernetes, Kafka, Kafka Connect, and Neo4j.

## Overview

Taxi trip records are transformed into a graph structure where taxi zones are represented as `Location` nodes and taxi trips are represented as `TRIP` relationships. The graph is then analyzed using Neo4j GDS algorithms, including PageRank for node centrality analysis and shortest-path traversal for route computation.

The project is divided into two phases:

1. **Localized graph processing with Docker**
   - Loads, cleans, and transforms taxi trip data.
   - Builds a Neo4j graph from static trip records.
   - Runs graph analytics using Neo4j GDS.

2. **Distributed streaming pipeline with Kubernetes and Kafka**
   - Deploys Kafka, Zookeeper, Kafka Connect, and Neo4j inside Minikube.
   - Streams trip events into Kafka.
   - Uses the Neo4j Kafka Connector to insert incoming records into Neo4j in near real time.

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

The project uses the March 2022 NYC Taxi dataset. The data contains timestamped pickup and dropoff events, location IDs, trip distance, fare amount, and other trip metadata.

For this implementation, the dataset is filtered to include selected Bronx taxi zone IDs. Each unique pickup or dropoff location becomes a graph node, and each taxi trip becomes a directed relationship between two locations.

## Graph Model

### Nodes

```cypher
(:Location {location_id: <taxi_zone_id>})
