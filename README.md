LIRR-Tracker: Real-Time Long Island Rail Road Monitoring
Input Data
Source: Official MTA GTFS Realtime API feed for the Long Island Rail Road (LIRR).

Frequency: The data is a live stream, updated every few seconds.

Data Format: Protocol Buffers (Protobuf).

Goal / Objective
The goal is to build a complete, end-to-end data pipeline that ingests, processes, and visualizes live LIRR train data. The system will track train locations in real-time, identify service delays, and store historical data for performance analysis using a free, locally-run tech stack.

Tech Stack
Category	Tool
Version Control	GitHub
Automation	Jenkins
Ingestion	Python
Streaming	Apache Kafka (via Upstash)
Data Warehouse	MySQL
Transformation	dbt Core
Visualization	Metabase

Export to Sheets
High-Level Architecture
The architecture is designed to process data in a continuous, real-time flow from the source API to the final dashboard.

![Alt text for the image](./img/architecture.png)
