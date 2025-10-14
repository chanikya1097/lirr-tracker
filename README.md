Project Plan: LIRR-Tracker
1. Project Overview
Goal: To build a complete, end-to-end data pipeline that ingests, processes, and visualizes live LIRR train data using a free, locally-run tech stack.

Outcome: A functional, real-time dashboard monitoring the Long Island Rail Road, suitable for a professional data engineering portfolio.

2. Data Source & API Links
Provider: New York City Metropolitan Transportation Authority (MTA).

API Key Signup: You must sign up for a free, personal API key at the MTA Developer Portal.

Real-time Data URL: The Python script will fetch data from this specific endpoint: https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr.

Data Format: Protocol Buffers (Protobuf).

3. Phases & Tasks
Phase 1: Project Setup & Environment Configuration
Goal: Prepare the local development environment, version control, and all necessary services.

Local Repository Setup: Initialize a local Git repository and create the core project structure (src/, dbt_project/, docs/, img/).

GitHub Synchronization: Create a new remote repository on GitHub, link the local repository, and push the initial structure.

Service Installation: Install MySQL Community Server, Java, Jenkins, and Metabase directly on your machine.

Cloud Service Setup: Create a free Upstash account, provision a Kafka cluster, and create a topic named lirr_realtime_updates. Securely save the connection credentials.

Documentation: Update the README.md with the final architecture diagram and project description.

Phase 2: Data Ingestion (Producer)
Goal: Write a Python script to fetch live data from the MTA API and produce it to Kafka.

Script Development: In src/, create producer.py. Install required libraries: requests, gtfs-realtime-bindings, kafka-python.

API Connection: Add logic to connect to the MTA API endpoint using your API key (sent as an x-api-key header).

Data Parsing: Parse the binary Protobuf response and extract key fields (e.g., trip_id, vehicle_id, latitude, longitude, timestamp).

Kafka Production: Serialize the extracted data into JSON and send it as a message to the lirr_realtime_updates Kafka topic on Upstash.

Phase 3: Data Processing & Storage (Consumer)
Goal: Write a Python script to consume data from Kafka and store it in our local data lake.

Script Development: In src/, create consumer.py.

Kafka Consumption: Configure the script to connect to the Upstash Kafka topic as a consumer.

Data Lake Storage: Upon receiving a message, write the data to a file in the data/ directory (e.g., as timestamped JSON or CSV files).

Phase 4: Data Warehousing & Transformation (dbt + MySQL)
Goal: Move data from the local data lake into a structured data warehouse and transform it for analysis.

dbt Project Initialization: In dbt_project/, initialize a new dbt project and configure it to connect to your local MySQL database.

Data Loading: Create a process (e.g., a Python script or dbt seed/source) to load the raw data from the data/ folder into a "raw" table in MySQL.

Data Modeling: Create dbt models to clean and structure the raw data into final analytical tables, such as fct_trip_updates and dim_trains.

Phase 5: Automation & Orchestration (Jenkins)
Goal: Automate the entire pipeline using Jenkins.

Jenkins Setup: Start the Jenkins server locally and complete the initial setup.

Job Creation:

Ingestion Job: A Jenkins job to run producer.py every minute.

Processing Job: A Jenkins job to run consumer.py continuously.

Transformation Job: A Jenkins job to run dbt run every hour.

Phase 6: Visualization (Metabase)
Goal: Connect Metabase to the data warehouse and build an insightful dashboard.

Metabase Setup: Start the Metabase server locally and connect it to your local MySQL database.

Dashboard Creation: Build a dashboard with key visualizations, such as a map of current train locations, a table of recent updates, and charts showing active trains per route.