[Документация на русском языке](README.ru.md)

# IoT Data Processing Pipeline Using the ELT Workflow

__Project Goal__: Develop a system for ingesting, processing, storing, and visualizing IoT telemetry data. Data is collected in real time using Apache NiFi, stored in MinIO object storage, and loaded into a PostgreSQL data warehouse for analytical processing. Apache Airflow orchestrates data processing and transformations, while PGAdmin and Metabase enable analysis and visualization of aggregated data, providing structured insights for further exploration.

## Data source

https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k

## Architecture

An overview of the project’s architecture, illustrating data flow from ingestion to storage and analysis.

![Alt text]()

## Components

A detailed breakdown of each component in the pipeline, including their roles in data ingestion, processing, storage, and visualization.

1. __dmitry-airflow Virtual Machine__

- __Role__: Hosts Airflow for Scheduling and Coordinating Data Workflows
- __Deployment__: Yandex Compute Cloud.
- __Description__: Executes Airflow DAGs to retrieve raw data from MinIO, process it, and store it in PostgreSQL's DDS and Datamart layers. DAGs are triggered every 15 and 60 minutes, using the boto3 library for MinIO connections and PostgresHooks for PostgreSQL, ensuring seamless data transfer and workflow automation.

2. __dmitry-de Virtual Machine__

- __Role__: Hosts NiFi, MinIO, PostgreSQL, PGAdmin, and Metabase for Data Ingestion, Storage, and Visualization
- __Deployment__: Yandex Compute Cloud.
- __Description__: Manages data ingestion from IoT devices via NiFi, storage of raw data in MinIO, storage of processed data in PostgreSQL, and visualization in Metabase. Each service runs in a Docker container, all configured and managed by Docker Compose for easy deployment and service interaction.

3. __Apache NiFi__

- __Role__: Data Ingestion and Preprocessing
- __Deployment__: Runs as a Docker container on the dmitry-de VM.
- __Description__: Ingests real-time IoT data through its ListenHTTP processor, which accepts incoming requests via API. Data is preprocessed using the MergeContent processor to batch records in groups of 1,000, with ExecuteScript and UpdateAttribute processors applied to set each batch’s timestamp-based name. Processed data is stored in MinIO via the PutS3Object processor.

4. __MinIO__

- __Role__: Raw Data Object Storage
- __Deployment__: Runs as a Docker container on the dmitry-de VM.
- __Description__: Provides scalable, S3-compatible storage for large volumes of IoT data, storing batch files from NiFi as timestamped txt files. Files are organized by ingestion time for efficient retrieval and processing by Airflow.

5. __Apache Airflow__

- __Role__: Workflow Orchestration and Scheduling
- __Deployment__: Runs on the dmitry-airflow VM.
- __Description__: Orchestrates and schedules IoT data processing workflows, moving data from raw ingestion to structured storage in PostgreSQL. Key DAGs include:
    - __create_dds_dag__ and __create_datamart_dag__: Initialize DDS (dds_iot_database) and Datamart (datamart_iot_database) tables in PostgreSQL.
    - __drop_dds_dag__ and __drop_datamart_dag__: Remove all tables in the DDS and Datamart layers.
    - __s3_to_dds_dag__: Runs every 15 minutes to retrieve data from MinIO, aggregate it by minute, and load it into the DDS layer.
    - __dds_to_dm_dag__: Runs every hour to retrieve data from the DDS layer, process and aggregate it by hour, and move it into the Datamart tables for analysis and visualization.

6. __PostgreSQL__

- __Role__: Data Warehouse
- __Deployment__: Runs as a Docker container on the dmitry-de VM.
- __Description__: Stores and organizes processed IoT data, divided into DDS and Datamart tables for efficient querying and reporting:
    - __DDS Layer (dds_iot_database)__: Stores timestamped data with minute-level granularity for historical analysis.
    - __Datamart Layer (datamart_iot_database)__: Contains hourly aggregates for reporting, including:
        - dm_iot_average: Average metrics per variable by hour.
        - dm_iot_extreme: Hourly min and max values.
        - dm_iot_count: Record counts by hour.
        - dm_iot_ml: ML-ready statistical features for predictive modeling.

7. __PGAdmin__

- __Role__: Database Management Interface
- __Deployment__: Runs as a Docker container on the dmitry-de VM.
- __Description__: Web-based interface for managing and querying PostgreSQL databases. Allows visual inspection of DDS and Datamart tables for data validation, query execution, and debugging.

8. __Metabase__

- __Role__: Data Visualization
- __Deployment__: Runs as a Docker container on the dmitry-de VM.
- __Description__: Connects directly to the datamart_iot_database in PostgreSQL, pulling pre-aggregated data tables for efficient visualizations. Dashboards provide insights into IoT data trends, supporting easy data exploration.

9. __Docker__

- __Role__: Containerization and Service Management
- __Deployment__: Runs on the dmitry-de VM.
- __Description__: Isolates each service and provides configuration consistency. Managed by Docker Compose, which configures container interactions and ensures reproducible deployments across services.

## Pipeline

A step-by-step description of the data processing flow, from raw data ingestion to transformations, aggregations, and visualizations.

1. __Data Generation and Ingestion__

- __Data Generation__: The system simulates IoT data for environmental conditions. Each record is sent as a single string, with all features (timestamp, device_id, co, humidity, lpg, smoke, temperature, light) separated by spaces. Data generation can be done in two ways:

    - __Dummy Data Generation__: The generate_dummy_data.py script generates random values for each feature, including extreme outliers and missing values, to simulate real-world device malfunctions. It sends these records as HTTP POST requests to an Apache NiFi endpoint. Key arguments include: 

        - url: NiFi endpoint
        - requests_per_second: Number of requests per second for each device
        - total_requests: Total number of requests for each device

    - __Real Data Generation__: The generate_real_data.py script reads actual historical IoT data from a CSV file located in the raw-data folder and sends each row as a string in an HTTP POST request to NiFi. Key arguments include:

        - csv_file: Path to the file with real data
        - url: NiFi endpoint
        - requests_per_second: Number of requests per second for each device

- __Data Ingestion with Apache NiFi__:

    - ListenHTTP: NiFi’s ListenHTTP processor receives each HTTP POST request and captures each line of data.
    - MergeContent: The MergeContent processor groups 1,000 records into a single .txt file for easier storage.
    - After batching records, NiFi extracts the timestamp from the last line in a batch and uses it as the filename.
    - PutS3Object: Each file is uploaded to MinIO storage with filenames set to the extracted timestamp, simplifying file retrieval.

2. __Data Storage in MinIO__

The .txt files from the ingestion process are stored in the env-telemetry-data bucket in MinIO.

3. __Data Transformation and Loading into PostgreSQL DDS__

The s3_to_dds_dag.py DAG in Airflow is set to run every 15 minutes, performing the following steps:

- Retrieves the latest timestamp from the DDS layer and selects all new .txt files from MinIO with names later than the latest timestamp.
- Each .txt file is broken into individual records, converted into a single pandas DataFrame.
- Filters data to include only full minutes.
- Groups the data by minute to calculate average values for key features.
- Cleans the data, removing rows with NaN values or unusually large values.
- Loads the transformed data into the DDS layer in PostgreSQL as structured, time-series records.

4. __Aggregation and Feature Engineering in the DataMart Layer__

The dds_to_dm_dag.py DAG in Airflow is set to run every 60 minutes, performing the following tasks:

- Retrieves the latest timestamp from the DataMart layer and selects all records from the DDS layer later than this timestamp. 
- Filters data to include only full hours.
- Groups data by hour to calculate features for data analysis and machine learning, populating the following tables:

    - dm_iot_average: Stores hourly averages for each variable.
    - dm_iot_extreme: Stores maximum and minimum values for each variable, capturing environmental extremes.
    - dm_iot_count: Tracks the number of records per hour per device.
    - dm_iot_ml: Contains features for machine learning, such as hourly changes, ratios, and other statistical metrics.

- Aggregated data is saved in the DataMart layer tables.

5. Business Intelligence and Visualization in Metabase

- Metabase connects to PostgreSQL, allowing direct access to the DataMart tables for querying and visualization.
- Key metrics, including averages, minimums, and maximums for temperature, humidity, CO, LPG, and smoke, are tracked with time-series plots.
- Users can filter data by device_id and time range.
- A dedicated tab monitors the number of records per hour (expected at 60) to ensure system performance is consistent.

## Settings and Commands

Setup instructions, configuration guidelines, and command-line commands required to initialize and manage each component of the pipeline. 

1. Ensure you have two virtual machines deployed in Yandex Cloud:

![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/YandexCloud%20VMs.png)

2. Configure Airflow for DAG Execution:

On the dmitry-airflow VM:

- Copy the DAG files from the airflow-dags repository folder to the /dags folder on the VM.

- Install required libraries:

```
sudo python3 -m pip install boto3 python-dotenv
```

- Set up the S3 connection by adding environment variables to the airflow-dags/.env file.

- Configure PostgreSQL connections in the Airflow UI, as illustrated below:

![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/airflow_dds_connection.png)
![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/airflow_dm_connection.png)

3. Set Up Main Services on dmitry-de VM:

- Add necessary credentials to the docker-compose file.

- Deploy the containers (only required for the first run):

```
docker-compose up -d
```

- For subsequent runs, start and stop containers without affecting stored data:

```
docker-compose start
docker-compose stop
```

- Connect to the PostgreSQL instance and create two databases:

```
docker exec -it postgres psql -U ... -d postgres
CREATE DATABASE dds_iot_database;
CREATE DATABASE datamart_iot_database;
\q
```

- Confirm that MinIO is running on the specified port:

![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/minio.png)

- Configure NiFi as demonstrated in the screenshots below:

![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/ni-fi.png)
![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/ni-fi_listenhttp_config.png)
![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/ni-fi_mergecontent_config.png)
![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/ni-fi_executescript_config.png)
![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/ni-fi_updateattribute_config.png)
![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/ni-fi_puts3object_config.png)

4. Initialize Data Pipeline:

On the dmitry-airflow VM:

- Run the create_dds_dag and create_datamart_dag DAGs to create empty tables for processed data.
- Activate the s3_to_dds_dag and dds_to_dm_dag DAGs to begin data processing.

5. Generate Data and Start Data Ingestion:

On the dmitry-de VM, choose one of the following options to ingest data:

- For Dummy Data Generation:

```
python3 generate_dummy_data.py --url http://127.0.0.1:8081/loglistener --requests_per_second 10 --total_requests 10000
```

- For Real Data Generation:

```
python3 generate_real_data.py --csv_file raw-data/iot_telemetry_data.csv --url http://127.0.0.1:8081/loglistener --requests_per_second 100
```

6. Verify Data Processing and Visualization:

![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/send_real_data.png)

- Ensure the Airflow DAGs have completed successfully according to the defined schedule:

![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/s3_to_dds_dag.png)
![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/dds_to_dm_dag.png)

- Open the Metabase UI to confirm that data from the DataMart tables has been loaded correctly:

![Alt text](https://github.com/horacemtb/iot-streaming/blob/main/images/metabase_connected.png)