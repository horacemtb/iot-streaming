[Русский](README.ru.md)

# IoT Data Processing Pipeline Using the ELT Workflow

__Project Goal__: Develop a system for streaming ingestion, processing, storage, and visualization of IoT telemetry data. Data is collected in real time using Apache NiFi, stored in MinIO object storage, and loaded into PostgreSQL for analytical processing. Apache Airflow orchestrates data processing and transformations, while PGAdmin and Metabase enable easy access, analysis, and visualization, providing users with structured insights for further exploration.

## Data source

https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k

## Project Architecture

An overview of the project’s architecture, illustrating data flow from ingestion to storage and analysis. This section explains how data moves through each stage of the pipeline and highlights the system’s primary components.

....................

## Components

A detailed breakdown of each component in the pipeline, including their individual roles in the data ingestion, processing, storage, and visualization stages.

1. __dmitry-airflow Virtual Machine__

- __Role__: Orchestration and Processing Hosting
- __Description__:
    - __Deployment__: This virtual machine hosts the Airflow service and is responsible for orchestrating data workflows in the pipeline.
    - __Functionality__: dmitry-airflow serves as the primary orchestration point for all scheduled data processing. Airflow DAGs on this VM coordinate data retrieval from MinIO, data transformation, and loading into PostgreSQL.
    - __Configuration__: Airflow connects to both MinIO and PostgreSQL using environment-specific hooks to execute scheduled tasks, maintaining the flow of data from raw ingestion to processed storage.

2. __dmitry-de Virtual Machine__

- __Role__: Data Ingestion, Storage, and Visualization Hosting
- __Description__:
    - __Deployment__: This VM hosts the services for data ingestion, storage, and visualization, including NiFi, MinIO, PostgreSQL, PGAdmin, and Metabase, all containerized within Docker.
    - __Functionality__: The dmitry-de VM manages the initial data ingestion from IoT devices, object storage via MinIO, and data warehousing with PostgreSQL. It also runs Metabase for visualizing processed data.
    - __Configuration__: Each service on this VM operates within its Docker container, with Docker Compose ensuring streamlined deployment, start-up, and maintenance. Service credentials and configurations are managed within Docker to maintain secure and consistent settings across restarts.

3. __Apache NiFi__

- __Role__: Data Ingestion and Preprocessing
- __Description__:
    - __Deployment__: NiFi runs on the dmitry-de virtual machine within a Docker container.
    - __Functionality__: NiFi ingests real-time IoT data using its ListenHTTP processor, which accepts incoming data from IoT devices. It preprocesses this data by merging records with the MergeContent processor and storing the results in MinIO using the PutS3Object processor.
    - __Configuration__: NiFi is configured to handle high-throughput data ingestion, with each processor customized to control data batch size and upload frequency to MinIO. Timestamps generated in NiFi help in organizing data files stored in MinIO for easy retrieval and tracking.

4. __MinIO__

- __Role__: Raw Data Object Storage
- __Description__:
    - __Deployment__: MinIO runs as a containerized service on the dmitry-de virtual machine.
    - __Functionality__: MinIO provides an S3-compatible storage layer for handling large volumes of IoT data. It receives batch files from NiFi and stores them in the txt format, making them easily accessible for Airflow processing.
    - __Configuration__: Data files in MinIO are timestamped and organized by ingestion time, allowing Airflow DAGs to retrieve the latest files efficiently for further processing. MinIO’s interface allows for easy management and scaling to handle significant storage needs.

5. __Apache Airflow__

- __Role__: Workflow Orchestration and Scheduling
- __Description__:
    - __Deployment__: Airflow runs on the dmitry-airflow virtual machine.
    - __Functionality__: Airflow automates and schedules data processing workflows by running custom DAGs that manage the flow of IoT data from raw ingestion to structured storage in PostgreSQL.
    - __DAGs__:
        - __create_dds_dag__ and __create_datamart_dag__: These DAGs initialize the PostgreSQL environment by creating tables for the DDS (dds_iot_database) and Datamart (datamart_iot_database) layers, structuring the databases for efficient data storage and analysis.
        - __drop_dds_dag__ and __drop_datamart_dag__: These DAGs remove all of the tables from the DDS (dds_iot_database) and Datamart (datamart_iot_database) layers
        - __s3_to_dds_dag__: This DAG periodically retrieves raw IoT data files from MinIO, processes and aggregates the data, and loads it into the DDS layer in PostgreSQL, organizing data for easy querying.
        - __dds_to_dm_dag__: This DAG runs after the data has been moved to the DDS layer, further processing and aggregating it into the Datamart tables for analytics-ready format.
    - __Tables__:
        - __DDS Layer (dds_iot_database)__: Stores detailed, timestamped data for each IoT reading, enabling granular historical analysis.
        - __Datamart Layer (datamart_iot_database)__: Contains pre-aggregated tables:
            - dm_iot_average (average metrics per variable by hour),
            - dm_iot_extreme (extreme values by hour),
            - dm_iot_count (record counts by hour),
            - dm_iot_ml (ML-ready statistical features by hour).
    - __Configuration__: Each DAG is configured to run at specific intervals, with s3_to_dds_dag and dds_to_dm_dag ensuring timely ingestion and transformation of new data. Connection hooks for MinIO and PostgreSQL streamline data transfer and enable efficient automation of all pipeline stages.

6. __PostgreSQL__

- __Role__: Data Warehouse
- __Description__:
    - __Deployment__: PostgreSQL runs in a Docker container on the dmitry-de virtual machine.
    - __Functionality__: PostgreSQL stores processed IoT data, divided into dds_iot_database for detailed data storage and datamart_iot_database for aggregated analytical and ML data. This layered structure supports efficient querying and historical analysis.
    - __Configuration__: Tables are organized for easy querying and reporting, with specific aggregations for hourly data. PostgreSQL is optimized for fast access to historical data, supporting both analytics and visualization needs.

7. __PGAdmin__

- __Role__: Database Management Interface
- __Description__:
    - __Deployment__: Runs in a Docker container on the dmitry-de VM.
    - __Functionality__: PGAdmin provides a web-based interface to manage, query, and inspect PostgreSQL databases. Users can visually manage both the DDS and Datamart databases, run SQL queries, and inspect data tables for validation and reporting.
    - __Configuration__: PGAdmin is connected to PostgreSQL on the same VM, allowing convenient access for database management and debugging through a dedicated web interface.

8. __Metabase__

- __Role__: Data Visualization
- __Description__:
    - __Deployment__: Metabase is containerized and runs on the dmitry-de virtual machine.
    - __Functionality__: Metabase enables interactive data visualization, allowing users to explore the aggregated IoT data stored in the PostgreSQL datamart. Custom dashboards provide insights into trends and metrics, with options for filtering by device_id and time range.
    - __Configuration__: Metabase connects directly to the datamart_iot_database in PostgreSQL, pulling pre-aggregated data tables for efficient, real-time visualizations. Dashboards are set up for easy access, enabling stakeholders to monitor and interpret IoT data.

9. __Docker__

- __Role__: Containerization and Service Management
- __Description__:
    - __Deployment__: Docker runs on both the dmitry-de and dmitry-airflow virtual machines, containerizing NiFi, MinIO, PostgreSQL, and Metabase on the dmitry-de VM.
    - __Functionality__: Docker isolates each service, ensuring consistent environments and simplifying management, scaling, and deployment. Docker Compose is used to orchestrate multi-container services, allowing easy startup, shutdown, and maintenance.
    - __Configuration__: Each container has its own configuration settings in Docker Compose, which manages environment variables, storage settings, and network configurations.

## Pipeline

A step-by-step description of the data processing flow, covering everything from raw data ingestion to data transformations and aggregations.

....................

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