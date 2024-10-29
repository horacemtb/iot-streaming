# iot-streaming
Data Engineering project. Work in progress!

## Data source

https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k

## Project Architecture

An overview of the project’s architecture, illustrating data flow from ingestion to storage and analysis. This section explains how data moves through each stage of the pipeline and highlights the system’s primary components.

....................

## Components

A detailed breakdown of each component in the pipeline, including their individual roles in the data ingestion, processing, storage, and visualization stages.

....................

## Pipeline

A step-by-step description of the data processing flow, covering everything from raw data ingestion to data transformations and aggregations.

....................

## Settings and Commands

Setup instructions, configuration guidelines, and command-line commands required to initialize and manage each component of the pipeline. 

1. Ensure you have two virtual machines deployed in Yandex Cloud:

![Alt text]()

2. Configure Airflow for DAG Execution:

On the dmitry-airflow VM:

- Copy the DAG files from the airflow-dags repository folder to the /dags folder on the VM.

- Install required libraries:

```
sudo python3 -m pip install boto3 python-dotenv
```

- Set up the S3 connection by adding environment variables to the airflow-dags/.env file.

- Configure PostgreSQL connections in the Airflow UI, as illustrated below:

![Alt text]()

![Alt text]()

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

![Alt text]()

- Configure NiFi as demonstrated in the screenshots below:

![Alt text]()
![Alt text]()
![Alt text]()
![Alt text]()
![Alt text]()
![Alt text]()

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

![Alt text]()

- Ensure the Airflow DAGs have completed successfully according to the defined schedule:

![Alt text]()
![Alt text]()

- Open the Metabase UI to confirm that data from the DataMart tables has been loaded correctly:

![Alt text]()