from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def _create_dm_tables():
    """
    Creates four datamart tables in the PostgreSQL database for IoT data:
    - dm_iot_average: stores mean values of variables per device/h.
    - dm_iot_extreme: stores max and min values of variables per device/h.
    - dm_iot_count: stores the count of records per device/h.
    - dm_iot_ml: stores ML features inc. statistics, ratios, and changes.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_iot_datamart')

    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS dm_iot_average (
            timestamp TIMESTAMP NOT NULL,
            device_id VARCHAR(17) NOT NULL,
            co NUMERIC(12, 10) NOT NULL,
            humidity NUMERIC(5, 1) NOT NULL,
            lpg NUMERIC(12, 10) NOT NULL,
            smoke NUMERIC(12, 10) NOT NULL,
            temperature NUMERIC(5, 1) NOT NULL,
            light NUMERIC(5, 2) NOT NULL
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS dm_iot_extreme (
            timestamp TIMESTAMP NOT NULL,
            device_id VARCHAR(17) NOT NULL,
            max_co NUMERIC(12, 10) NOT NULL,
            min_co NUMERIC(12, 10) NOT NULL,
            max_humidity NUMERIC(5, 1) NOT NULL,
            min_humidity NUMERIC(5, 1) NOT NULL,
            max_lpg NUMERIC(12, 10) NOT NULL,
            min_lpg NUMERIC(12, 10) NOT NULL,
            max_smoke NUMERIC(12, 10) NOT NULL,
            min_smoke NUMERIC(12, 10) NOT NULL,
            max_temperature NUMERIC(5, 1) NOT NULL,
            min_temperature NUMERIC(5, 1) NOT NULL
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS dm_iot_count (
            timestamp TIMESTAMP NOT NULL,
            device_id VARCHAR(17) NOT NULL,
            record_count INT NOT NULL
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS dm_iot_ml (
            timestamp TIMESTAMP NOT NULL,
            device_id VARCHAR(17) NOT NULL,

            -- Statistical features
            mean_co NUMERIC(12, 10) NOT NULL,
            median_co NUMERIC(12, 10) NOT NULL,
            variance_co NUMERIC(12, 10) NOT NULL,
            mean_humidity NUMERIC(5, 1) NOT NULL,
            median_humidity NUMERIC(5, 1) NOT NULL,
            variance_humidity NUMERIC(5, 2) NOT NULL,
            mean_lpg NUMERIC(12, 10) NOT NULL,
            median_lpg NUMERIC(12, 10) NOT NULL,
            variance_lpg NUMERIC(12, 10) NOT NULL,
            mean_smoke NUMERIC(12, 10) NOT NULL,
            median_smoke NUMERIC(12, 10) NOT NULL,
            variance_smoke NUMERIC(12, 10) NOT NULL,
            mean_temperature NUMERIC(5, 1) NOT NULL,
            median_temperature NUMERIC(5, 1) NOT NULL,
            variance_temperature NUMERIC(5, 2) NOT NULL,

            -- Change over the last hour for all continuous variables
            co_change_last_hour NUMERIC(12, 10) NULL,
            humidity_change_last_hour NUMERIC(5, 1) NULL,
            lpg_change_last_hour NUMERIC(12, 10) NULL,
            smoke_change_last_hour NUMERIC(12, 10) NULL,
            temperature_change_last_hour NUMERIC(5, 1) NULL,

            -- Ratios between variables
            co_to_humidity_ratio NUMERIC(12, 10) NOT NULL,
            smoke_to_temperature_ratio NUMERIC(12, 10) NOT NULL,
            lpg_to_co_ratio NUMERIC(12, 10) NOT NULL,

            -- Variance-to-mean ratios for each variable
            variance_to_mean_co NUMERIC(12, 10) NOT NULL,
            variance_to_mean_humidity NUMERIC(12, 10) NOT NULL,
            variance_to_mean_lpg NUMERIC(12, 10) NOT NULL,
            variance_to_mean_smoke NUMERIC(12, 10) NOT NULL,
            variance_to_mean_temperature NUMERIC(12, 10) NOT NULL,

            -- Percentage of high/low readings
            high_low_percentage_co NUMERIC(5, 2) NOT NULL,
            high_low_percentage_humidity NUMERIC(5, 2) NOT NULL,
            high_low_percentage_lpg NUMERIC(5, 2) NOT NULL,
            high_low_percentage_smoke NUMERIC(5, 2) NOT NULL,
            high_low_percentage_temperature NUMERIC(5, 2) NOT NULL
        );
        """
    ]

    for query in create_table_queries:
        pg_hook.run(query)


with DAG(
    dag_id='create_datamart_dag',
    description='DAG to create datamart tables for IoT data',
    start_date=datetime(2024, 10, 18),
    schedule_interval=None,
    catchup=False,
    tags=['datamart'],
) as dag:

    create_dm_task = PythonOperator(
        task_id='create_dm_tables',
        python_callable=_create_dm_tables
    )
