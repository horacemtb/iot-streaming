from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


def _create_dds_table():
    """
    Creates the DDS IoT data table if it doesn't exist.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_iot_dds')

    sql = """
    CREATE TABLE IF NOT EXISTS dds_iot_data (
        timestamp TIMESTAMP NOT NULL,
        device_id VARCHAR(17) NOT NULL,
        co NUMERIC(12, 10) NOT NULL,
        humidity NUMERIC(5, 1) NOT NULL,
        lpg NUMERIC(12, 10) NOT NULL,
        smoke NUMERIC(12, 10) NOT NULL,
        temperature NUMERIC(5, 1) NOT NULL,
        light SMALLINT NOT NULL CHECK (light IN (0, 1))
    );

    SELECT table_name
    FROM information_schema.tables
    WHERE table_name = 'dds_iot_data';
    """
    pg_hook.run(sql)


with DAG(
    dag_id='create_dds_dag',
    description='DAG to create DDS table for IoT data',
    start_date=datetime(2024, 10, 18),
    schedule_interval=None,
    catchup=False,
    tags=['dds'],
) as dag:

    create_dds_task = PythonOperator(
        task_id='create_dds_table',
        python_callable=_create_dds_table
    )
