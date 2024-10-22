from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


def _drop_dds_table():
    """
    Drops the DDS table 'dds_iot_data' if it exists.

    The function uses a PostgresHook to connect to the 'postgres_iot_dds'
    Postgres instance, execute a DROP TABLE command, and verify if table exists
    by checking the 'information_schema.tables'.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_iot_dds')

    sql = """
    DROP TABLE IF EXISTS dds_iot_data;

    SELECT table_name
    FROM information_schema.tables
    WHERE table_name = 'dds_iot_data';
    """
    pg_hook.run(sql)


with DAG(
    dag_id='drop_dds_dag',
    description='DAG to drop DDS table',
    start_date=datetime(2024, 10, 18),
    schedule_interval=None,
    catchup=False,
    tags=['dds'],
) as dag:

    drop_dds_task = PythonOperator(
        task_id='drop_dds_table',
        python_callable=_drop_dds_table
    )
