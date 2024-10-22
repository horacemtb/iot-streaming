from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


def _drop_dm_tables():
    """
    Drops the four datamart tables: dm_iot_average, dm_iot_extreme,
    dm_iot_count, and dm_iot_ml.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_iot_datamart')

    drop_table_queries = [
        "DROP TABLE IF EXISTS dm_iot_average;",
        "DROP TABLE IF EXISTS dm_iot_extreme;",
        "DROP TABLE IF EXISTS dm_iot_count;",
        "DROP TABLE IF EXISTS dm_iot_ml;"
    ]

    for query in drop_table_queries:
        pg_hook.run(query)


with DAG(
    dag_id='drop_datamart_dag',
    description='DAG to drop datamart tables',
    start_date=datetime(2024, 10, 18),
    schedule_interval=None,
    catchup=False,
    tags=['datamart'],
) as dag:

    drop_dm_task = PythonOperator(
        task_id='drop_dm_tables',
        python_callable=_drop_dm_tables
    )
