from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import boto3
import pandas as pd
import numpy as np
import logging
import os
import dotenv

dotenv.load_dotenv()

MINIO_IP = os.getenv('MINIO_IP')
MINIO_PORT = os.getenv('MINIO_PORT')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

POSTGRES_CONN_ID = 'postgres_iot_dds'
PG_TABLE = 'dds_iot_data'

log = logging.getLogger(__name__)


def get_latest_timestamp_from_db():
    """
    Retrieves the latest timestamp from the specified PostgreSQL table using the PostgresHook.

    This function connects to a PostgreSQL database via Airflow's PostgresHook, runs a query to
    fetch the maximum timestamp from a specified table, and returns that timestamp. If no timestamp
    is found in the table (i.e., the table is empty), it returns a default value of `datetime.min`.

    Returns:
        datetime: The latest timestamp from the table if present, or `datetime.min` if no timestamp is found.
    """
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_query = f"SELECT MAX(timestamp) FROM {PG_TABLE};"
    latest_timestamp = postgres_hook.get_first(sql_query)[0]

    # Return the latest timestamp or a default value if none is found
    return latest_timestamp if latest_timestamp is not None else datetime.min


def filename_to_timestamp(filename):
    """
    Converts a filename in the format 'year-month-day_hour:minute:second.milliseconds.txt' to a datetime object.

    Args:
        filename (str): The filename to convert.

    Returns:
        datetime: A datetime object representing the timestamp in the filename.
    """
    # Remove only the '.txt' extension to correctly preserve the timestamp, including milliseconds
    timestamp_str = filename.replace('.txt', '').split('/')[-1]

    # Convert the timestamp string into a datetime object
    return pd.to_datetime(timestamp_str, format='%Y-%m-%d_%H:%M:%S.%f')


def _extract_and_process_data_from_s3():
    """
    This function extracts and processes data from S3 (MinIO). It connects to the specified S3 bucket,
    checks for new files based on the latest timestamp stored in the database, and processes those files
    into a DataFrame for further analysis.
    """
    try:
        latest_timestamp = get_latest_timestamp_from_db()
        log.info(f"Latest timestamp as recorded in the {PG_TABLE} table: {latest_timestamp}")
    except Exception as e:
        log.error(f"Failed to retrieve the latest timestamp from the database: {e}")
        return

    try:
        # Create a session with MinIO
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=f'http://{MINIO_IP}:{MINIO_PORT}',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )

        # List all files in the S3 bucket
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        files = response.get('Contents', [])
        log.info("Connected to S3.")

    except Exception as e:
        log.error(f"Failed to connect to S3 or list objects: {e}")
        return

    # Filter files by timestamp, only including those newer than the latest timestamp from the database
    valid_files = [file['Key'] for file in files if filename_to_timestamp(file['Key']) > latest_timestamp]

    if not valid_files:
        log.warning("No new files found in S3 that are newer than the latest timestamp in the dds layer.")
        return

    log.info(f"New files since last fetch: {len(valid_files)}")

    try:
        log.info("Processing and aggregating data...")
        combined_df = pd.DataFrame()

        for file_key in valid_files:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)

            file_content = obj['Body'].read().decode('utf-8').splitlines()

            # Parse each line into a DataFrame, assuming a space-separated format, for example: 2024-10-18 11:53:32.234140 b8:27:eb:bf:9d:53 0.00659914 53.9 0.005256 0.01950598 23.1 1
            # Also handle cases with missing data
            data = [
                [' '.join(line.split()[:2])] + line.split()[2:]
                for line in file_content if len(line.split()) == 9
                ]

            df = pd.DataFrame(data, columns=['timestamp', 'device_id', 'co', 'humidity', 'lpg', 'smoke', 'temperature', 'light'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

            # Take a slice of data strictly less than the latest timestamp from the DataFrame rounded down to the nearest minute
            max_timestamp_in_df = df['timestamp'].max()
            df = df[(df['timestamp'] > latest_timestamp) & (df['timestamp'] < max_timestamp_in_df.floor('min'))]

            combined_df = pd.concat([combined_df, df], ignore_index=True)

        combined_df = combined_df.sort_values(by='timestamp').reset_index(drop=True)

        # Handle outliers in continuous features
        continuous_features = ['co', 'humidity', 'lpg', 'smoke', 'temperature']
        for feature in continuous_features:
            q_low = combined_df[feature].astype(float).quantile(0.01)
            q_high = combined_df[feature].astype(float).quantile(0.99)
            combined_df[feature] = np.where((combined_df[feature].astype(float) < q_low) | (combined_df[feature].astype(float) > q_high), np.nan, combined_df[feature].astype(float))
            combined_df[feature].interpolate(method='linear', inplace=True)

        # Calculate average values grouped by minute for each device
        aggregated_df = combined_df.groupby([pd.Grouper(key='timestamp', freq='min'), 'device_id']).agg(
            {
                'co': 'mean',
                'humidity': 'mean',
                'lpg': 'mean',
                'smoke': 'mean',
                'temperature': 'mean',
                'light': lambda x: x.mode()[0]
                }
                ).reset_index()

        log.info("Data processing and aggregation completed successfully.")

    except Exception as e:
        log.error(f"An error occurred during data extraction or processing: {e}")

    try:
        # Convert numpy.datetime64 to Python's native datetime object
        aggregated_df['timestamp'] = aggregated_df['timestamp'].apply(lambda x: pd.Timestamp(x).to_pydatetime())
        records_to_insert = [tuple(row) for row in aggregated_df.itertuples(index=False, name=None)]

        # Insert the aggregated data into the target PostgreSQL table using PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        postgres_hook.insert_rows(
            table=PG_TABLE,
            rows=records_to_insert,
            target_fields=['timestamp', 'device_id', 'co', 'humidity', 'lpg', 'smoke', 'temperature', 'light']
            )
        log.info(f"Successfully inserted {len(records_to_insert)} records into {PG_TABLE}.")

    except Exception as e:
        log.error(f"Error inserting data into PostgreSQL: {e}")

    try:
        delete_duplicates_query = f"""
        WITH duplicates AS (
            SELECT ctid,
                ROW_NUMBER() OVER (PARTITION BY timestamp, device_id ORDER BY ctid) AS row_num
            FROM {PG_TABLE}
        )
        DELETE FROM {PG_TABLE}
        WHERE ctid IN (SELECT ctid FROM duplicates WHERE row_num > 1);
        """
        # Execute the delete query
        postgres_hook.run(delete_duplicates_query)
        log.info("Duplicate rows deleted successfully.")
    except Exception as e:
        log.error(f"Error deleting duplicates from the table: {str(e)}")


with DAG(
    dag_id='s3_to_dds_dag',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2024, 10, 18),
    catchup=False,
    max_active_runs=1,
) as dag:

    process_and_insert_data = PythonOperator(
        task_id='extract_and_process_data_from_s3',
        python_callable=_extract_and_process_data_from_s3,
    )
