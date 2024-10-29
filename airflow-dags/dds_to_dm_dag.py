from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging

log = logging.getLogger(__name__)

DDS_CONN_ID = 'postgres_iot_dds'
DATAMART_CONN_ID = 'postgres_iot_datamart'
DATAMART_TABLES = {
    'dm_iot_average': [
        'timestamp', 'device_id', 'co', 'humidity',
        'lpg', 'smoke', 'temperature', 'light'
    ],
    'dm_iot_extreme': [
        'timestamp', 'device_id',
        'max_co', 'min_co',
        'max_humidity', 'min_humidity',
        'max_lpg', 'min_lpg',
        'max_smoke', 'min_smoke',
        'max_temperature', 'min_temperature'
    ],
    'dm_iot_count': [
        'timestamp', 'device_id', 'record_count'
    ],
    'dm_iot_ml': [
        'timestamp', 'device_id',
        'mean_co', 'median_co', 'variance_co',
        'mean_humidity', 'median_humidity', 'variance_humidity',
        'mean_lpg', 'median_lpg', 'variance_lpg',
        'mean_smoke', 'median_smoke', 'variance_smoke',
        'mean_temperature', 'median_temperature', 'variance_temperature',
        'co_change_last_hour', 'humidity_change_last_hour',
        'lpg_change_last_hour', 'smoke_change_last_hour',
        'temperature_change_last_hour',
        'co_to_humidity_ratio', 'smoke_to_temperature_ratio',
        'lpg_to_co_ratio',
        'variance_to_mean_co', 'variance_to_mean_humidity',
        'variance_to_mean_lpg', 'variance_to_mean_smoke',
        'variance_to_mean_temperature',
        'high_low_percentage_co', 'high_low_percentage_humidity',
        'high_low_percentage_lpg', 'high_low_percentage_smoke',
        'high_low_percentage_temperature'
    ]
}


def get_latest_date():
    """
    Retrieve the latest timestamp from the datamart's average table.

    Returns:
        datetime: The latest timestamp found, or the minimum datetime if none exists.
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=DATAMART_CONN_ID)
        query = "SELECT MAX(timestamp) FROM dm_iot_average"
        latest_date = postgres_hook.get_first(query)[0]
        log.info(f"Latest date retrieved from datamart: {latest_date}")
        return latest_date if latest_date is not None else datetime.min
    except Exception as e:
        log.error(f"Error retrieving latest date from datamart: {e}")
        raise


def load_dds_data(latest_date):
    """
    Load data from the DDS table that is newer than the specified latest date.

    Args:
        latest_date (datetime): The date to filter the data.

    Returns:
        DataFrame: A pandas DataFrame containing the filtered data.
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=DDS_CONN_ID)
        query = f"""
        SELECT * FROM dds_iot_data
        WHERE timestamp > '{latest_date}'
        """
        dds_df = postgres_hook.get_pandas_df(query)
        log.info(f"Loaded {len(dds_df)} records from DDS after {latest_date}")
        return dds_df
    except Exception as e:
        log.error(f"Error loading data from DDS: {e}")
        raise


def filter_full_hours(dds_df):
    """
    Filter the DDS DataFrame to include only complete hours.

    Args:
        dds_df (DataFrame): The DataFrame to filter.

    Returns:
        DataFrame: A DataFrame containing only complete hour data.
    """
    max_date = dds_df['timestamp'].max().floor('H')
    dds_df = dds_df[dds_df['timestamp'] < max_date]
    return dds_df


def generate_average_features(df):
    """
    Generate average features for each device by hour.

    Args:
        df (DataFrame): The input DataFrame with device data.

    Returns:
        DataFrame: A DataFrame with average values for each device per hour.
    """
    return df.groupby([pd.Grouper(key='timestamp', freq='H'), 'device_id']).agg(
        co=('co', 'mean'),
        humidity=('humidity', 'mean'),
        lpg=('lpg', 'mean'),
        smoke=('smoke', 'mean'),
        temperature=('temperature', 'mean'),
        light=('light', 'mean')
    ).reset_index()


def generate_extreme_features(df):
    """
    Generate extreme features (max/min) for each device by hour.

    Args:
        df (DataFrame): The input DataFrame with device data.

    Returns:
        DataFrame: A DataFrame with extreme values for each device per hour.
    """
    return df.groupby([pd.Grouper(key='timestamp', freq='H'), 'device_id']).agg(
        max_co=('co', 'max'),
        min_co=('co', 'min'),
        max_humidity=('humidity', 'max'),
        min_humidity=('humidity', 'min'),
        max_lpg=('lpg', 'max'),
        min_lpg=('lpg', 'min'),
        max_smoke=('smoke', 'max'),
        min_smoke=('smoke', 'min'),
        max_temperature=('temperature', 'max'),
        min_temperature=('temperature', 'min')
    ).reset_index()


def generate_count_features(df):
    """
    Count the number of records for each device by hour.

    Args:
        df (DataFrame): The input DataFrame with device data.

    Returns:
        DataFrame: A DataFrame with record counts for each device per hour.
    """
    return df.groupby([pd.Grouper(key='timestamp', freq='H'), 'device_id']).size().reset_index(name='record_count')


def generate_ml_features(df):
    """
    Generate machine learning features for each device by hour.

    Args:
        df (DataFrame): The input DataFrame with device data.

    Returns:
        DataFrame: A DataFrame with various statistical features for ML models.
    """
    grouped_df = df.groupby([pd.Grouper(key='timestamp', freq='H'), 'device_id'])
    ml_features = grouped_df.agg(
        mean_co=('co', 'mean'),
        median_co=('co', 'median'),
        variance_co=('co', np.var),
        mean_humidity=('humidity', 'mean'),
        median_humidity=('humidity', 'median'),
        variance_humidity=('humidity', np.var),
        mean_lpg=('lpg', 'mean'),
        median_lpg=('lpg', 'median'),
        variance_lpg=('lpg', np.var),
        mean_smoke=('smoke', 'mean'),
        median_smoke=('smoke', 'median'),
        variance_smoke=('smoke', np.var),
        mean_temperature=('temperature', 'mean'),
        median_temperature=('temperature', 'median'),
        variance_temperature=('temperature', np.var)
    ).reset_index()

    # Calculate changes over the last hour
    ml_features['co_change_last_hour'] = ml_features.groupby('device_id')['mean_co'].diff()
    ml_features['humidity_change_last_hour'] = ml_features.groupby('device_id')['mean_humidity'].diff()
    ml_features['lpg_change_last_hour'] = ml_features.groupby('device_id')['mean_lpg'].diff()
    ml_features['smoke_change_last_hour'] = ml_features.groupby('device_id')['mean_smoke'].diff()
    ml_features['temperature_change_last_hour'] = ml_features.groupby('device_id')['mean_temperature'].diff()

    # Calculate ratios and variance-to-mean ratios
    ml_features['co_to_humidity_ratio'] = ml_features['mean_co'] / ml_features['mean_humidity']
    ml_features['smoke_to_temperature_ratio'] = ml_features['mean_smoke'] / ml_features['mean_temperature']
    ml_features['lpg_to_co_ratio'] = ml_features['mean_lpg'] / ml_features['mean_co']
    ml_features['variance_to_mean_co'] = ml_features['variance_co'] / ml_features['mean_co']
    ml_features['variance_to_mean_humidity'] = ml_features['variance_humidity'] / ml_features['mean_humidity']
    ml_features['variance_to_mean_lpg'] = ml_features['variance_lpg'] / ml_features['mean_lpg']
    ml_features['variance_to_mean_smoke'] = ml_features['variance_smoke'] / ml_features['mean_smoke']
    ml_features['variance_to_mean_temperature'] = ml_features['variance_temperature'] / ml_features['mean_temperature']

    # Calculate high/low percentage for each variable
    for variable in ['co', 'humidity', 'lpg', 'smoke', 'temperature']:
        high_low_percentage = df.groupby([pd.Grouper(key='timestamp', freq='H'), 'device_id'])[variable].apply(
            lambda x: (x > x.mean()).sum() / len(x) * 100).reset_index(name=f'high_low_percentage_{variable}')

        # Merge back into ml_features
        ml_features = ml_features.merge(high_low_percentage, on=['timestamp', 'device_id'], how='left')

    return ml_features


def remove_duplicates(table_name, postgres_hook):
    """
    Remove duplicate records from a specified datamart table.

    Args:
        table_name (str): The name of the table from which to remove duplicates.
        postgres_hook (PostgresHook): The PostgreSQL hook for executing queries.
    """
    try:
        delete_duplicates_query = f"""
        WITH duplicates AS (
            SELECT ctid,
                ROW_NUMBER() OVER (PARTITION BY timestamp, device_id ORDER BY ctid) AS row_num
            FROM {table_name}
        )
        DELETE FROM {table_name}
        WHERE ctid IN (SELECT ctid FROM duplicates WHERE row_num > 1);
        """
        postgres_hook.run(delete_duplicates_query)
        log.info("Duplicate rows deleted successfully.")
    except Exception as e:
        log.error(f"Error deleting duplicates from table {table_name}: {str(e)}")
        raise


def insert_into_datamart(df, table_name, columns):
    """
    Insert a DataFrame into a specified datamart table.

    Args:
        df (DataFrame): The DataFrame to insert.
        table_name (str): The name of the table.
        columns (list): The list of columns in the DataFrame to insert.
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id=DATAMART_CONN_ID)
        records_to_insert = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]
        postgres_hook.insert_rows(
            table=table_name,
            rows=records_to_insert,
            target_fields=columns
        )
        log.info(f"Successfully inserted {len(records_to_insert)} records into {table_name}.")
        remove_duplicates(table_name, postgres_hook)
    except Exception as e:
        log.error(f"Error inserting data into PostgreSQL: {e}")
        raise


def _process_data():
    """
    Main function to process data from the DDS to the datamart.

    This function retrieves the latest date from the datamart,
    loads the corresponding data from the DDS, filters the data
    for complete hours, and generates various features (average,
    extreme, count, and ML features) before inserting them into
    their respective tables in the datamart.
    """
    try:
        # Retrieve the latest date from the datamart to ensure new data is loaded
        latest_date = get_latest_date()

        # Load data from the DDS that is more recent than the latest date
        dds_df = load_dds_data(latest_date)

        # Filter the DataFrame to include only complete hour entries
        dds_df = filter_full_hours(dds_df)

        # Generate average features from the filtered DataFrame and insert into the 'dm_iot_average' table
        average_df = generate_average_features(dds_df)
        insert_into_datamart(average_df, 'dm_iot_average', DATAMART_TABLES['dm_iot_average'])

        # Generate extreme features from the filtered DataFrame and insert into the 'dm_iot_extreme' table
        extreme_df = generate_extreme_features(dds_df)
        insert_into_datamart(extreme_df, 'dm_iot_extreme', DATAMART_TABLES['dm_iot_extreme'])

        # Generate count features from the filtered DataFrame and insert into the 'dm_iot_count' table
        count_df = generate_count_features(dds_df)
        insert_into_datamart(count_df, 'dm_iot_count', DATAMART_TABLES['dm_iot_count'])

        # Generate ML features from the filtered DataFrame and insert into the 'dm_iot_ml' table
        ml_features_df = generate_ml_features(dds_df)
        insert_into_datamart(ml_features_df, 'dm_iot_ml', DATAMART_TABLES['dm_iot_ml'])

        log.info("Data processing completed successfully.")
    except Exception as e:
        log.error(f"Error in data processing: {e}")
        return


with DAG(
    dag_id='dds_to_dm_dag',
    schedule_interval='@hourly',
    default_args={'start_date': datetime(2024, 10, 18), 'retries': 1, 'retry_delay': timedelta(minutes=5)},
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=_process_data
    )
