import argparse
import time
import requests
import pandas as pd
from loguru import logger
import random
from datetime import datetime


# Function to introduce rare empty lines and format row data
def process_row(row):

    timestamp = datetime.fromtimestamp(float(row["ts"])).strftime('%Y-%m-%d %H:%M:%S.%f')
    device_id = row["device"]

    # Introduce a very rare empty line
    if random.random() < 0.001:  # 0.1% chance for an empty line
        return f"{timestamp} {device_id}       "

    co = row["co"]
    humidity = row["humidity"]
    lpg = row["lpg"]
    smoke = row["smoke"]
    temperature = row["temp"]
    light = int(row["light"])

    return f"{timestamp} {device_id} {co} {humidity} {lpg} {smoke} {temperature} {light}"


# Function to send data to NiFi for each row
def send_data(row, url):

    data_entry = process_row(row)

    try:
        response = requests.post(url, data=data_entry)
        if response.status_code == 200:
            logger.info(f"Data sent successfully: {data_entry}")
        else:
            logger.warning(f"Failed to send data: {data_entry} | Status code: {response.status_code}")
    except requests.RequestException as e:
        logger.error(f"Error sending data: {e}")


# Main function to coordinate the sending of data from the CSV
def send_data_from_csv(csv_file, url, requests_per_second):

    data = pd.read_csv(csv_file)
    request_interval = 1 / requests_per_second

    for _, row in data.iterrows():
        row = row.to_dict()
        send_data(row, url)
        time.sleep(request_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send historical IoT data to NiFi')
    parser.add_argument('--csv_file', type=str, required=True, help='Path to the CSV file with historical data')
    parser.add_argument('--url', type=str, required=True, help='NiFi listener URL (e.g., http://127.0.0.1:8081/loglistener)')
    parser.add_argument('--requests_per_second', type=int, default=10, help='Number of requests per second')

    args = parser.parse_args()
    send_data_from_csv(args.csv_file, args.url, args.requests_per_second)
