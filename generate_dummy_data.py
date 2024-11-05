import argparse
import time
import random
import requests
from datetime import datetime
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed


# Function to generate a random data entry for a device
def generate_data_entry(device_id):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # Randomly decide if this entry will simulate an error with empty values (very rare occurrence)
    if random.random() < 0.01:  # 1% chance for empty values
        return f"{timestamp} {device_id}       "

    # Generate sensor values with a small chance for extreme outliers
    co = round(random.uniform(0.001, 0.01), 8) if random.random() > 0.01 else round(random.uniform(1, 10), 2)  # Outlier chance
    humidity = round(random.uniform(30.0, 70.0), 1) if random.random() > 0.01 else round(random.uniform(0, 100), 1)  # Outlier chance
    lpg = round(random.uniform(0.005, 0.015), 6) if random.random() > 0.01 else round(random.uniform(0.1, 1), 4)  # Outlier chance
    smoke = round(random.uniform(0.01, 0.03), 8) if random.random() > 0.01 else round(random.uniform(0.1, 1), 4)  # Outlier chance
    temperature = round(random.uniform(15.0, 30.0), 1) if random.random() > 0.01 else round(random.uniform(-50, 100), 1)  # Outlier chance

    # Generate light value with a probability of approximately 1/3 for 1
    light = 1 if random.random() < 1/3 else 0

    return f"{timestamp} {device_id} {co} {humidity} {lpg} {smoke} {temperature} {light}"


# Function to send data to NiFi for a single device
def send_data(device_id, url, total_requests, request_interval):
    for _ in range(total_requests):
        data_entry = generate_data_entry(device_id)
        try:
            response = requests.post(url, data=data_entry)
            if response.status_code == 200:
                logger.info(f"Data sent successfully: {data_entry}")
            else:
                logger.warning(f"Failed to send data: {data_entry} | Status code: {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Error sending data: {e}")
        time.sleep(request_interval)


# Main function to coordinate the data sending process for all devices
def send_data_to_nifi(url, requests_per_second, total_requests):
    device_ids = ['b8:27:eb:bf:9d:51', 'b8:27:eb:bf:9d:52', 'b8:27:eb:bf:9d:53']
    request_interval = 1 / requests_per_second

    with ThreadPoolExecutor(max_workers=len(device_ids)) as executor:
        futures = [
            executor.submit(send_data, device_id, url, total_requests, request_interval)
            for device_id in device_ids
        ]

        for future in as_completed(futures):
            try:
                future.result()  # Check for any exceptions in the threads
            except Exception as e:
                logger.error(f"Thread error: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send IoT data to NiFi')
    parser.add_argument('--url', type=str, required=True, help='NiFi listener URL (e.g., http://127.0.0.1:8081/loglistener)')
    parser.add_argument('--requests_per_second', type=int, default=5, help='Number of requests per second per device')
    parser.add_argument('--total_requests', type=int, default=1000, help='Total number of requests to send per device')

    args = parser.parse_args()
    send_data_to_nifi(args.url, args.requests_per_second, args.total_requests)
