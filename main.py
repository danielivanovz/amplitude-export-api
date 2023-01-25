import concurrent.futures
import requests
import time
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import os

# Logging configuration
logging.basicConfig(filename='download.log', level=logging.DEBUG)

# Initial query parameters
start_date = datetime(2022, 1, 1, 0, 0)
end_date = datetime(2022, 1, 30, 23, 0)
query_params = {'start': start_date.strftime('%Y%m%dT%H'), 'end': end_date.strftime('%Y%m%dT%H')}

# Configuration
INCREMENT = 32 # 32 days
MAX_RETRIES = 3 # 3 retries
SLEEP_TIME = 5 # 5 seconds
CONCURRENCY = 3 # 3 concurrent threads

headers = {
    'Authorization': f'Basic {os.environ["API_KEY"]}'
}

def download_file(query_params):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            # Send GET request with query parameters
            response = requests.get('https://amplitude.com/api/2/export', params=query_params, headers=headers, stream=True)

            # Check if the response is successful
            if response.status_code == 200:
                # Get the total file size
                total_size = int(response.headers.get('content-length', 0))
                block_size = 1024 # 1 Kibibyte
                wrote = 0 
                filename = f"{start_date.strftime('%Y%m')}-{end_date.strftime('%Y%m')}.zip"
                with open(filename, 'wb') as f:
                    for data in tqdm(response.iter_content(block_size), total=total_size//block_size, unit='KB', unit_scale=True):
                        wrote = wrote  + len(data)
                        f.write(data)
                print(f"\n{filename} Download Complete!")
                break
            else:
                raise ValueError(f'Error: {response.status_code}')
        except Exception as e:
            retries += 1
            logging.error(e)
            if retries == MAX_RETRIES:
                logging.error(f"Failed to download {filename} after {MAX_RETRIES} retries")
                break
            time.sleep(SLEEP_TIME)
            continue
    # File validation
    if os.path.getsize(filename) == total_size:
        logging.info(f'{filename} is valid')
    else:
        logging.error(f'{filename} is corrupted')
        os.remove(filename)

with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
    while True:
        # Increment the month for the query parameters
        start_date += timedelta(days=INCREMENT)
        end_date += timedelta(days=INCREMENT)
        query_params = {'start': start_date.strftime('%Y%m%dT%H'), 'end': end_date.strftime('%Y%m%dT%H')}
        future = executor.submit(download_file,query_params)
        time.sleep(SLEEP_TIME) # sleep for 30 seconds before next request
