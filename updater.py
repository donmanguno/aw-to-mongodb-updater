# TODO: Validate required environment variables
# TODO: Set up logging
# TODO: Sync devices as well as data
# TODO: Improve the backfill mechanism

import os
import time
from datetime import datetime, timezone

# Load Environment Variables
from os.path import join, dirname
from dotenv import load_dotenv
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Connect to Database
import pymongo
mongo_client = pymongo.MongoClient(os.environ['MONGODB_ADDRESS'])
mongo_db = mongo_client[os.environ['MONGODB_DATABASE']]
mongo_data_collection = mongo_db[os.environ['MONGODB_DATA_COLLECTION']]
print('database connected')

# Instantiate AmbientAPI
from ambient_api.ambientapi import AmbientAPI
api = AmbientAPI(log_level='console')

# Get device list
devices = api.get_devices()
print(f'got {len(devices)} devices')

# Iterate through devices
for device in devices:
    print(f'device: {device}')
    time.sleep(1)
    
    # Find most recent datum in DB
    latest_entry = mongo_data_collection.find_one({'macAddress' : device.mac_address}, 
                                   sort=[('dateutc', pymongo.DESCENDING)])

    # How many updates (at 5 minute increments) have occurred since the last update in the database
    ts_latest_entry = latest_entry['dateutc'] if latest_entry is not None else int(os.environ['AMBIENT_DATA_START_TIMESTAMP'])
    ts_now = int(time.time()) * 1000
    updates_diff = (ts_now - ts_latest_entry) // (5 * 60000)
    print(f'updates_diff: {updates_diff}')

    # If more than 23.5 hours (282 updates) have elapsed since the last update, set the query end date to 23.5 hours ahead of the last update in the db
    # Otherwise, set the query limit to the number of missed queries plus 2, for a 10 minute overlap
    query_end_date = ts_now if updates_diff < 282 else ts_latest_entry + (86400000 - (30 * 60000))
    query_limit = 288 if updates_diff >= 282 else updates_diff + 2

    # Get device data
    data = device.get_data(limit=query_limit, end_date=query_end_date)
    for datum in data:
        datum['macAddress'] = device.mac_address
    try:
        result = mongo_data_collection.insert_many(data)
    except pymongo.errors.BulkWriteError as e:
        print(e.details['writeErrors'])