# TODO: Validate required environment variables
# TODO: Set up logging
# TODO: Sync devices as well as data
# TODO: If db doesn't exist create as time series db

import os
import time
from datetime import datetime, timezone
from operator import attrgetter

# Load Environment Variables
from os.path import join, dirname
from dotenv import load_dotenv
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

def get_max_value(list_of_dicts, key):
  if not list_of_dicts:
    return None

  values = [d[key] for d in list_of_dicts if key in d]
  if values:
    return max(values)
  else:
    return None

# Connect to Database
import pymongo
mongo_client = pymongo.MongoClient(os.environ['MONGODB_ADDRESS'])
mongo_db = mongo_client[os.environ['MONGODB_DATABASE']]
mongo_data_collection = mongo_db[os.environ['MONGODB_DATA_COLLECTION']]
print('database connected')

# Instantiate AmbientAPI
from ambient_api.ambientapi import AmbientAPI
api = AmbientAPI()

# Get device list
try: 
    devices = api.get_devices()
    print(f'got {len(devices)} device{"s" if len(devices) > 1 else ""}')
except:
    print('failed to get devices, trying again later')
    exit()

# Iterate through devices
for device in devices:
    print(f'device: {device}')
    time.sleep(1)
    
    # Find most recent datum in DB
    db_latest_entry = mongo_data_collection.find_one({'metadata.device.macAddress' : device.mac_address}, 
                                   sort=[('ts', pymongo.DESCENDING)])
    print(f'latest entry in database: {db_latest_entry}')

    # How many 5 minute increments have occurred since the last update in the database
    ts_latest_entry = db_latest_entry['dateutc'] if db_latest_entry is not None else int(os.environ['AMBIENT_DATA_START_TIMESTAMP'])
    ts_now = int(time.time()) * 1000
    updates_diff = (ts_now - ts_latest_entry) // (5 * 60000)
    print(f'updates_diff: {updates_diff}')

    # If more than 23.5 hours (282 updates) have elapsed since the last update, set the query end date to 23.5 hours ahead of the last update in the db
    # Otherwise set the query end date to now
    api_query_end_date = ts_now if updates_diff < 282 else ts_latest_entry + (86400000 - (30 * 60000))

    # Get existing database data
    db_data = mongo_data_collection.find({'metadata.device.macAddress' : device.mac_address, 'dateutc': {'$lte': api_query_end_date, '$gt': api_query_end_date-86400000}}, 
                                   sort=[('ts', pymongo.DESCENDING)])
    db_data_list = list(db_data) 
    db_data_max_date = get_max_value(db_data_list, 'dateutc')

    # Get device data from API
    api_data = []
    searching = True
    while searching:
        # this logic skips a data gap > 23.5 hours.  We searched for data 23.5 hours newer than the latest data in the db, 
        # but if no such data is returned we'll increment the search period by 6 hours (up to ts_now) and try again
        if (api_query_end_date <= ts_now):
            api_data = device.get_data(limit=288, end_date=api_query_end_date)
            api_data_max_date = get_max_value(api_data, 'dateutc')
            # check the highest ts in the db data vs the highest ts in the api data. If there's nothing new, look farther ahead
            if api_data_max_date is None or api_data_max_date <= db_data_max_date:
                print('no data found newer than that in DB, moving query end date forward 6 hours')
                api_query_end_date = min(ts_now+1, api_query_end_date+21600000)
                time.sleep(5)
            else:
                searching = False
                print(f'retrieved last {len(api_data)} entries')
        else:
            searching = False


    if len(api_data) > 0:
        new_data = []
        for datum in api_data:
            if not any(d['dateutc'] == datum['dateutc'] for d in db_data_list):
                datum['metadata'] = {}
                datum['metadata']['device'] = {}
                datum['metadata']['device']['macAddress'] = device.mac_address
                datum['metadata']['device']['info'] = device.info
                datum['ts'] = datetime.fromtimestamp(datum['dateutc']/1000)
                new_data.append(datum)
        try:
            print(f'inserting {len(new_data)} entries')
            if len(new_data) > 0:
                result = mongo_data_collection.insert_many(new_data)
        except pymongo.errors.BulkWriteError as e:
            print(e.details['writeErrors'])
        print('done')


