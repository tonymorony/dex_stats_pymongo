from pymongo import MongoClient
import os
import json
import sys

client = MongoClient('mongodb://localhost:27017/')

swaps_db = client.swaps

db_successful_swaps = swaps_db.successful
db_failed_swaps = swaps_db.failed


maker_folder_path = "SWAPS/STATS/MAKER/"
taker_folder_path = "SWAPS/STATS/TAKER/"

maker_files_list = [pos_json for pos_json in os.listdir(maker_folder_path) if pos_json.endswith('.json')]
taker_files_list = [pos_json for pos_json in os.listdir(taker_folder_path) if pos_json.endswith('.json')]


# TODO: probably have to combine maker/taker data for uuid into single document (however not sure if it needed)
for maker_file in maker_files_list:
    with open(maker_folder_path + maker_file) as f:
        data = json.load(f)
        swap_events = [ x['event']['type'] for x in data['events'] ]
        if 'Failed' in swap_events:
            db_failed_swaps.insert(data)
        else:
            db_successful_swaps.insert(data)


for taker_file in taker_files_list:
    with open(taker_folder_path + taker_file) as f:
        data = json.load(f)
        swap_events = [ x['event']['type'] for x in data['events'] ]
        if 'Failed' in swap_events:
            db_failed_swaps.insert(data)
        else:
            db_successful_swaps.update({"uuid": data["uuid"]}, data, upsert=True)


resp = swaps_db.create_index([ ("uuid", 1) ])