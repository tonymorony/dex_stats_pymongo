from pymongo import MongoClient
import os
import json
import sys

client = MongoClient('mongodb://localhost:27017/')

db = client['swaps']

maker_folder_path = "DB/<pubkey>/SWAPS/STATS/MAKER/"
taker_folder_path = "DB/<pubkey>/SWAPS/STATS/TAKER/"

maker_files_list = [pos_json for pos_json in os.listdir(maker_folder_path) if pos_json.endswith('.json')]
taker_files_list = [pos_json for pos_json in os.listdir(taker_folder_path) if pos_json.endswith('.json')]

is_fresh_db = db["swaps"].count_documents({}) < 1

# TODO: probably have to combine maker/taker data for uuid into single document (however not sure if it needed)
for file in maker_files_list:
    with open(maker_folder_path + file) as f:
        data = json.load(f)
        if is_fresh_db:
            db["swaps"].insert(data)
        # TODO: can speedup this process if keep files list from previous run and make update requests only for new
        else:
            db["swaps"].update({"uuid": data["uuid"]}, data, upsert=True)

for file in taker_files_list:
    with open(taker_folder_path + file) as f:
        data = json.load(f)
        if is_fresh_db:
            db["swaps"].insert(data)
        else:
            db["swaps"].update({"uuid": data["uuid"]}, data, upsert=True)

if is_fresh_db:
    resp = db["swaps"].create_index([ ("uuid", 1) ])
