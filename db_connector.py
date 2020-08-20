from pymongo import MongoClient
import os
import json
from datetime import datetime, timedelta
from pprint import pprint

client = MongoClient("mongodb://localhost:27017/")

db = client["swaps"]

swaps_collection = db.swaps


def find_swap_by_uuid(uuid):
    query = {"uuid": uuid}
    result = swaps_collection.find(query)
    return result


def find_swaps_since_timestamp(timestamp):
    query = {"events.event.data.started_at": {"$gt": timestamp}}
    result = swaps_collection.find(query)
    return result


def find_swaps_for_market(maker_coin, taker_coin):
    query = {"$and": [{"maker_coin": maker_coin}, {"taker_coin": taker_coin}]}
    result = swaps_collection.find(query)
    return result
    
# temp tests for self-ref
#time_24_hours_ago = datetime.now() - timedelta(hours = 24)
#res = find_swaps_since_timestamp(time_24_hours_ago.timestamp())
#res = find_swap_by_uuid("c68d1e79-18da-4ebd-891a-e514e3e041d7")
#for document in res:
#    pprint(document)
