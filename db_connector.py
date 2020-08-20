from pymongo import MongoClient
import os
import json
from datetime import datetime, timedelta
from pprint import pprint

class MongoAPI:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["swaps"]
        self.swaps_collection = self.db.swaps

    def find_swap_by_uuid(self, uuid):
        query = {"uuid": uuid}
        result = self.swaps_collection.find(query)
        return result


    def find_swaps_since_timestamp(self, timestamp):
        query = {"events.event.data.started_at": {"$gt": timestamp}}
        result = self.swaps_collection.find(query)
        return result


    def find_swaps_for_market(self, maker_coin, taker_coin):
       query = {"$and": [{"maker_coin": maker_coin}, {"taker_coin": taker_coin}]}
       result = self.swaps_collection.find(query)
       return result
