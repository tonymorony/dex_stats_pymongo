from pymongo import MongoClient
import os
import json
from datetime import datetime, timedelta


class MongoAPI:
    def __init__(self, collection_name):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.swaps = self.client.swaps.collection_name


    def find_swap_by_uuid(self, uuid):
        query = {"uuid": uuid}
        result = self.self.swaps.find(query)
        return result


    def find_swaps_since_timestamp(self, timestamp):
        query = {"events.event.data.started_at": {"$gt": timestamp}}
        result = self.self.swaps.find(query)
        return result


    def find_swaps_for_market(self, maker_coin, taker_coin):
       query = {"$and": [{"maker_coin": maker_coin}, {"taker_coin": taker_coin}]}
       result = self.self.swaps.find(query)
       return result


    def find_successful_swap_pairs(self):
        pass
    
