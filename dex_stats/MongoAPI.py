import os
import json
from pymongo import MongoClient
from datetime import datetime, timedelta



class MongoAPI:
    def __init__(self):
        self.client           = MongoClient("mongodb://localhost:27017/")
        self.db               = self.client["swaps"]
        self.swaps_collection = self.db.successful
        self.trading_pairs    = self.db.trading_pairs


    def find_swap_by_uuid(self, uuid):
        query  = { "uuid" : uuid }
        result = self.swaps_collection.find(query)
        return dict(result)


    def find_swaps_since_timestamp(self, timestamp):
        query  = { "events.0.event.data.started_at": {"$gt": timestamp} }
        result = self.swaps_collection.find(query)
        return list(result)


    def find_swaps_for_market(self, maker_coin, taker_coin):
        query  = { "$and":[{ "events.0.event.data.maker_coin" : maker_coin },
                           { "events.0.event.data.taker_coin" : taker_coin }] 
                 }
        result = self.swaps_collection.find(query)
        return list(result)
    

    def find_swaps_for_market_since_timestamp(self,
                                              maker_coin,
                                              taker_coin,
                                              timestamp):
        query  = {"$and":[{ "events.0.event.data.maker_coin" : maker_coin },
                          { "events.0.event.data.taker_coin" : taker_coin },
                          { "events.0.event.data.started_at" : {"$gt" : timestamp} }]
                 }
        result = self.swaps_collection.find(query)
        return list(result)


    def get_trading_pairs(self):
        query      = { 'data' : {'$exists' : 'true','$ne': {}} }
        projection = { 'data' : 1 , '_id' : 0 }
        result     = self.trading_pairs.find_one(query, projection=projection)
        return dict(result)['data']
