#!/usr/bin/env python3
from gevent import monkey
_ = monkey.patch_all()

from pymongo import MongoClient, ReturnDocument
from functools import wraps
from timeit import timeit
from time import time

import logging
import json
import sys
import os



def measure(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time() * 1000)) - start
            print(f"Total execution time for {func.__name__} : {end_ if end_ > 0 else 0} ms")
    return _time_it


class Parser_Error(Exception):
    pass


class ArgumentInputParserError(Parser_Error):
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


db = client['swaps']

class DB_Parser():
    def __init__(self,                parse_uuid=True,
                 async_mode=False,    parse_pairs=True,
                 parse_maker=True,    parse_taker=True,
                 save_failed=True,    save_successful=True,
                 data_analysis=False, use_swap_events=True,
                 mongo_port=27017,    mongo_ip='localhost',
                 maker_folder_path="../SWAPS/STATS/MAKER/",
                 taker_folder_path="../SWAPS/STATS/TAKER/"):
        #maker_folder_path = "DB/<pubkey>/SWAPS/STATS/MAKER/"
        #taker_folder_path = "DB/<pubkey>/SWAPS/STATS/TAKER/"
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        #raise if both are False
        if not save_failed and not save_successful:
            raise ArgumentInputParserError('save_failed=False, save_successful=False,',
                  'you cant have both save_failed and save_successful as False at the'
                  ' same time. Parser has nothing to do...')

        #raise if both are False
        if not parse_maker and not parse_taker:
            raise ArgumentInputParserError('parse_maker=False, parse_taker=False,',
             'you cant have both parse_maker and parse_taker as False at the same'
             ' time. Parser has nothing to do...')

        # Parser config
        self.parse_maker = parse_maker
        self.parse_taker = parse_taker
        self.async_mode = async_mode
        self.save_failed = save_failed
        self.parse_pairs = parse_pairs
        self.data_analysis = data_analysis
        self.use_swap_events = use_swap_events
        self.save_successful = save_successful
        self.maker_folder_path = maker_folder_path
        self.taker_folder_path = taker_folder_path

        #init mongo client connection and create swaps db
        self.client = MongoClient('mongodb://{}:{}/'.format(mongo_ip, mongo_port))
        self.swaps = self.client.swaps

        #creating main successful/failed collections
        if self.save_failed:
            self.failed = self.swaps.failed_swaps_collection

        if self.save_successful:
            self.successful = self.swaps.successsful_swaps_collection

        #enabling collection of top swap pairs
        if self.parse_pairs:
            self.pairs = self.swaps.top_pairs_collection

        #enabling collection of uuids with timestamp
        if self.parse_uuid:
            self.uuids = self.swaps.uuids_with_timestamp_collection

        #enabling extensive collection of swap info for data analysis
        if self.data_analysis:
            self.data = self.swaps.data_analysis_collection

        #enabling swap events navigation
        if self.use_swap_events:
            self.maker_swap_success_events = [
                "Started",
                "Negotiated",
                "TakerFeeValidated",
                "MakerPaymentSent",
                "TakerPaymentReceived",
                "TakerPaymentWaitConfirmStarted",
                "TakerPaymentValidatedAndConfirmed",
                "TakerPaymentSpent",
                "Finished"
            ]

            self.taker_swap_success_events = [
                "Started",
                "Negotiated",
                "TakerFeeSent",
                "MakerPaymentReceived",
                "MakerPaymentWaitConfirmStarted",
                "MakerPaymentValidatedAndConfirmed",
                "TakerPaymentSent",
                "TakerPaymentSpent",
                "MakerPaymentSpent",
                "Finished"
            ]

            self.maker_swap_error_events = [
                "StartFailed",
                "NegotiateFailed",
                "TakerFeeValidateFailed",
                "MakerPaymentTransactionFailed",
                "MakerPaymentDataSendFailed",
                "TakerPaymentValidateFailed",
                "TakerPaymentSpendFailed",
                "MakerPaymentRefunded",
                "MakerPaymentRefundFailed"
            ]

            self.taker_swap_error_events = [
                "StartFailed",
                "NegotiateFailed",
                "TakerFeeSendFailed",
                "MakerPaymentValidateFailed",
                "MakerPaymentWaitConfirmFailed",
                "TakerPaymentTransactionFailed",
                "TakerPaymentWaitConfirmFailed",
                "TakerPaymentDataSendFailed",
                "TakerPaymentWaitForSpendFailed",
                "MakerPaymentSpendFailed",
                "TakerPaymentWaitRefundStarted",
                "TakerPaymentRefunded",
                "TakerPaymentRefundFailed"
            ]


    def __str__(self):
        config = ('parser-conf:'
                 '\n  async={}'
                 '\n  data_analysis={}').format(self.async_mode,
                                                self.data_analysis)
        return config


    # ~ 40-50ms each
    @measure
    def create_maker_files_pool(self):
        self.maker_files_pool = [ x
                                  for x
                                  in os.listdir(self.maker_folder_path)
                                  if x.endswith('.json') ]


    @measure
    def create_taker_files_pool(self):
        self.taker_files_pool = [ x
                                  for x
                                  in os.listdir(self.taker_folder_path)
                                  if x.endswith('.json') ]


    #TODO: parse uuids, if there are new uuids, update else do nothing
    #      also could be good to save last timestamp or check only for
    #      swaps that happen/created in the last 24h
    @measure
    def update_maker_files_pool(self):
        self.maker_files_pool = [ x
                                  for x
                                  in os.listdir(self.maker_folder_path)
                                  if x.endswith('.json') ]


    @measure
    def update_taker_files_pool(self):
        self.taker_files_pool = [ x
                                  for x
                                  in os.listdir(self.taker_folder_path)
                                  if x.endswith('.json') ]


    # hmmm do we really need to know this?
    def is_db_fresh(self):
        return not bool(self.swaps.list_collection_names())


    def check_connection_to_mongo(self):
        return self.swaps



    ### SWAP FILES PARSING FUNCTIONS

    @measure

    def parse_swap_data(self, path_to_swap_json : str):
        """Parses json from filepath and (returns : dict)"""
        with open(path_to_swap_json) as f:
            return json.load(f)


    @measure
    def parse_swap_events(self, swap : dict):
        """Parses (swap : dict) and (returns : list) of swap events."""
        return [ x['event']['type'] for x in swap['events'] ]


    @measure
    def is_swap_finished(self, swap_events : list):
        """Checks if (swap_events : list) has Finished event (returns : bool). """
        return True if 'Finished' in swap_events else False


    @measure
    def is_swap_successful(self, swap_events : list):
        """Checks if swap has Finished successfully, (returns : bool). """
        return True if (swap_events == self.maker_swap_success_events or
                        swap_events == self.taker_swap_success_events) else False


    @measure
    def parse_traiding_pair(self, swap : dict):
        try:
            return {
                    'maker_coin' : swap['maker_coin'],
                    'taker_coin' : swap['taker_coin'],
                    'count' : 0
                    }
        except KeyError:
            try:
                start_event_data = swap['events'][0]['event']['data']
                return {
                        'maker_coin' : start_event_data['maker_coin'],
                        'taker_coin' : start_event_data['taker_coin'],
                        'count' : 0
                        }
            except KeyError:
                logging.debug('this swap hides somewhere maker and taker '
                              'coins/amount --> {}'.format(swap['uuid']))
        return {
                'maker_coin' : 'None',
                'taker_coin' : 'None',
                'count' : 0
                }


    @measure
    def parse_uuid(self, swap : dict):
        """ Trying to get Finished event timestamp first """
        try:
            return {
                    'uuid' : swap['uuid'],
                    'timestamp' : swap['events'][-1:]['timestamp'],
                    'found_match' : False
                    }
        except KeyError:
            start_event_data = swap['events'][0]['event']['data']
            return {
                    'uuid' : start_event_data['uuid'],
                    'timestamp' : start_event_data['started_at'],
                    'found_match' : False
                    }


    @measure
    def parse_swap_type(self, swap : dict):
        try:
            return swap['type']
        except KeyError:
            return 'Taker'


    # PYMONGO INPUT FUNCTIONS
    @measure
    def insert_traiding_pair_into_collection(self, swap : dict):
        logging.debug('parsing of traiding pair: STARTED')
        data = self.parse_traiding_pair(swap)

        logging.debug('insertion into DB: STARTED')
        result = self.pairs.find_one_and_update(data,
                                                {
                                                    '$inc': {'count': 1 },
                                                    '$setOnInsert' : { 'count' : 1 }
                                                },
                                                upsert=True,
                                                return_document=ReturnDocument.AFTER
                                                )

        logging.debug('New entry --> {}'.format(result))
        if not result:
            logging.debug('something wrong with insertion --> {}'.format(data))
        return data


    def insert_into_uuid_collection(self, swap : dict):
        logging.debug('parsing of uuid and timestamp: STARTED')
        data = self.parse_uuid(swap)
        pass

    def insert_swap_into_db(self, swap):
        #is_swap_successful(swap)
        pass

    def loop_through_all_swaps_in_directory(self):
        pass

"""
#@measure('parse_maker_swaps/')
def parse_swaps_directory(is_db_fresh : bool, ):
    for swap_file in maker_files_list:
        data = parse_swap(maker_folder_path + swap_file)
        if is_fresh_db:
            db["swaps"].insert(data)
        # TODO: can speedup this process if keep files list from previous run and make update requests only for new
        else:
            db["swaps"].update({"uuid": data["uuid"]}, data, upsert=True)
# TODO: probably have to combine maker/taker data for uuid into single document (however not sure if it needed)


for file in taker_files_list:
    with open(taker_folder_path + file) as f:
        data = json.load(f)
        if is_fresh_db:
            db["swaps"].insert(data)
        else:
            db["swaps"].update({"uuid": data["uuid"]}, data, upsert=True)

if is_fresh_db:
    resp = db["swaps"].create_index([ ("uuid", 1) ])
"""
