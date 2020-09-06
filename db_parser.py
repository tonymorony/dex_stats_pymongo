#!/usr/bin/env python3
# from gevent import monkey
# _ = monkey.patch_all()


from utils.swap_events import (maker_swap_success_events,
                               taker_swap_success_events,
                               maker_swap_error_events,
                               taker_swap_error_events)

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


class DB_Parser():
    def __init__(self,                parsed_files=True,
                 async_mode=False,    parse_pairs=True,
                 parse_maker=True,    parse_taker=True,
                 save_failed=True,    save_successful=True,
                 data_analysis=False, use_swap_events=True,
                 mongo_port=27017,    mongo_ip='localhost',
                 swaps_folder_path="../SWAPS/STATS/MAKER/"):
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
        self.async_mode  = async_mode
        self.save_failed = save_failed
        self.parse_pairs = parse_pairs
        self.parsed_files = parsed_files
        self.data_analysis   = data_analysis
        self.use_swap_events = use_swap_events
        self.save_successful = save_successful
        self.maker_folder_path = swaps_folder_path
        self.taker_folder_path = swaps_folder_path[:-6] + 'TAKER/'

        #init mongo client connection and create swaps db
        self.client = MongoClient('mongodb://{}:{}/'.format(mongo_ip, mongo_port))
        self.swaps = self.client.swaps

        self.is_fresh_run = not bool(self.swaps.list_collection_names())
        self.parsed_files_list = []

        #creating main successful/failed collections
        if self.save_failed:
            self.failed = self.swaps.failed_swaps_collection

        if self.save_successful:
            self.successful = self.swaps.successsful_swaps_collection

        if self.parsed_files:
            self.parsed = self.swaps.parsed_files


        '''
        #enabling collection of top swap pairs
        if self.parse_pairs:
            self.pairs = self.swaps.top_pairs_collection

        #enabling collection of uuids with timestamp
        

        #enabling extensive collection of swap info for data analysis
        if self.data_analysis:
            self.data = self.swaps.data_analysis_collection
        '''

        #enabling swap events for validation of successful/failed swaps
        if self.use_swap_events:
            self.maker_swap_success_events = maker_swap_success_events
            self.taker_swap_success_events = taker_swap_success_events
            self.maker_swap_error_events = maker_swap_error_events
            self.taker_swap_error_events = taker_swap_error_events


    def __str__(self):
        config = ('parser-conf:'
                 '\n  async={}'
                 '\n  data_analysis={}').format(self.async_mode,
                                                self.data_analysis)
        return config

    
    #Generator test
    #@measure
    def create_maker_files_pool_with_abs_path(self):
        for dirpath,_,filenames in os.walk(self.maker_folder_path):
            for filename in filenames:
                if filename.endswith('.json'):
                    yield os.path.abspath(os.path.join(dirpath, filename)), filename


    @measure
    def create_maker_files_pool(self):
        self.maker_files_pool = [ x
                                  for x
                                  in os.listdir(self.maker_folder_path)
                                  if x.endswith('.json') ]


    def create_taker_files_pool(self):
        self.taker_files_pool = [ x
                                  for x
                                  in os.listdir(self.taker_folder_path)
                                  if x.endswith('.json') ]


    #TODO: parse uuids, if there are new uuids, update, else do nothing
    #      also could be good to save last timestamp or check only for
    #      swaps that happen/created in the last 24h
    '''
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
                                  in os.listdir(self.maker_folder_path)
                                  if x.endswith('.json') ]
    

    def check_connection_to_mongo(self):
        return self.swaps
    '''


    ### SWAP FILES PARSING FUNCTIONS

    def parse_swap_data(self, path_to_swap_json : str):
        """Parses json from filepath and (returns : dict)"""
        with open(path_to_swap_json) as f:
            return json.load(f)


    def parse_swap_events(self, swap : dict):
        """Parses (swap : dict) and (returns : list) of swap events."""
        return [ x['event']['type'] for x in swap['events'] ]


    def is_swap_finished(self, swap_events : list):
        """Checks if (swap_events : list) has Finished event (returns : bool). """
        return True if 'Finished' in swap_events else False


    def is_swap_successful(self, swap_events : list):
        """Checks if swap has Finished successfully, (returns : bool). """
        return True if (swap_events == self.maker_swap_success_events or
                        swap_events == self.taker_swap_success_events) else False


    def is_duplicate(self, swap_file : str):
        return True if swap_file in self.parsed_files else False


    def is_maker(self, swap_file_abspath : str):
        return True if 'MAKER' in swap_file_abspath else False


    '''
    def parse_traiding_pair(self, swap : dict):
        try:
            return {
                    'maker_coin' : swap['maker_coin'],
                    'taker_coin' : swap['taker_coin'],
                    'count' : 1
                    }
        except KeyError:
            try:
                start_event_data = swap['events'][0]['event']['data']
                return {
                        'maker_coin' : start_event_data['maker_coin'],
                        'taker_coin' : start_event_data['taker_coin'],
                        'count' : 1
                        }
            except KeyError:
                pass
        return {
                'maker_coin' : 'None',
                'taker_coin' : 'None',
                'count' : 1
                }


    def parse_uuid(self, swap : dict):
        """ Trying to get Started event timestamp first """
        try:
            return {
                    'uuid' : swap['uuid'],
                    'timestamp' : swap['events'][0]['timestamp'],
                    'found_match' : False
                    }
        except KeyError:
            start_event_data = swap['events'][0]['event']['data']
            return {
                    'uuid' : start_event_data['uuid'],
                    'timestamp' : start_event_data['started_at'],
                    'found_match' : False
                    }


    def parse_swap_type(self, swap : dict):
        try:
            return swap['type']
        except KeyError:
            return 'Taker'
    '''

    # PYMONGO INPUT FUNCTIONS
    '''
    #@measure
    def insert_into_traiding_pair_collection(self, swap : dict):
        logging.debug('parsing of traiding pair: STARTED')
        data = self.parse_traiding_pair(swap)

        logging.debug('insertion into top_pairs_collection: STARTED')
        result = self.pairs.find_one_and_update(data,
                                                {
                                                    '$inc' : { 'count': 1 }
                                                },
                                                upsert=True
                                                #return_document=ReturnDocument.AFTER
                                                )
        logging.debug('New entry --> {}'.format(result))
        #return data #not sure if we need this yet...


    #@measure
    def insert_into_uuid_collection(self, swap : dict):
        logging.debug('parsing of uuid and timestamp: STARTED')
        data = self.parse_uuid(swap)

        logging.debug('insertion into uuids_with_timestamp_collection: STARTED')
        if self.is_fresh_run:
            result = self.uuids.insert(data)
        else:
            result = self.uuids.find_one_and_update({"uuid": data["uuid"]},
                                                    {
                                                        '$set' : { 'found_match' : True }
                                                    },
                                                    upsert=True)
        logging.debug('New entry --> {}'.format(result))
        if not result:
            logging.debug('something wrong with insertion --> {}'.format(data))
        #return data #not sure if we need this yet...
    '''
    @measure
    def insert_into_parsed_files_collection(self):
        self.parsed.insert({'data' : self.parsed_files_list})


    @measure
    def insert_into_swap_collection(self, swap_file : str) -> bool:
        logging.debug('\n\nInsertion into collection:'
                      '\n  reading swap file {}'.format(swap_file))
        raw_swap_data = self.parse_swap_data(swap_file)
        swap_events = self.parse_swap_events(raw_swap_data)

        swap_successful = self.is_swap_successful(swap_events)
        logging.debug('Checking if swap was successful ----> {}'.format(swap_successful))
        
        #commented out for now since takes too much time
        #self.insert_into_traiding_pair_collection(raw_swap_data)
        #self.insert_into_uuid_collection(raw_swap_data)

        if swap_successful:
            if self.is_swap_finished(swap_events):
                self.successful.insert(raw_swap_data)
            else:
                logging.debug('Insertion into collection: ABORTED - Ongoing swap')
                return False
        else:
            self.failed.insert(raw_swap_data)


        logging.debug('Insertion into collection: DONE')
        return True
        '''
        elif not self.is_fresh_run and is_swap_successful:
            self.successful.update({"uuid": raw_swap_data["uuid"]}, raw_swap_data, upsert=True)
        else:
            self.failed.update({"uuid": raw_swap_data["uuid"]}, raw_swap_data, upsert=True)
        
        '''

    @measure
    def create_mongo_collections(self):
        '''
        self.create_maker_files_pool()
        maker_files_pool = self.maker_files_pool
        self.create_taker_files_pool()
        taker_files_pool = self.taker_files_pool
        matched_maker_swaps = []

        for maker_file in maker_files_pool:
            if maker_file in taker_files_pool:
                matched_maker_swaps.append(maker_files_pool.pop())
        '''
        
        swap_files_pool = self.create_maker_files_pool_with_abs_path()

        for swap_file_abspath, swap_file_name in swap_files_pool:
            self.parsed_files_list.append(swap_file_name)
            self.insert_into_swap_collection(swap_file_abspath)
        
        self.insert_into_parsed_files_collection()

        resp = self.successful.create_index([ ("uuid", 1) ])
        logging.debug('creating index for successful collection --> {}'.format(resp))


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
