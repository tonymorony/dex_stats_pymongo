import os
import sys
import json
import logging

from utils.utils import measure
from pymongo import MongoClient, ReturnDocument
from ParserError import ArgumentInputParserError
from utils.swap_events import (taker_swap_error_events,
                               maker_swap_error_events,
                               taker_swap_success_events,
                               maker_swap_success_events)



class Parser():
    def __init__(self,                async_mode=False,
                 data_analysis=False, use_swap_events=True,
                 mongo_port=27017,    mongo_ip='localhost',
                 swaps_folder_path="../../dex_stats-data/STATS/MAKER/"):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        #raise if both are False
        if not save_failed and not save_successful:
            raise ArgumentInputParserError('save_failed=False, save_successful=False,',
                  'you cant have both save_failed and save_successful as False at the'
                  ' same time. Parser has nothing to do...')

        #parser config
        self.async_mode        = async_mode
        self.data_analysis     = data_analysis
        self.use_swap_events   = use_swap_events
        self.maker_folder_path = swaps_folder_path
        self.taker_folder_path = swaps_folder_path[:-6] + 'TAKER/'

        #init mongo client connection and create swaps database
        self.client = MongoClient('mongodb://{}:{}/'.format(mongo_ip, mongo_port))
        self.swaps  = self.client.swaps

        #creating main collections for successful/failed swaps
        self.failed     = self.swaps.failed
        self.successful = self.swaps.successful

        #creating utility collections
        self.parsed = self.swaps.parsed_files
        self.pairs  = self.swaps.unique_pairs
        
        self.is_fresh_run = not bool(self.swaps.list_collection_names())
        if self.is_fresh_run:
            self.parsed_files = []
            self.unique_pairs = []
        else:
            self.parsed_files = list(self.parsed.find({'data': {
                                                                '$exists': 'true',
                                                                '$ne': []
                                                               }
                                                     })
                                    )[0]['data']
            self.unique_pairs = dict(self.pairs.find({'data': {
                                                               '$exists': 'true',
                                                               '$ne': {}
                                                              }
                                                    })
                                    )['data']

        #enabling swap events for validation of successful/failed swaps
        if self.use_swap_events:
            self.maker_swap_success_events = maker_swap_success_events
            self.taker_swap_success_events = taker_swap_success_events
            self.maker_swap_error_events   = maker_swap_error_events
            self.taker_swap_error_events   = taker_swap_error_events


    def __str__(self):
        config = ('parser-conf:'
                 '\n  async={}'
                 '\n  data_analysis={}').format(self.async_mode,
                                                self.data_analysis)
        return config


    def create_maker_files_pool_with_abs_path(self):
        for dirpath ,_, filenames in os.walk(self.maker_folder_path):
            for filename in filenames:
                if filename.endswith('.json'):
                    yield os.path.abspath(os.path.join(dirpath, filename)), filename


    ### SWAP FILE PARSING
    def parse_swap_data(self, path_to_swap_json : str) -> dict:
        """Parses json from filepath and (returns : dict)"""
        with open(path_to_swap_json) as f:
            return json.load(f)


    def parse_swap_events(self, swap : dict) -> list:
        """Parses (swap : dict) and (returns : list) of swap events."""
        return [ x['event']['type'] for x in swap['events'] ]


    def parse_traiding_pair(self, swap : dict) -> dict:
        try:
            pair = "{}_{}".format(swap['maker_coin'], swap['taker_coin'])
        except KeyError:
            try:
                start_event = swap['events'][0]['event']['data']
                pair = "{}_{}".format(start_event['maker_coin'], 
                                      start_event['taker_coin'])
            except KeyError:
                pair = "{}_{}".format('None', 'None')

        if pair in self.unique_pairs:
            self.unique_pairs[pair] += 1
        else:
            self.unique_pairs[pair]  = 1


    ### SWAP VALIDATION
    def is_swap_finished(self, swap_events : list) -> bool:
        """Checks if (swap_events : list) has Finished event (returns : bool). """
        return True if 'Finished' in swap_events else False


    def is_swap_successful(self, swap_events : list) -> bool:
        """Checks if swap has Finished successfully, (returns : bool). """
        return True if (swap_events == self.maker_swap_success_events or
                        swap_events == self.taker_swap_success_events) else False


    def is_duplicate(self, swap_file : str) -> bool:
        return True if swap_file in self.parsed_files else False


    def is_maker(self, swap_file_abspath : str) -> bool:
        return True if 'MAKER' in swap_file_abspath else False


    ### PYMONGO INPUT
    @measure
    def insert_into_parsed_files_collection(self):
        self.parsed.update_one({'data': {
                                         '$exists': 'true',
                                         '$ne': []
                                        }
                               },
                               {'$addToSet': {'data': {'$each': self.parsed_files_list}}}, 
                               upsert=True)


    @measure
    def insert_into_unique_pairs_collection(self, pair):
        #TODO: if pair exists then do d[pair] + 1 else upsert
        self.pairs.update_one({'data': {
                                        '$exists': 'true',
                                        '$ne': {}
                                       }
                               },
                               {'$addToSet': {'data': {'$each': self.unique_pairs}}}, 
                               upsert=True)


    @measure
    def insert_into_swap_collection(self, swap_file : str):
        logging.debug('\n\nInsertion into collection:'
                      '\n  reading swap file {}'.format(swap_file))
        raw_swap_data = self.parse_swap_data(swap_file)
        swap_events   = self.parse_swap_events(raw_swap_data)

        #exit if file was previously parsed or swap is unfinished
        if raw_swap_data.get('uuid') in self.parsed_files_list:
            return
        if not self.is_swap_finished(swap_events):
            return

        swap_successful = self.is_swap_successful(swap_events)
        logging.debug('Checking if swap was successful ----> {}'.format(swap_successful))

        if swap_successful:
            self.successful.insert(raw_swap_data)
            logging.debug('Inserting into (((successful))) collection: DONE')
        else:
            self.failed.insert(raw_swap_data)
            logging.debug('Inserting into --(((failed)))-- collection: DONE')

        self.parsed_files_list.append(raw_swap_data.get('uuid'))
        self.unique_pairs.append(self.parse_traiding_pair(raw_swap_data))


    def create_index_for_uuid(self):
        resp = self.successful.create_index([ ("uuid", 1) ])
        logging.debug('Created index for successful collection --> {}'.format(resp))


    ### DEBUG
    @measure
    def create_mongo_collections(self):
        swap_files_pool = self.create_maker_files_pool_with_abs_path()

        for swap_file_abspath, swap_file_name in swap_files_pool:
            self.parsed_files_list.append(swap_file_name)
            self.insert_into_swap_collection(swap_file_abspath)
        
        self.insert_into_parsed_files_collection()
