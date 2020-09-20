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
    def __init__(self,                
                 async_mode=False,    parse_pairs=True,
                 save_failed=True,    save_successful=True,
                 data_analysis=False, use_swap_events=True,
                 mongo_port=27017,    mongo_ip='localhost',
                 swaps_folder_path="../../dex_stats_pymongo-data/STATS/MAKER/"):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        #raise if both are False
        if not save_failed and not save_successful:
            raise ArgumentInputParserError('save_failed=False, save_successful=False,',
                  'you cant have both save_failed and save_successful as False at the'
                  ' same time. Parser has nothing to do...')

        # Parser config
        self.async_mode   = async_mode
        self.save_failed  = save_failed
        self.parse_pairs  = parse_pairs
        self.data_analysis   = data_analysis
        self.use_swap_events = use_swap_events
        self.save_successful = save_successful
        self.maker_folder_path = swaps_folder_path
        self.taker_folder_path = swaps_folder_path[:-6] + 'TAKER/'

        #init mongo client connection and create swaps db
        self.client = MongoClient('mongodb://{}:{}/'.format(mongo_ip, mongo_port))
        self.swaps = self.client.swaps

        self.parsed = self.swaps.parsed_files

        self.is_fresh_run = not bool(self.swaps.list_collection_names())
        if self.is_fresh_run:
            self.parsed_files_list = []
        else:
            self.parsed_files_list = list(self.parsed.find({'data': {
                                                                     '$exists': 'true',
                                                                     '$ne': []
                                                                    }
                                                            }))[0]['data']

        #creating collections for successful/failed swaps
        if self.save_failed:
            self.failed = self.swaps.failed_swaps_collection

        if self.save_successful:
            self.successful = self.swaps.successful

        #enabling swap events for validation of successful/failed swaps
        if self.use_swap_events:
            self.maker_swap_success_events = maker_swap_success_events
            self.taker_swap_success_events = taker_swap_success_events
            self.maker_swap_error_events = maker_swap_error_events
            self.taker_swap_error_events = taker_swap_error_events
        
        '''
        #enabling collection of top swap pairs
        if self.parse_pairs:
            self.pairs = self.swaps.top_pairs_collection
        '''


    def __str__(self):
        config = ('parser-conf:'
                 '\n  async={}'
                 '\n  data_analysis={}').format(self.async_mode,
                                                self.data_analysis)
        return config

    
    def create_maker_files_pool_with_abs_path(self):
        for dirpath,_,filenames in os.walk(self.maker_folder_path):
            for filename in filenames:
                if filename.endswith('.json'):
                    yield os.path.abspath(os.path.join(dirpath, filename)), filename


    ### SWAP FILES PARSING FUNCTIONS
    def parse_swap_data(self, path_to_swap_json : str) -> dict:
        """Parses json from filepath and (returns : dict)"""
        with open(path_to_swap_json) as f:
            return json.load(f)


    def parse_swap_events(self, swap : dict) -> list:
        """Parses (swap : dict) and (returns : list) of swap events."""
        return [ x['event']['type'] for x in swap['events'] ]


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


    # TODO: implement parsing of unique trading pairs
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
    '''

    # TODO: do it only once on startup -> save to file on shutdown
    def parse_unique_pairs():
        # TODO: lookup for local file with pairs
        db_con = MongoAPI()
        key = 'maker_coin'
        pairs = []
        maker_options = db_con.swaps_collection.distinct(key)
        for maker_coin in maker_options:
            if maker_coin:  # prevents NoneType calls
                swaps = list(db_con.swaps_collection.find({"maker_coin": maker_coin}))
                for swap in swaps:
                    taker_coin = swap.get('taker_coin')
                    pair = (maker_coin, taker_coin)
                    pairs.append(pair)
        return set(pairs)

    # PYMONGO INPUT FUNCTIONS
    @measure
    def insert_into_parsed_files_collection(self):
        self.parsed.update_one({'data': {'$exists': 'true', '$ne': []}},
                               {'$addToSet': {'data': {'$each': self.parsed_files_list}}}, 
                               upsert=True)


    @measure
    def insert_into_swap_collection(self, swap_file : str) -> bool:
        logging.debug('\n\nInsertion into collection:'
                      '\n  reading swap file {}'.format(swap_file))
        raw_swap_data = self.parse_swap_data(swap_file)
        swap_events = self.parse_swap_events(raw_swap_data)

        swap_successful = self.is_swap_successful(swap_events)
        logging.debug('Checking if swap was successful ----> {}'.format(swap_successful))
        
        if not (raw_swap_data.get('uuid') in self.parsed_files_list):
            if swap_successful:
                if self.is_swap_finished(swap_events):
                    self.successful.insert(raw_swap_data)
                    self.parsed_files_list.append(raw_swap_data.get('uuid'))
                    logging.debug('Insertion into collection: DONE')
                    return True
                else:
                    logging.debug('Insertion into collection: ABORTED - Ongoing swap')
                    return False
            else:
                self.failed.insert(raw_swap_data)
                self.parsed_files_list.append(raw_swap_data.get('uuid'))
                logging.debug('Insertion into collection: DONE')
                return True
        logging.debug('Insertion into collection: ABORTED -- Existing swap')
        return False


    @measure
    def create_mongo_collections(self):
        swap_files_pool = self.create_maker_files_pool_with_abs_path()

        for swap_file_abspath, swap_file_name in swap_files_pool:
            self.parsed_files_list.append(swap_file_name)
            self.insert_into_swap_collection(swap_file_abspath)
        
        self.insert_into_parsed_files_collection()

        resp = self.successful.create_index([ ("uuid", 1) ])
        logging.debug('creating index for successful collection --> {}'.format(resp))



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
    '''
