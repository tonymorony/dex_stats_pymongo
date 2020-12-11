import json
import logging
import os
import sys

from pymongo import MongoClient
from utils.adex_tickers import adex_tickers
from utils.swap_events import (taker_swap_error_events,
                               maker_swap_error_events,
                               taker_swap_success_events,
                               maker_swap_success_events)
from utils.utils import measure


class Parser():
    def __init__(self, async_mode=False,
                 data_analysis=False, use_swap_events=True,
                 swaps_folder_path="/home/shutdowner/seed_db/2bf44966eba9c6da8a888833dec412618997949d/SWAPS/STATS/MAKER"):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        # parser config
        self.adex_tickers = adex_tickers
        self.async_mode = async_mode
        self.data_analysis = data_analysis
        self.use_swap_events = use_swap_events
        self.maker_folder_path = swaps_folder_path
        self.taker_folder_path = swaps_folder_path[:-6] + 'TAKER/'

        # init mongo client connection and create swaps database
        self.client = MongoClient('mongodb://localhost:27017/')
        self.swaps = self.client.swaps

        # creating main collections for successful/failed swaps
        self.failed = self.swaps.failed
        self.successful = self.swaps.successful

        # creating utility collections
        self.parsed = self.swaps.parsed_files
        self.pairs = self.swaps.trading_pairs
        self.validate_uuid = 0
        self.validate_pairs = 0
        self.is_fresh_run = not bool(self.swaps.list_collection_names())
        if self.is_fresh_run:
            self.parsed_files = []
            self.unique_pairs = {}
        else:
            self.parsed_files = list(self.parsed.find({'data': {
                '$exists': 'true',
                '$ne': []
            }
            },
                {'data': 1})  # projection
            )
            self.unique_pairs = dict(self.pairs.find({'data': {
                '$exists': 'true',
                '$ne': {}
            }
            },
                {'data': 1})
            )

        # enabling swap events for validation of successful/failed swaps
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

    # UTILITIES
    def create_maker_files_pool_with_abs_path(self):
        for dirpath, _, filenames in os.walk(self.maker_folder_path):
            for filename in filenames:
                if filename.endswith('.json'):
                    yield os.path.abspath(os.path.join(dirpath, filename)), filename

    def clean_up(self):
        self.swaps.drop_collection("successful")
        self.swaps.drop_collection("failed")
        self.swaps.drop_collection("trading_pairs")
        self.swaps.drop_collection("parsed_files")

    ### SWAP VALIDATION
    def is_swap_finished(self, swap_events: list) -> bool:
        """Checks if (swap_events : list) has Finished event (returns : bool). """
        return True if 'Finished' in swap_events else False

    def is_swap_successful(self, swap_events: list) -> bool:
        """Checks if swap has Finished successfully, (returns : bool). """
        return True if (swap_events == self.maker_swap_success_events or
                        swap_events == self.taker_swap_success_events) else False

    def is_duplicate(self, swap_file: str) -> bool:
        return True if swap_file in self.parsed_files else False

    def is_maker(self, swap_file_abspath: str) -> bool:
        return True if 'MAKER' in swap_file_abspath else False

    ### SWAP PARSING
    def parse_swap_data(self, path_to_swap_json: str) -> dict:
        """Parses json from filepath and (returns : dict)"""
        with open(path_to_swap_json, 'r') as f:
            return json.load(f)

    def parse_swap_events(self, swap: dict) -> list:
        """Parses (swap : dict) and (returns : list) of swap events."""
        return [x['event']['type'] for x in swap['events']]

    def parse_traiding_pair(self, swap: dict):
        try:
            pair = "{}_{}".format(swap['maker_coin'],
                                  swap['taker_coin'])
        except KeyError:
            try:
                start_event = swap['events'][0]['event']['data']
                pair = "{}_{}".format(start_event['maker_coin'],
                                      start_event['taker_coin'])
            except KeyError:
                # there are around 1000 of those among all swaps,
                # probably old mm json format
                pair = "{}_{}".format('None', 'None')
                self.validate_pairs += 1
        return pair

    def parse_uuid(self, swap: dict):
        """ Trying to get Started event timestamp first """
        try:
            return swap['uuid']
        except KeyError:
            try:
                start_event_data = swap['events'][0]['event']['data']
                return start_event_data['uuid']
            except KeyError:
                self.validate_uuid += 1
                return 'None'

    def add_trading_pair(self, pair):
        if pair in self.unique_pairs:
            self.unique_pairs[pair] += 1
        else:
            self.unique_pairs[pair] = 1

    ### PYMONGO INPUT
    @measure
    def insert_into_parsed_files_collection(self):
        self.parsed.update_one({'data': {
            '$exists': 'true',
            '$ne': []
        }
        },
            {'$addToSet': {'data': {'$each': self.parsed_files}}},
            upsert=True)
        logging.debug('Input to parser files collection: DONE')

    @measure
    def insert_into_unique_pairs_collection(self):
        self.pairs.insert_one({'data': self.unique_pairs})
        logging.debug('Input to unique pairs collection: DONE')

    @measure
    def insert_into_swap_collection(self, swap_file: str):
        logging.debug('\n\nInsertion into collection:'
                      '\n  reading swap file {}'.format(swap_file))

        raw_swap_data = self.parse_swap_data(swap_file)
        swap_events = self.parse_swap_events(raw_swap_data)
        uuid = self.parse_uuid(raw_swap_data)

        # DATA VALIDATION
        # probably need to move this to validate_swap()
        # exit if:
        #   file with this uuid was previously parsed
        if uuid in self.parsed_files:
            return
        #   swap is unfinished
        if not self.is_swap_finished(swap_events):
            return
        #   tickers are not in adex_tickers
        pair = self.parse_traiding_pair(raw_swap_data)
        tickers = pair.split('_')
        if tickers[0] not in self.adex_tickers:
            return
        if tickers[1] not in self.adex_tickers:
            return
        #   swap is unsuccessful
        swap_successful = self.is_swap_successful(swap_events)
        if not swap_successful:
            return

        self.successful.insert_one(raw_swap_data)
        logging.debug('Inserting into (((successful))) collection: DONE')
        self.parsed_files.append(uuid)
        self.add_trading_pair(pair)

    def create_index_uuid(self):
        resp = self.successful.create_index([("uuid", 1)])
        logging.debug('Created index for successful collection --> {}'.format(resp))

    ### DEBUG
    @measure
    def create_mongo_collections(self):
        swap_files_pool = self.create_maker_files_pool_with_abs_path()

        for swap_file_abspath, swap_file_name in swap_files_pool:
            self.insert_into_swap_collection(swap_file_abspath)

        self.insert_into_parsed_files_collection()
        self.insert_into_unique_pairs_collection()
        self.create_index_uuid()
        logging.debug('total uuids with None: {}'.format(self.validate_uuid))
        logging.debug('total pairs with None: {}'.format(self.validate_uuid))


if __name__ == "__main__":
    p = Parser()
    p.create_mongo_collections()
