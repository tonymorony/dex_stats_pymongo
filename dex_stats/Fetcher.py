import sys
import json
import logging
from itertools import permutations
from datetime import timedelta
from datetime import datetime

from decimal import *

from requests.exceptions import ConnectionError

from MongoAPI import MongoAPI
from utils.adex_tickers import adex_tickers
from utils.adex_calls import get_orderbook
from utils.utils import prettify_orders
from utils.utils import enforce_float
from utils.utils import sort_orders
from utils.utils import measure
from utils import adex_calls



class Fetcher:
    def __init__(self):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        self.mongo = MongoAPI()

        # endpoint data variables
        self.summary = []
        self.ticker = {}
        self.orderbook = {}
        self.trades = {}


        self.possible_pairs = list(["{}_{}".format(perm[0], perm[1])
                                    for perm
                                    in permutations(adex_tickers, 2)])
        self.pairs = self.mongo.get_trading_pairs()
        self.null_pairs = [ x
                            for x
                            in self.possible_pairs
                            if x not in self.pairs ]

    @measure
    def pipeline(self):
        for pair in self.pairs:
            self.fetch_data_for_existing_pair(pair)

        for pair in self.null_pairs:
            self.fetch_data_for_null_pair(pair)

        self.save_orderbook_data_as_json()
        self.save_summary_data_as_json()
        self.save_ticker_data_as_json()
        self.save_trades_data_as_json()

    def fetch_data_for_existing_pair(self, pair):
        trading_pairs = pair.split("_")
        base_currency = trading_pairs[0]
        quote_currency = trading_pairs[1]
        self.trades[pair] = []

        last_price = Decimal(0)
        lowest_ask = Decimal(0)
        highest_bid = Decimal(0)
        base_volume = Decimal(0)
        quote_volume = Decimal(0)
        last_trade_time = Decimal(0)

        price_change_24h = Decimal(0)
        highest_price_24h = Decimal(0)
        lowest_price_24h = Decimal(0)
        swap_prices = list()

        mm_orderbook = self.fetch_mm2_orderbook(base_currency, quote_currency)
        asks, lowest_ask, bids, highest_bid = self.parse_orderbook(mm_orderbook)

        timestamp_right_now = int(datetime.now().strftime("%s"))
        timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
        swaps_last_24h = self.mongo.find_swaps_for_market_since_timestamp(base_currency,
                                                                          quote_currency,
                                                                          timestamp_24h_ago)

        # TODO: figure this one out as well...
        # to make sure swaps are in the ascending order
        # swaps_last_24h = sorted( swaps_last_24h,
        #                          key=lambda q: q["events"][0]["timestamp"] )

        for swap in swaps_last_24h:
            first_event = swap["events"][0]["event"]["data"]

            swap_price = (
                    Decimal(first_event["taker_amount"])
                    /
                    Decimal(first_event["maker_amount"])
            )
            swap_prices.append(swap_price)

            base_volume += Decimal(first_event["maker_amount"])
            quote_volume += Decimal(first_event["taker_amount"])
            swap_timestamp = swap["events"][0]["timestamp"] // 1000

            if swap_timestamp > last_trade_time:
                last_trade_time = swap_timestamp

            try:
                lowest_price_24h = min(swap_prices)
            except ValueError:
                lowest_price_24h = Decimal(0)

            try:
                highest_price_24h = max(swap_prices)
            except ValueError:
                highest_price_24h = Decimal(0)

            price_start_24h = (swap_prices[0]
                               if swap_prices
                               else Decimal(0))
            last_price = (swap_prices[-1]
                          if swap_prices
                          else Decimal(0))

            price_change_24h = ((
                                        last_price
                                        -
                                        price_start_24h
                                ) /
                                Decimal(100)
                                )

            # TRADES CALL
            self.trades[pair].append({
                "trade_id": first_event['uuid'],
                "price": enforce_float(swap_price),
                "base_volume": enforce_float(base_volume),
                "quote_volume": enforce_float(quote_volume),
                "timestamp": "{}".format(first_event['started_at']),
                "type": "buy"
            })

        # SUMMARY CALL
        self.summary.append({
            "trading_pairs": pair,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "last_price": enforce_float(last_price),
            "lowest_ask": enforce_float(lowest_ask),
            "highest_bid": enforce_float(highest_bid),
            "base_volume_24h": enforce_float(base_volume),
            "quote_volume_24h": enforce_float(quote_volume),
            "price_change_percent_24h": enforce_float(price_change_24h),
            "highest_price_24h": enforce_float(highest_price_24h),
            "lowest_price_24h": enforce_float(lowest_price_24h),
            "last_trade_time": str(last_trade_time)
        })

        # TICKER CALL
        self.ticker[pair] = {
            "last_price": enforce_float(last_price),
            "quote_volume": enforce_float(quote_volume),
            "base_volume": enforce_float(base_volume),
            "isFrozen": "0"
        }

        # ORDERBOOK CALL
        self.orderbook[pair] = {
                            "timestamp" : "{}".format(timestamp_right_now),
                                 "bids" : prettify_orders(sort_orders(bids)),
                                 "asks" : prettify_orders(sort_orders(asks, 
                                                                      reverse=True))
        }

    def fetch_data_for_null_pair(self, pair):
        trading_pairs = pair.split("_")
        base_currency = trading_pairs[0]
        quote_currency = trading_pairs[1]
        timestamp_right_now = int(datetime.now().strftime("%s"))

        self.summary.append({
            "trading_pairs": pair,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "last_price": enforce_float(Decimal(0)),
            "lowest_ask": enforce_float(Decimal(0)),
            "highest_bid": enforce_float(Decimal(0)),
            "base_volume": enforce_float(Decimal(0)),
            "quote_volume": enforce_float(Decimal(0)),
            "price_change_percent_24h": enforce_float(Decimal(0)),
            "highest_price_24h": enforce_float(Decimal(0)),
            "lowest_price_24h": enforce_float(Decimal(0))
        })

        self.trades[pair] = []
        self.ticker[pair] = {
            "last_price": enforce_float(Decimal(0)),
            "quote_volume": enforce_float(Decimal(0)),
            "base_volume": enforce_float(Decimal(0)),
            "isFrozen": "0"
        }
        self.orderbook[pair] = {
            "timestamp": timestamp_right_now,
            "bids": [],
            "asks": []
        }


    def parse_orderbook(self, orderbook):
        try:
            asks = [[float(ask['price']), float(ask['maxvolume'])]
                    for ask
                    in orderbook["asks"]]
        except (KeyError, ValueError):
            asks = []

        try:
            bids = [[float(bid['price']), float(bid['maxvolume'])]
                    for bid
                    in orderbook["bids"]]
        except (KeyError, ValueError):
            bids = []

        try:
            lowest_ask = min([float(ask['price'])
                              for ask
                              in orderbook["asks"]])
        except (KeyError, ValueError):
            lowest_ask = Decimal(0)

        try:
            highest_bid = max([float(bid['price'])
                               for bid
                               in orderbook["bids"]])
        except (KeyError, ValueError):
            highest_bid = Decimal(0)

        return asks, lowest_ask, bids, highest_bid



    def fetch_mm2_orderbook(self, base_currency, quote_currency):
        mm2_localhost = "http://127.0.0.1:7783"
        mm2_username  = "testuser"
        return get_orderbook(mm2_localhost,
                             mm2_username,
                             base_currency,
                             quote_currency)


    #TODO: create mongo db collections for this, 
    #      serving json files is probably! not very good

    def save_orderbook_data_as_json(self):
        with open('../data/orderbook.json', 'w') as f:
            json.dump(self.orderbook, f)

    def save_ticker_data_as_json(self):
        with open('../data/ticker.json', 'w') as f:
            json.dump(self.ticker, f)

    def save_summary_data_as_json(self):
        with open('../data/summary.json', 'w') as f:
            json.dump(self.summary, f)

    def save_trades_data_as_json(self):
        with open('../data/trades.json', 'w') as f:
            json.dump(self.trades, f)

    # DATA VALIDATION
    def validate_by_amount(self):
        count_true = 0
        total_count = len(self.trading_pairs) - 1
        for pair, amount_in_pairs_collection in self.trading_pairs.items():
            base = pair.split("_")[0]
            quote = pair.split("_")[1]
            swaps = self.mongo.find_swaps_for_market(base, quote)
            swaps_amount_in_db = len(swaps)
            decision = False
            if swaps_amount_in_db == amount_in_pairs_collection:
                decision = True
                count_true += 1
            logging.debug("\n\tIn trading_pairs: {} --> {}\
                        \nIn successful collection: {}\
                                    \n\t\tDecision: {}\n".format(pair, amount_in_pairs_collection,
                                                                 swaps_amount_in_db,
                                                                 decision))
        logging.debug("BY AMOUNT: out of {} pairs {} validated as true".format(total_count,
                                                                               count_true))


if __name__ == "__main__":
    f = Fetcher()
    f.pipeline()
