import json
import bson

import logging
import sys
from datetime import datetime, timedelta
from decimal import *
from itertools import permutations

from requests.exceptions import ConnectionError
from utils import adex_calls
from utils.adex_tickers import adex_tickers

from collections import Counter
import operator

from MongoAPI import MongoAPI
from utils.adex_calls import get_orderbook
from utils.adex_tickers import adex_tickers
from utils.utils import enforce_float
from utils.utils import measure
from utils.utils import prettify_orders
from utils.utils import sort_orders
from utils.rpc_lib import def_credentials



class Fetcher:
    def __init__(self):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        self.mongo = MongoAPI()

        # endpoint data variables
        self.summary = []
        self.stress_test_summary = {}
        self.ticker = {}
        self.orderbook = {}
        self.trades = {}
        self.graph_data = {}
        self.graph_data_2 = {}
        self.graph_data_start_timestamp = 0


        self.possible_pairs = list(["{}_{}".format(perm[0], perm[1]) 
                                    for perm
                                    in permutations(adex_tickers, 2)])
        self.stress_test_swaps_data = {}

    @measure
    def pipeline(self):
        # for pair in self.pairs:
        #     self.fetch_data_for_existing_pair(pair)

        # for pair in self.null_pairs:
        #     self.fetch_data_for_null_pair(pair)

        # TODO: set stress test pairs here
        self.fetch_data_for_existing_pair("RICK_MORTY")
        self.fetch_data_for_existing_pair("MORTY_RICK")

        self.save_orderbook_data_as_json()
        self.save_summary_data_as_json()

        # temp dirty trick. have to combine RICK_MORTY and MORTY_RICK into single data set
        stress_test_unique_participants_list = []
        stress_test_unique_participants_count = 0
        stress_test_swap_counter = 0
        stress_test_leaderboard = {}
        with open('/root/dex_stats_pymongo/data/summary.json', 'r') as f:
            data = json.load(f)
            for pair in data:
                stress_test_unique_participants_list += pair["swaps_unique_participants"]
                stress_test_leaderboard = Counter(stress_test_leaderboard) + Counter(pair["swaps_leaderboard"])
                stress_test_swap_counter += pair["swaps_count_total"]
            stress_test_unique_participants_list = list(set(stress_test_unique_participants_list))
            stress_test_unique_participants_count = len(stress_test_unique_participants_list)
            stress_test_leaderboard = dict(sorted(stress_test_leaderboard.items(), key=operator.itemgetter(1),reverse=True))
        # writing into special stress test file
        with open('/root/dex_stats_pymongo/data/stress_test.json', 'w') as f:
            json.dump({
                "stress_test_unique_participants_count": stress_test_unique_participants_count,
                "stress_test_unique_participants_list": stress_test_unique_participants_list,
                "stress_test_leaderboard": stress_test_leaderboard
            }, f)
        self.stress_test_summary["stress_test_unique_participants_count"] = stress_test_unique_participants_count
        self.stress_test_summary["stress_test_total_swaps"] = stress_test_swap_counter
        with open('/root/dex_stats_pymongo/data/stress_test_summary.json', 'w') as f:
            json.dump(self.stress_test_summary, f)
        with open('/root/dex_stats_pymongo/data/stress_test_uuids.json', 'w') as f:
            sorted_stress_test_swaps_data = {}
            for key, value in sorted(self.stress_test_swaps_data.items(), key=lambda x: x[0], reverse=True):
                sorted_stress_test_swaps_data[key] = value
            json.dump(sorted_stress_test_swaps_data, f)
            #json.dump(self.stress_test_swaps_data, f)
        #graph_objects = [{k: v} for k, v in self.graph_data.items()]
        with open('/root/dex_stats_pymongo/data/graph_data.json', 'w') as f:
            json.dump(self.graph_data, f)
        with open('/root/dex_stats_pymongo/data/graph_data_2.json', 'w') as f:
           json.dump(self.graph_data_2, f)
        self.save_ticker_data_as_json()
        self.save_trades_data_as_json()

    # stress_test edition
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

        swap_prices = []
        price_change_24h = Decimal(0)
        highest_price_24h = Decimal(0)
        lowest_price_24h = Decimal(0)
        uniquie_participants = Decimal(0)


        timestamp_right_now = int(datetime.now().strftime("%s"))

        # TODO: set stress test timestamp here
        # 2020 year start for testing now
        stress_test_start = 1606780800
        stress_test_end =   1607700485
        timestamp_1h_ago = int((datetime.now() - timedelta(hours = 1)).strftime("%s"))
        swaps_since_test_start = self.mongo.find_swaps_for_market_since_timestamp(base_currency,
                                                                          quote_currency,
                                                                          stress_test_start)
        swaps_last_hr = self.mongo.find_swaps_for_market_since_timestamp(base_currency,
                                                                          quote_currency,
                                                                          timestamp_1h_ago)
        swaps_count = len(swaps_since_test_start)
        minutes_since_stress_test_start =  (int(datetime.now().timestamp()) - stress_test_start) / 60

        # TODO: figure this one out as well...
        # to make sure swaps are in the ascending order
        # swaps_last_24h = sorted( swaps_last_24h,
        #                         key=lambda q: q["events"][0]["timestamp"] )

        swaps_participants = []
        swaps_leaderboard = {}
        stress_test_swaps_detailed_data = {}

        temp_time_stamp = stress_test_start
        # have to count from same time for RICK_MORTY and MORTY_RICK
        if self.graph_data_start_timestamp == 0:
            self.graph_data_start_timestamp = int(datetime.now().strftime("%s"))
        swaps_counter = 0
        timestamps_list = []
        # data for graph with 10 minutes step
        for swap in swaps_since_test_start:
            timestamps_list.append(swap["events"][0]["timestamp"] // 1000)
        while temp_time_stamp < self.graph_data_start_timestamp:
            temp_time_stamp += 600
            for timestamp in timestamps_list:
                if  timestamp < temp_time_stamp:
                    swaps_counter += 1
                    timestamps_list.remove(timestamp)
            if temp_time_stamp in self.graph_data.keys():
                self.graph_data[temp_time_stamp] += swaps_counter
            else:
                self.graph_data[temp_time_stamp] = swaps_counter

        temp_time_stamp = stress_test_start
        for swap in swaps_since_test_start:
            timestamps_list.append(swap["events"][0]["timestamp"] // 1000)
        while temp_time_stamp < self.graph_data_start_timestamp:
            swaps_counter = 0
            temp_time_stamp += 3600
            for timestamp in timestamps_list:
                if  timestamp < temp_time_stamp:
                    swaps_counter += 1
                    timestamps_list.remove(timestamp)
            if temp_time_stamp in self.graph_data_2.keys():
                self.graph_data_2[temp_time_stamp] += swaps_counter
            else:
                self.graph_data_2[temp_time_stamp] = swaps_counter

        for swap in swaps_since_test_start:

            first_event = swap["events"][0]["event"]["data"]
            # filling detailed info about swap
            stress_test_swaps_detailed_data[swap["events"][0]["timestamp"] // 1000] = {
                "uuid": swap["uuid"],
                "base_coin": trading_pairs[0],
                "base_coin_amount": format(float(first_event["maker_amount"]), ".10f"),
                "rel_coin": trading_pairs[1],
                "rel_coin_amount": format(float(first_event["taker_amount"]), ".10f")
            }
            stress_test_swaps_detailed_data = dict(sorted(stress_test_swaps_detailed_data.items()))

            # adding swap participants addys
            rick_proxy = def_credentials("RICK")
            for event in swap["events"]:
                # case for taker statuses
                if "TakerFeeSent" in swap["success_events"]:
                    # adding taker addy
                    if event["event"]["type"] == "TakerFeeSent":
                        #swaps_participants.append(event["event"]["data"]["from"][0])
                        taker_fee_hex = event["event"]["data"]["tx_hex"]
                        taker_fee_hex_decoded = rick_proxy.decoderawtransaction(taker_fee_hex)
                        taker_addy = taker_fee_hex_decoded["vout"][1]["scriptPubKey"]["addresses"][0]
                        swaps_participants.append(taker_addy)
                        #if event["event"]["data"]["from"][0] in swaps_leaderboard.keys():
                        if taker_addy in swaps_leaderboard.keys():
                            #swaps_leaderboard[event["event"]["data"]["from"][0]] += 1
                            swaps_leaderboard[taker_addy] += 1
                        else:
                            swaps_leaderboard[taker_addy] = 1
                            #swaps_leaderboard[event["event"]["data"]["from"][0]] = 1
                    # adding maker addy
                    if event["event"]["type"] == "MakerPaymentReceived":
                        maker_payment_hex = event["event"]["data"]["tx_hex"]
                        maker_payment_hex_decoded = rick_proxy.decoderawtransaction(maker_payment_hex)
                        maker_addy = maker_payment_hex_decoded["vout"][2]["scriptPubKey"]["addresses"][0]
                        #swaps_participants.append(event["event"]["data"]["from"][0])
                        swaps_participants.append(maker_addy)
                        #if event["event"]["data"]["from"][0] in swaps_leaderboard.keys():
                        if maker_addy in swaps_leaderboard.keys():
                            swaps_leaderboard[maker_addy] += 1
                            #swaps_leaderboard[event["event"]["data"]["from"][0]] += 1
                        else:
                            swaps_leaderboard[maker_addy] = 1
                            #swaps_leaderboard[event["event"]["data"]["from"][0]] = 1
                # case for maker statuses
                elif "TakerFeeValidated" in swap["success_events"]:
                    # adding taker addy
                    if event["event"]["type"] == "TakerFeeValidated":
                        #swaps_participants.append(event["event"]["data"]["from"][0])
                        taker_payment_hex = event["event"]["data"]["tx_hex"]
                        taker_payment_hex_decoded = rick_proxy.decoderawtransaction(taker_payment_hex)
                        taker_addy = taker_payment_hex_decoded["vout"][1]["scriptPubKey"]["addresses"][0]
                        swaps_participants.append(taker_addy)
                        #if event["event"]["data"]["from"][0] in swaps_leaderboard.keys():
                        if taker_addy in swaps_leaderboard.keys():
                            #swaps_leaderboard[event["event"]["data"]["from"][0]] += 1
                            swaps_leaderboard[taker_addy] += 1
                        else:
                            swaps_leaderboard[taker_addy] = 1
                            #swaps_leaderboard[event["event"]["data"]["from"][0]] = 1
                    # adding maker addy
                    if event["event"]["type"] == "MakerPaymentSent":
                        maker_payment_hex = event["event"]["data"]["tx_hex"]
                        maker_payment_hex_decoded = rick_proxy.decoderawtransaction(maker_payment_hex)
                        maker_addy = maker_payment_hex_decoded["vout"][2]["scriptPubKey"]["addresses"][0]
                        swaps_participants.append(maker_addy)
                        #swaps_participants.append(event["event"]["data"]["from"][0])
                        if maker_addy in swaps_leaderboard.keys():
                            swaps_leaderboard[maker_addy] += 1
                        else:
                            swaps_leaderboard[maker_addy] = 1
                        #if event["event"]["data"]["from"][0] in swaps_leaderboard.keys():
                        #    swaps_leaderboard[event["event"]["data"]["from"][0]] += 1
                        #else:
                        #    swaps_leaderboard[event["event"]["data"]["from"][0]] = 1
            self.stress_test_swaps_data = {**self.stress_test_swaps_data, **stress_test_swaps_detailed_data}
            # self.stress_test_swaps_data = stress_test_swaps_detailed_data

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

        unique_participants = list(set(swaps_participants))
        # SUMMARY CALL
        self.summary.append({
            "trading_pairs": pair,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "swaps_count_total": swaps_count,
            "swaps_unique_participants": unique_participants,
            "swaps_leaderboard": swaps_leaderboard
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
        }

        self.stress_test_summary = {
            "stress_test_start": stress_test_start,
            "stress_test_end": stress_test_end,
            "swaps_per_hour": abs(round((float(60 * swaps_count / minutes_since_stress_test_start)), 10)),
            "participants_per_hour": abs(round((float(60 * len(unique_participants) / minutes_since_stress_test_start)), 10))
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
        with open('/root/dex_stats_pymongo/data/orderbook.json', 'w') as f:
            json.dump(self.orderbook, f)

    def save_ticker_data_as_json(self):
        with open('/root/dex_stats_pymongo/data/ticker.json', 'w') as f:
            json.dump(self.ticker, f)

    def save_summary_data_as_json(self):
        with open('/root/dex_stats_pymongo/data/summary.json', 'w') as f:
            json.dump(self.summary, f)

    def save_trades_data_as_json(self):
        with open('/root/dex_stats_pymongo/data/trades.json', 'w') as f:
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
