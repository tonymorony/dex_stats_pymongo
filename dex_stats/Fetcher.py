import datetime
import time
import json
from db_connector import MongoAPI
from utils import adex_calls
from utils.util_funct import find_unique_pairs
from utils.util_funct import enforce_float_type as enforce



class Fetcher:
    def __init__(self):
        self.mongo = MongoAPI()

        #endpoint data variables
        self.summary_data = []
        self.ticker_data = []
        self.orderbook_data = []
        self.trades_data = []

        #utils
        self.possible_pairs = find_unique_pairs()


    def fetch_summary_data(self):
        pass


    def fetch_tickers_data(self):
        pass


    def fetch_orderbook_data(self):
        pass


    def fetch_trades_data(self):
        pass


    def save_orderbook_data_as_json(self):
        with open('orderbook_data.json', 'w') as f:
            json.dump(orderbook_data, f)


    def save_ticker_endpoint_data_as_json(self):
        with open('ticker.json', 'w') as f:
            json.dump(ticker_endpoint_data, f)


    def save_summary_endpoint_data_as_json(self):
        with open('summary.json', 'w') as f:
            json.dump(summary_endpoint_data, f)


    def save_trades_data_as_json(self):
        with open('trades.json', 'w') as f:
            json.dump(trades_data, f)











'''
def fetch_summary_data():

    for pair in possible_pairs:

        pair_swaps = list(db_connection.find_swaps_for_market(pair[0], pair[1]))
        total_swaps = len(pair_swaps)
        # fetching data for pairs with historical swaps only
        if total_swaps > 0:

            first_timestamp_24h = 0
            last_timestamp_24h = 0
            last_timestamp = 0
            last_price = 0
            base_volume_24h = 0
            quote_volume_24h = 0
            price_change_percent_24h = 0
            highest_price_24h = 0
            lowest_price_24h = 0
            first_swap_price = 0
            last_swap_price = 0
            lowest_ask = 0
            highest_bid = 0
            timestamp_24h_ago = int((datetime.date.today() - datetime.timedelta(1)).strftime("%S"))
            pair_orderbook = json.loads(adex_calls.get_orderbook("http://127.0.0.1:7783", "testuser", pair[0], pair[1]).text)
            pair_swaps_last_24h = []

            try:
                lowest_ask = min([float(x['price']) for x in pair_orderbook["asks"]])
            except (ValueError, KeyError):
                lowest_ask = float("{:.2f}".format(0.0))
            try:
                highest_bid = max([float(x['price']) for x in pair_orderbook["bids"]])
            except (ValueError, KeyError):
                highest_bid = float("{:.2f}".format(0.0))

            for swap in pair_swaps:
                # TODO: make a get_price funciton or maybe it worth to add on DB population stage
                first_event = swap["events"][0]["event"]
                # adex timestamp are in ms
                swap_timestamp = swap["events"][0]["timestamp"] // 1000
                # TODO: have to get only succesful swaps or last price might be counted for failed too!
                if first_event["type"] == "Started":
                    swap_price = (float(first_event["data"]["taker_amount"])
                                  / float(first_event["data"]["maker_amount"]))
                    # 24h volume and price calculating price
                    if swap_timestamp > timestamp_24h_ago:
                        pair_swaps_last_24h.append(swap)
                        base_volume_24h += float(first_event["data"]["maker_amount"])
                        quote_volume_24h += float(first_event["data"]["taker_amount"])
                        if swap_price > highest_price_24h:
                            highest_price_24h = swap_price
                        if lowest_price_24h == 0:
                            lowest_price_24h = swap_price
                        elif swap_price < lowest_price_24h:
                            lowest_price_24h = swap_price
                        # last trade 24h determining part
                        if swap_timestamp > last_timestamp_24h:
                            last_price = float(swap_price)
                            last_timestamp_24h = swap_timestamp
                            last_swap_price = float(swap_price)
                        # first trade 24h determining part
                        if swap_timestamp == 0:
                            first_timestamp_24h = swap_timestamp
                            first_swap_price = float(swap_price)
                        elif swap_timestamp < first_timestamp_24h:
                            first_timestamp_24h = swap_timestamp
                            first_swap_price = float(swap_price)
                    # last trade overall determining part - there might be no swaps in 24h, thats why its here
                    if swap_timestamp > last_timestamp:
                        last_price = float(swap_price)
                        last_timestamp = swap_timestamp
                price_change_percent_24h = (float(last_swap_price) - float(first_swap_price)) / float(100)

            pair_data = {"trading_pair": pair[0] + "_" + pair[1],
                         "base_currency": pair[0],
                         "quote_currency": pair[1],
                         "last_price": enforce(last_price),
                         "last_trade_time": last_timestamp,
                         "base_volume_24h": enforce(base_volume_24h),
                         "quote_volume_24h": enforce(quote_volume_24h),
                         "highest_price_24h": enforce(highest_price_24h),
                         "lowest_price_24h": enforce(lowest_price_24h),
                         "price_change_percent_24h": enforce(price_change_percent_24h),
                         "lowest_ask": enforce(lowest_ask),
                         "highest_bid": enforce(highest_bid)}
            summary_endpoint_data.append(pair_data)

            ticker_data_pair = {pair[0] + "_" + pair[1]: {
                "last_price": enforce(last_swap_price),
                "base_volume": enforce(base_volume_24h),
                "quote_volume": enforce(quote_volume_24h)
            }}
            ticker_endpoint_data.append(ticker_data_pair)

            orderbook_data_pair = {pair[0] + "_" + pair[1]: {
                "timestamp": int(round(time.time() * 1000)),
                # TODO: sort orders
                "bids": [],
                "asks": []
            }}
            for bid in pair_orderbook["bids"]:
                orderbook_data_pair[pair[0] + "_" + pair[1]]["bids"].append([bid["price"], bid["maxvolume"]])

            for ask in pair_orderbook["asks"]:
                orderbook_data_pair[pair[0] + "_" + pair[1]]["bids"].append([ask["price"], ask["maxvolume"]])

            orderbook_data.append(orderbook_data_pair)

            trades_data_pair = {pair[0] + "_" + pair[1]: []}

            for swap in pair_swaps_last_24h:
                first_event = swap["events"][0]["event"]
                trades_data_pair[pair[0] + "_" + pair[1]].append({
                    "trade_id": swap["uuid"],
                    "price": enforce(
                              float(first_event["data"]["taker_amount"])
                              / float(first_event["data"]["maker_amount"])),
                    "base_volume": enforce(float(first_event["data"]["maker_amount"])),
                    "quote_volume": enforce(float(first_event["data"]["taker_amount"])),
                    "timestamp": int(swap["events"][0]["timestamp"] // 1000),
                    #TODO: a bit confused here, probably directions like a DEX/KMD KMD/DEX needs to be combined to determine buys/sells
                    "type": "buy"
                })

            trades_data.append(trades_data_pair)



# adex_tickers = ["AWC", "AXE", "BAT", "BCH", "BET", "BOTS", "BTC", "BUSD", "CCL", "CHIPS", "CRYPTO", "DAI", "DASH",
#                 "DEX", "DGB", "DOGE", "ECA", "EMC2", "ETH", "FTC", "HUSH", "ILN", "JUMBLR", "KMD", "LABS", "LTC",
#                 "MCL", "MGW", "MORTY", "NAV", "OOT", "PANGEA", "PAX", "QTUM", "REVS", "RFOX", "RICK", "RVN",
#                 "SUPERNET", "TUSD", "USDC", "VRSC", "XZC", "ZEC", "ZER"]
#
# 45 tickers atm = 1980 pairs // 283 in db as of 09.27
# possible_pairs = list(itertools.permutations(adex_tickers, 2))



fetch_summary_data()
'''