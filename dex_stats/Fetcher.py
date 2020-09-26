import sys
import time
import json
import logging
from datetime import date, datetime, timedelta

from utils import adex_calls
from utils import adex_tickers
from MongoAPI import MongoAPI
from utils.adex_calls import get_orderbook
from utils.utils import enforce_float
from utils.utils import measure



class Fetcher:
    def __init__(self):
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        self.mongo = MongoAPI()

        #endpoint data variables
        self.summary    = []
        self.ticker     = {}
        self.orderbook  = []
        self.trades     = []

        self.possible_pairs = list(itertools.permutations(adex_tickers, 2))
        self.pairs = self.mongo.get_trading_pairs()


    @measure
    def pipeline(self):
        for pair in self.pairs:
            self.fetch_data_for_existing_pair(pair)

        for c, summary in enumerate(self.summary):
            print("\n{} --> {}".format(c, summary))


    #TODO: Add zero-calls for non-existent tickers
    @measure
    def fetch_data_for_existing_pair(self, pair):
        trading_pairs  = pair.split("_")
        base_currency  = trading_pairs[0]
        quote_currency = trading_pairs[1]

        last_price   = 0.
        lowest_ask   = 0.
        highest_bid  = 0.
        base_volume  = 0.
        quote_volume = 0.

        swap_prices       = list()
        price_change_24h  = 0.
        highest_price_24h = 0.
        lowest_price_24h  = 0.

        asks, lowest_ask, bids, highest_bid = self.fetch_orderbook(base_currency, quote_currency)

        timestamp_right_now = datetime.now().strftime("%s") // 1000
        timestamp_24h_ago = int((datetime.now() - timedelta(1000)).strftime("%s")) // 1000
        swaps_last_24h    = self.mongo.find_swaps_for_market_since_timestamp( base_currency,
                                                                              quote_currency,
                                                                              timestamp_24h_ago )

        #TODO: figure this one out as well...
        #to make sure swaps are in the ascending order
        #swaps_last_24h = sorted( swaps_last_24h,
        #                         key=lambda q: q["events"][0]["timestamp"] )

        for swap in swaps_last_24h:
            first_event = swap["events"][0]["event"]["data"]

            swap_price  = (
                            enforce_float(first_event["taker_amount"])
                            /
                            enforce_float(first_event["maker_amount"])
                          )
            swap_prices.append(swap_price)

            base_volume  += enforce_float(first_event["maker_amount"])
            quote_volume += enforce_float(first_event["taker_amount"])

            try:
                lowest_price_24h  = min(swap_prices)
            except ValueError:
                lowest_price_24h  = 0.

            try:
                highest_price_24h = max(swap_prices)
            except ValueError:
                highest_price_24h = 0.

            price_start_24h = swap_prices[0]  
                              if swap_prices 
                              else 0.
            last_price      = swap_prices[-1]
                              if swap_prices 
                              else 0.

            price_change_24h = ( (
                                    last_price
                                    -
                                    price_start_24h 
                                  )
                                    /
                                    100.
                                )
            uuid = first_event['uuid']

            #TRADES CALL
            #TODO: figure out type buy/sell
            self.trades.append({
                             "​trade_id​" ​: uuid,
                        ​        "price"​ : swap_price,
                        ​  "base_volume" ​: base_volume,
                         ​"quote_volume"​ : quote_volume,
                        ​    "timestamp"​ : timestamp_right_now,
                                 "type"​ : "sell"
            })

        #SUMMARY CALL
        #TODO: figure out exponents.
        self.summary.append({
                        "trading_pairs" : pair,
                        "base_currency" : base_currency,
                       "quote_currency" : quote_currency,
                           "last_price" : enforce_float(last_price),
                           "lowest_ask" : enforce_float(lowest_ask),
                          "highest_bid" : enforce_float(highest_bid),
                          "base_volume" : enforce_float(base_volume),
                         "quote_volume" : enforce_float(quote_volume),
             "price_change_percent_24h" : enforce_float(price_change_24h),
                    "highest_price_24h" : enforce_float(highest_price_24h),
                     "lowest_price_24h" : enforce_float(lowest_price_24h)
        })

        #TICKER CALL
        #TODO: figure out base/quote id --> https://docs.google.com/document/d/1a5JfNE8aXusvfZBnEokwzp1-vGNJ_SPo-jIXhfnnEYE/edit
        self.ticker[pair] = {
                            ​  "base_id" ​: "0",
                             "quote_id"​ : "0",
                           ​"last_price" ​: last_price,
                         "quote_volume"​ : quote_volume,
                          "base_volume"​ : base_volume,
                             "isFrozen"​ : "0"
        }

        #ORDERBOOK CALL
        #TODO: figure out sorting by best asks/bids
        self.orderbook[pair] = {
                            "timestamp"​: datetime.now().strftime("%s"),
                                 "bids"​: bids,
                                 ​"asks"​: asks
        }




    def fetch_orderbook(base_currency, quote_currency):
        mm2_localhost = "http://127.0.0.1:7783"
        mm2_username  = "testuser"
        orderbook     = get_orderbook( mm2_localhost,
                                       mm2_username,
                                       base_currency,
                                       quote_currency )
        try:
            asks = [ ask
                     for ask
                     in orderbook["asks"] ]
        except (KeyError, ValueError):
            asks = []

        try:
            lowest_ask = min([  float(ask['price'])
                                for ask
                                in orderbook["asks"] ])
        except (KeyError, ValueError):
            lowest_ask = 0.

        try:
            bids = [ float(bid)
                     for bid
                     in orderbook["bids"] ]
        except (KeyError, ValueError):
            bids = []

        try:
            highest_bid = max([ float(bid['price'])
                                for bid
                                in orderbook["bids"] ])
        except (KeyError, ValueError):
            highest_bid = 0.

        return asks, lowest_ask, bids, highest_bid







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



    # DATA VALIDATION
    def validate_by_amount(self):
        count_true  = 0
        total_count = len(self.trading_pairs) - 1
        for pair, amount_in_pairs_collection in self.trading_pairs.items():
            base  = pair.split("_")[0]
            quote = pair.split("_")[1]
            swaps = self.mongo.find_swaps_for_market( base, quote )
            swaps_amount_in_db = len(swaps)
            decision           = False
            if swaps_amount_in_db == amount_in_pairs_collection:
                decision    = True
                count_true += 1
            logging.debug(   "\n\tIn trading_pairs: {} --> {}\
                        \nIn successful collection: {}\
                                    \n\t\tDecision: {}\n".format( pair, amount_in_pairs_collection,
                                                                  swaps_amount_in_db,
                                                                  decision ))
        logging.debug("BY AMOUNT: out of {} pairs {} validated as true".format( total_count,
                                                                                count_true ))


if __name__ == "__main__":
    f = Fetcher()
    f.pipeline()







'''
def fetch_summary_data():
        
    summary_endpoint_data = []
    ticker_endpoint_data = []
    orderbook_data = []
    trades_data = []

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




fetch_summary_data()
'''