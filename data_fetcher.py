import datetime
import time
import json
from db_connector import MongoAPI


# TODO: do it only once on startup -> save to file on shutdown
def find_unique_pairs():
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

start_time = time.time()

# adex_tickers = ["AWC", "AXE", "BAT", "BCH", "BET", "BOTS", "BTC", "BUSD", "CCL", "CHIPS", "CRYPTO", "DAI", "DASH",
#                 "DEX", "DGB", "DOGE", "ECA", "EMC2", "ETH", "FTC", "HUSH", "ILN", "JUMBLR", "KMD", "LABS", "LTC",
#                 "MCL", "MGW", "MORTY", "NAV", "OOT", "PANGEA", "PAX", "QTUM", "REVS", "RFOX", "RICK", "RVN",
#                 "SUPERNET", "TUSD", "USDC", "VRSC", "XZC", "ZEC", "ZER"]
#
# 45 tickers atm = 1980 pairs // 283 in db as of 09.27
# possible_pairs = list(itertools.permutations(adex_tickers, 2))
possible_pairs = find_unique_pairs()  # ~25s execution vs ~207s for above
db_connection = MongoAPI()


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
            #pair_orderbook = json.loads(adex_calls.get_orderbook("http://127.0.0.1:7783", "testuser", pair[0], pair[1]).text)
            pair_swaps_last_24h = []

            #try:
            #    lowest_ask  = min([float(x['price']) for x in pair_orderbook["asks"]])
            #    highest_bid = max([float(x['price']) for x in pair_orderbook["bids"]])
            ## TODO: proper handling of empty bid/asks
            #except Exception:
            #     pass

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
                         "last_price": float("{:.10f}".format(last_price)),
                         "last_trade_time": last_timestamp,
                         "base_volume_24h": float("{:.10f}".format(base_volume_24h)),
                         "quote_volume_24h": float("{:.10f}".format(quote_volume_24h)),
                         "highest_price_24h": float("{:.10f}".format(highest_price_24h)),
                         "lowest_price_24h": float("{:.10f}".format(lowest_price_24h)),
                         "price_change_percent_24h": float("{:.10f}".format(price_change_percent_24h)),
                         "lowest_ask": float("{:.10f}".format(lowest_ask)),
                         "highest_bid": float("{:.10f}".format(highest_bid))}
            summary_endpoint_data.append(pair_data)

            ticker_data_pair = {pair[0] + "_" + pair[1]: {
                "last_price": float("{:.10f}".format(last_swap_price)),
                "base_volume": float("{:.10f}".format(base_volume_24h)),
                "quote_volume": float("{:.10f}".format(quote_volume_24h))
            }}
            ticker_endpoint_data.append(ticker_data_pair)

            orderbook_data_pair = {pair[0] + "_" + pair[1]: {
                "timestamp": int(round(time.time() * 1000)),
                # TODO: sort orders
                "bids": [],
                "asks": []
            }}
            #for bid in pair_orderbook["bids"]:
            #    orderbook_data_pair[pair[0] + "_" + pair[1]]["bids"].append([bid["price"], bid["maxvolume"]])
#
            #for ask in pair_orderbook["asks"]:
            #    orderbook_data_pair[pair[0] + "_" + pair[1]]["bids"].append([ask["price"], ask["maxvolume"]])

            orderbook_data.append(orderbook_data_pair)

            trades_data_pair = {pair[0] + "_" + pair[1]: []}

            for swap in pair_swaps_last_24h:
                first_event = swap["events"][0]["event"]
                trades_data_pair[pair[0] + "_" + pair[1]].append({
                    "trade_id": swap["uuid"],
                    "price": float("{:.10f}".format(
                             (float(first_event["data"]["taker_amount"])
                              / float(first_event["data"]["maker_amount"]))
                    )),
                    "base_volume": float("{:.10f}".format(float(first_event["data"]["maker_amount"]))),
                    "quote_volume": float("{:.10f}".format(float(first_event["data"]["taker_amount"]))),
                    "timestamp": int(swap["events"][0]["timestamp"] // 1000),
                    #TODO: a bit confused here, probably directions like a DEX/KMD KMD/DEX needs to be combined to determine buys/sells
                    "type": "buy"
                })

            trades_data.append(trades_data_pair)

    with open('summary.json', 'w') as f:
        json.dump(summary_endpoint_data, f)

    with open('ticker.json', 'w') as f:
        json.dump(ticker_endpoint_data, f)

    with open('orderbook_data.json', 'w') as f:
        json.dump(orderbook_data, f)

    with open('trades.json', 'w') as f:
        json.dump(trades_data, f)


fetch_summary_data()

print("--- %s seconds ---" % (time.time() - start_time))