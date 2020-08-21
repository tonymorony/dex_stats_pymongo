import itertools
import json
from db_connector import MongoAPI

adex_tickers = ["AWC", "AXE", "BAT", "BCH", "BET", "BOTS", "BTC", "BUSD", "CCL", "CHIPS", "CRYPTO", "DAI", "DASH",
                "DEX", "DGB", "DOGE", "ECA", "EMC2", "ETH", "FTC", "HUSH", "ILN", "JUMBLR", "KMD", "LABS", "LTC",
                "MCL", "MGW", "MORTY", "NAV", "OOT", "PANGEA", "PAX", "QTUM", "REVS", "RFOX", "RICK", "RVN",
                "SUPERNET", "TUSD", "USDC", "VRSC", "XZC", "ZEC", "ZER"]

# 45 tickers atm = 1980 pairs
possible_pairs = list(itertools.permutations(adex_tickers, 2))
db_connection = MongoAPI()

def fetch_summary_data():
    summary_endpoint_data = []
    for pair in possible_pairs:
        # TODO: have to get only succesful swaps or last price might be counted for failed too!
        pair_swaps = list(db_connection.find_swaps_for_market(pair[0], pair[1]))
        total_swaps = len(pair_swaps)
        # fetching data for pairs with historical swaps only
        if total_swaps > 0:
            last_timestamp = 0
            last_price = 0
            for swap in pair_swaps:
                # TODO: make a get_price funciton or maybe it worth to add on DB population stage
                first_event = swap["events"][0]["event"]
                if first_event["type"] == "Started":
                    swap_price = float(first_event["data"]["taker_amount"]) / float(first_event["data"]["maker_amount"])
                    if swap["events"][0]["timestamp"] > last_timestamp:
                        last_price =  format(swap_price, '.10f')
                        last_timestamp = swap["events"][0]["timestamp"]
            pair_data = {"trading_pair": pair[0] + "_" + pair[1], "base_currency": pair[0],
                         "quote_currency": pair[1], "last_price": last_price,
                         "last_trade_time": last_timestamp}
            summary_endpoint_data.append(pair_data)

    with open('summary.json', 'w') as f:
        json.dump(summary_endpoint_data, f)

fetch_summary_data()
        
