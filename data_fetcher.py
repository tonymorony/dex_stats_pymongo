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

pairs_iter = 0
swaps_iter = 0
summary_endpoint_data = []
for pair in possible_pairs:
    total_swaps = len(list(db_connection.find_swaps_for_market(pair[0], pair[1])))
    # fetching data for pairs with historical swaps only
    if total_swaps > 0:
        pair_data = {}
        pair_data["trading_pair"] = pair[0] + "_" + pair[1]
        summary_endpoint_data.append(pair_data)

with open('summary.json', 'w') as f:
    json.dump(summary_endpoint_data, f)
    
