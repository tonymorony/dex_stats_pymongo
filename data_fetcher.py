import itertools
from db_connector import MongoAPI

adex_tickers = ["AWC", "AXE", "BAT", "BCH", "BET", "BOTS", "BTC", "BUSD", "CCL", "CHIPS", "CRYPTO", "DAI", "DASH",
                "DEX", "DGB", "DOGE", "ECA", "EMC2", "ETH", "FTC", "HUSH", "ILN", "JUMBLR", "KMD", "LABS", "LTC",
                "MCL", "MGW", "MORTY", "NAV", "OOT", "PANGEA", "PAX", "QTUM", "REVS", "RFOX", "RICK", "RVN",
                "SUPERNET", "TUSD", "USDC", "VRSC", "XZC", "ZEC", "ZER"]

# 45 tickers atm = 990 pairs
possible_pairs = list(itertools.combinations(adex_tickers, 2))

db_connection = MongoAPI()

for pair in possible_pairs:
    # TODO: calculate needed data and dump as json
    db_connection.find_swaps_for_market(pair[0], pair[1])
    
