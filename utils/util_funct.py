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


def enforce_float_type(num: [float, int, str]) -> float:
    return float("{:.10f}".format(num))
