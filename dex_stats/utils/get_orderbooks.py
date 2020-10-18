import itertools
from adex_tickers import tickers
from adex_calls   import get_orderbook


# 45 tickers atm = 1980 pairs
possible_pairs = list(itertools.permutations(adex_tickers, 2))

for pair in possible_pairs:
    print(get_orderbook("http://127.0.0.1:7783", "testuser", pair[0], pair[1]).text)
