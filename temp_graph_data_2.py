import json
from random import randint

stub_timestamp = 1604188800
graph_data = {}
#while (stub_timestamp < 1604275200):
while (stub_timestamp < 1604205200):
    swaps_amount = randint(20, 100)
    graph_data[stub_timestamp] = swaps_amount
    stub_timestamp += 3600

with open('/home/shutdowner/dex_stats_pymongo/data/graph_data_2.json', 'w') as f:
    json.dump(graph_data, f)
