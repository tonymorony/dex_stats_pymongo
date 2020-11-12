import json
from random import randint

stub_timestamp = 1604188800
graph_data = {}
new_max = 0
while (stub_timestamp < 1604275200):
    new_max = randint(new_max, new_max + 20)
    graph_data[stub_timestamp] = new_max
    stub_timestamp += 600

with open('/home/shutdowner/dex_stats_pymongo/data/graph_data.json', 'w') as f:
    json.dump(graph_data, f)
