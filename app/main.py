import json
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/api/v1/leaderboard')
async def summary():
    with open('data/stress_test.json') as f:
        summary = json.load(f)
    return summary

@app.get('/api/v1/stress_test_summary')
async def summary():
    with open('data/stress_test_summary.json') as f:
        summary = json.load(f)
    return summary

@app.get('/api/v1/stress_test_uuids')
async def summary():
    with open('data/stress_test_uuids.json') as f:
        summary = json.load(f)
    return summary

# @app.get('/api/v1/summary')
# async def summary():
#     with open('data/summary.json') as f:
#         summary = json.load(f)
#     return summary
#
#
# @app.get('/api/v1/ticker')
# async def ticker():
#     with open('data/ticker.json') as f:
#         ticker = json.load(f)
#     return ticker
#
#
# @app.get('/api/v1/orderbook/{market_pair}')
# async def orderbook(market_pair: str = "ALL"):
#     with open('data/orderbook.json') as f:
#         orderbook = json.load(f)
#         try:
#             return orderbook[market_pair]
#         except KeyError:
#             return {'error': 'no such pair'}
#
#
# @app.get('/api/v1/trades/{market_pair}')
# async def trades(market_pair: str = "ALL"):
#     with open('data/trades.json') as f:
#         trades = json.load(f)
#         try:
#             return trades[market_pair]
#         except KeyError:
#             return {'error': 'no such pair'}
