import json
import logging
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


@app.get('/api/v1/summary')
async def summary():
    with open('/app/data/summary.json') as f:
        summary = json.load(f)
    return summary


@app.get('/api/v1/ticker')
async def ticker():
    with open('/app/data/ticker.json') as f:
        ticker = json.load(f)
    return ticker


@app.get('/api/v1/orderbook/{market_pair}')
async def orderbook(market_pair: str = "ALL"):
    with open('/app/data/orderbook.json') as f:
        orderbook = json.load(f)
        try:
            return orderbook[market_pair]
        except KeyError:
            return {'error': 'no such pair'}


@app.get('/api/v1/trades/{market_pair}')
async def trades(market_pair: str = "ALL"):
    with open('/app/data/trades.json') as f:
        trades = json.load(f)
        try:
            return trades[market_pair]
        except KeyError:
            return {'error': 'no such pair'}
