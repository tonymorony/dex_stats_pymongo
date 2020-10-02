import flask
import json
from flask import jsonify, request
from flask_restx import Resource, Api
import logging

app = flask.Flask(__name__)
api = Api(app)


@api.route('/api/v1/summary')
class Summary(Resource):
    def get(self):
        with open('data/summary.json') as f:
            summary = json.load(f)
        return jsonify(summary)


@api.route('/api/v1/ticker')
class Ticker(Resource):
    def get(self):
        with open('data/ticker.json') as f:
            ticker = json.load(f)
        return jsonify(ticker)


@api.route('/api/v1/orderbook/<market_pair>')
class Orderbook(Resource):
    def get(self, market_pair):
        with open('data/orderbook.json') as f:
            #TODO: handle non existent pair
            orderbook = json.load(f)
            try:
                return jsonify(orderbook[market_pair])
            except KeyError:
                return 'no data'


@api.route('/api/v1/trades/<market_pair>')
class Trades(Resource):
    def get(self, market_pair):
        with open('data/trades.json') as f:
            #TODO: handle non existent pair
            trades = json.load(f)
            try:
                return jsonify(trades[market_pair])
            except KeyError:
                return 'no data'


if __name__ == '__main__':
    #logging.basicConfig(filename='/var/log/dex_prices_endpoint.log',level=logging.DEBUG)
    app.run(host='0.0.0.0', port=8080)
