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
        with open('summary.json') as json_file:
            data = json.load(json_file)
        return jsonify(data)


@api.route('/api/v1/ticker')
class Ticker(Resource):
    def get(self):
        with open('ticker.json') as json_file:
            data = json.load(json_file)
        return jsonify(data)


@api.route('/api/v1/orderbook/<market_pair>')
class Orderbook(Resource):
    def get(self, market_pair):
        with open('orderbook_data.json') as json_file:
            #TODO: handle non existent pair
            pairs_data = json.load(json_file)
            for pair in pairs_data:
                if market_pair in pair.keys():
                    data = pair
        return jsonify(data)


@api.route('/api/v1/trades/<market_pair>')
class Trades(Resource):
    def get(self, market_pair):
        with open('trades.json') as json_file:
            #TODO: handle non existent pair
            pairs_data = json.load(json_file)
            for pair in pairs_data:
                if market_pair in pair.keys():
                    data = pair
        return jsonify(data)


if __name__ == '__main__':
    #logging.basicConfig(filename='/var/log/dex_prices_endpoint.log',level=logging.DEBUG)
    app.run(host='0.0.0.0', port=8080)

