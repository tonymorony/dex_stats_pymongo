import flask
import json
from flask import jsonify, request
import logging

app = flask.Flask(__name__)

@app.route('/api/v1/summary', methods=['GET'])
def summary():
    with open('summary.json') as json_file:
        data = json.load(json_file)
    return jsonify(data)

@app.route('/api/v1/ticker', methods=['GET'])
def ticker():
    with open('ticker.json') as json_file:
        data = json.load(json_file)
    return jsonify(data)

@app.route('/api/v1/orderbook/<market_pair>', methods=['GET'])
def orderbook(market_pair="KMD_BTC"):
    with open('orderbook_data.json') as json_file:
        #TODO: handle non existent pair
        pairs_data = json.load(json_file)
        for pair in pairs_data:
            if market_pair in pair.keys():
                data = pair
    return jsonify(data)

if __name__ == '__main__':
    #logging.basicConfig(filename='/var/log/dex_prices_endpoint.log',level=logging.DEBUG)
    app.run(host='0.0.0.0', port=8080)
    
