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
    with open('ticker.jsonn') as json_file:
        data = json.load(json_file)
    return jsonify(data)

if __name__ == '__main__':
    logging.basicConfig(filename='/var/log/dex_prices_endpoint.log',level=logging.DEBUG)
    app.run(host='0.0.0.0', port=8080)
