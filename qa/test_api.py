import pytest
import requests
from pytest_utils.util import validate_template


class TestAPI:

    def test_ticker_call(self, test_params):
        schema_ticker = {
            'type': 'array',
            'items': {
                'type': 'object',
                'propertyNames': {'pattern': r"\A[A-Z]+-[A-Z]+\Z"},
                'patternProperties': {"": {
                    'type': 'object',
                    'properties': {
                        'base_volume': {'type': ['number', 'integer']},
                        'last_price': {'type': ['string', 'integer']},
                        'quote_volume': {'type': ['number', 'integer']}
                    }}
                }
            }
        }
        header = {"accept: application/json"}
        url = ("http://" + test_params.get('ip') + ':' +
               test_params.get('port') + "api/v1/ticker")
        res = requests.get(url, headers=header)
        assert validate_template(res, schema_ticker)

    def test_summary_call(self, test_params):
        schema_summary = {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'base_currency': {'type': 'string',
                                      'pattern': r"\A[A-Z]+\Z"},
                    'base_volume_24h': {'type': ['integer', 'number']},
                    'highest_bid': {'type': 'string'},
                    'highest_price_24h': {'type': 'string'},
                    'last_price': {'type': 'string'},
                    'last_trade_time': {'type': ['integer', 'number']},
                    'lowest_ask': {'type': 'string'},
                    'lowest_price_24h': {'type': 'string'},
                    'price_change_percent_24h': {'type': 'string'},
                    'quote_currency': {'type': 'string',
                                       'pattern': r"\A[A-Z]+\Z"},
                    'quote_volume_24h': {'type': ['integer', 'number']},
                    'trading_pair': {'type': 'string',
                                     'pattern': r"\A[A-Z]+_[A-Z]+\Z"}
                }
            }
        }
        header = {"accept: application/json"}
        url = ("http://" + test_params.get('ip') + ':' +
               test_params.get('port') + "api/v1/summary")
        res = requests.get(url, headers=header)
        assert validate_template(res, schema_summary)

    def test_orderbook_call(self, test_params):
        schema_orderbook = {
            'type': 'object',
            'propertyNames': {'pattern': r"\A[A-Z]+_[A-Z]+\Z"},
            'patternProperties': {"": {
                'type': 'object',
                'properties': {
                    'asks': {
                        'type': 'array',
                        'items': {
                            'type': 'array',
                            'items': {
                                'type': 'string',
                                'pattern': r"\A[0-9]+.[0-9]+\Z"
                            },
                            'additionalItems': False,
                            'minItems': 2,
                            'maxItems': 2
                        }
                    },
                    'bids': {
                        'type': 'array',
                        'items': {
                            'type': 'array',
                            'items': {
                                'type': 'string',
                                'pattern': r"\A[0-9]+.[0-9]+\Z"
                            },
                            'additionalItems': False,
                            'minItems': 2,
                            'maxItems': 2
                        }
                    },
                    'timestamp': {
                        'type': 'integer'
                    }
                }
            }
            }
        }
        pair = test_params.get('base') + '_' + test_params.get('rel')
        header = {"accept: application/json"}
        url = ("http://" + test_params.get('ip') + ':' +
               test_params.get('port') + "api/v1/orderbook/" + pair)
        res = requests.get(url, headers=header)
        assert validate_template(res, schema_orderbook)

    def test_trades_call(self, test_params):
        schema_trades = {
            'type': 'object',
            'propertyNames': {'pattern': r"\A[A-Z]+_[A-Z]+\Z"},
            'patternProperties': {"": {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'base_volume': {'type': ['integer', 'number']},
                        'price': {
                            'type': 'string',
                            'pattern': r"\A[0-9]+.[0-9]+\Z"
                        },
                        'quote_volume': {'type': ['integer', 'number']},
                        'timestamp': {'type': 'integer'},
                        'trade_id': {
                            'type': 'string',
                            'pattern': r"\A[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}\Z"
                        },
                        'type': {
                            'type': 'string',
                            'pattern': r"\ABuy|Sell|buy|sell\Z"
                        }
                    }
                }
            }
            }
        }
        pair = test_params.get('base') + '_' + test_params.get('rel')
        header = {"accept: application/json"}
        url = ("http://" + test_params.get('ip') + ':' +
               test_params.get('port') + "api/v1/trades/" + pair)
        res = requests.get(url, headers=header)
        assert validate_template(res, schema_trades)
