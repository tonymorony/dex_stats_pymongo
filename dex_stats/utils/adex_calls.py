import json
import requests


def batch_request(node_ip, user_pass, requests_list):
    r = requests.post(node_ip, json=requests_list)
    return r


def get_orderbook(node_ip, user_pass, base, rel):
    params = {
              'userpass': user_pass,
              'method': 'orderbook',
              'base': base,
              'rel': rel
             }
    r = requests.post(node_ip, json=params)
    return json.loads(r.text)
