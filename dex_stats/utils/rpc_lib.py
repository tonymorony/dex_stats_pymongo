import re
import os
import http
import platform
from slickrpc import Proxy


import re
import os
import http
import platform
from slickrpc import Proxy


# RPC connection
def get_rpc_details(chain):
    rpcport ='';
    operating_system = platform.system()
    if operating_system == 'Darwin':
        ac_dir = os.environ['HOME'] + '/Library/Application Support/Komodo'
    elif operating_system == 'Linux':
        ac_dir = os.environ['HOME'] + '/.komodo'
    elif operating_system == 'Win64' or operating_system == 'Windows':
        ac_dir = '%s/komodo/' % os.environ['APPDATA']
    if chain == 'KMD':
        coin_config_file = str(ac_dir + '/komodo.conf')
    else:
        coin_config_file = str(ac_dir + '/' + chain + '/' + chain + '.conf')
    with open(coin_config_file, 'r') as f:
        for line in f:
            l = line.rstrip()
            if re.search('rpcuser', l):
                rpcuser = l.replace('rpcuser=', '')
            elif re.search('rpcpassword', l):
                rpcpassword = l.replace('rpcpassword=', '')
            elif re.search('rpcport', l):
                rpcport = l.replace('rpcport=', '')
    if len(rpcport) == 0:
        if chain == 'KMD':
            rpcport = 7771
        else:
            print("rpcport not in conf file, exiting")
            print("check "+coin_config_file)
            exit(1)
    return rpcuser, rpcpassword, rpcport

def def_credentials(chain):
    rpc = get_rpc_details(chain)
    try:
        rpc_connection = Proxy("http://%s:%s@127.0.0.1:%d"%(rpc[0], rpc[1], int(rpc[2])))
    except Exception:
        raise Exception("Connection error! Probably no daemon on selected port.")
    return rpc_connection
