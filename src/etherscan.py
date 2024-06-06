#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
from web3 import Web3
from token_abi import token_abi
from UniswapV2Pair import pair_abi
from UniswapV3Pool import pool_abi

MAX_RETRY = 10
USDC_LIKE = ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(), '0x0000000000085d4780b73119b644ae5ecd22b376'.lower()]
HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
ETHERSCAN_GETINTERNALS = 'http://api.etherscan.io/api?module=account&action=txlistinternal&address={}&startblock={}&endblock={}&apikey={}'
ETHERSCAN_GETINTERNALS_TX = 'http://api.etherscan.io/api?module=account&action=txlistinternal&txhash={}&startblock={}&endblock={}&apikey={}'
ETHERSCAN_GETETHUSD_DAILY = 'https://api.etherscan.io/api?module=stats&action=ethdailyprice&startdate={:%Y-%m-%d}&enddate={:%Y-%m-%d}&sort=asc&apikey={}'
ETHERSCAN_GETETHUSD_LAST = 'https://api.etherscan.io/api?module=stats&action=ethprice&apikey={}'

def etherscan_get_ethusd(etherscan_key, startdate=None, enddate=None):
    if startdate is None:
        etherscan_request = ETHERSCAN_GETETHUSD_LAST.format(etherscan_key)
    else:
        etherscan_request = ETHERSCAN_GETETHUSD_DAILY.format(startdate, enddate, etherscan_key)
    res = requests.get(etherscan_request, headers=HEADERS)
    d = res.json()
    return d["result"]

def etherscan_get_internals(etherscan_key, block_number, address=None, txhash=None, session=None):
    if address:
        param = address
        etherscan_request = ETHERSCAN_GETINTERNALS
    elif txhash:
        param = txhash
        etherscan_request = ETHERSCAN_GETINTERNALS_TX
    try:
        if session:
            res = session.get(etherscan_request.format(param, block_number, block_number, etherscan_key), headers=HEADERS)
            d = res.json()
            if d["result"] == 'Max rate limit reached':
                time.sleep(0.2)
                res = session.get(etherscan_request.format(param, block_number, block_number, etherscan_key), headers=HEADERS, force_refresh=True)
                d = res.json()
        else:
            res = requests.get(etherscan_request.format(param, block_number, block_number, etherscan_key), headers=HEADERS)
            d = res.json()
        return d["result"]
    except:
        print("etherscan error", res.status_code)
        return None

def _get_abi(address, etherscan_key):
    try:
        res = requests.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS)
        d = res.json()
        abi = d["result"]
        return abi
    except:
        return None

def _get_contract(w3, abi, address):
    return w3.eth.contract(address=address, abi=abi)

def get_contract_sync(address, context=None, w3=None, abi_type=None):
    if address in context["contract_storage"]:
        return context["contract_storage"][address], context["abi_storage"][address]
        
    _address = Web3.to_checksum_address(address)
    abi = None
    if address in USDC_LIKE:
        abi = token_abi
    elif abi_type:
        if abi_type == "token":
            abi = token_abi
        elif abi_type == "pair":
            abi = pair_abi
        elif abi_type == "pool":
            abi = pool_abi        
    if abi is None:
        for i in range(MAX_RETRY):
            if not abi is None:
                break
            abi = _get_abi(_address, context["etherscan_key"])
           
            if not abi and i < MAX_RETRY-1:
                time.sleep(1)
        else:
            return None, None
    try:
        contract = _get_contract(w3, abi, _address)
    except:
        contract = None
    if not contract is None:
        context["abi_storage"][address] = abi
        context["contract_storage"][address] = contract
    return contract, abi
