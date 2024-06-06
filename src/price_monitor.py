#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import json
from functools import wraps
import pandas as pd
import numpy as np
from web3 import Web3

from price_monitor_db import DBMySQL
from remote import RemoteServer
from etherscan import get_contract_sync, etherscan_get_internals, etherscan_get_ethusd

WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

PARAMETERS_FILE = "~/git/mev_price_monitor/parameters.json"
KEY_FILE = '../keys/alchemy.sec'
# ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

with open(os.path.expanduser(parameters["ETHERSCAN_KEY_FILE"]), 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

def provide_db(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "db" in kwargs:
            return f(*args, **kwargs)
        else:
            with RemoteServer(remote=REMOTE) as server:
                with DBMySQL(port=server.local_bind_port) as db:
                    return f(*args, **kwargs, db=db)
    return decorated

def s64(q):
    return -(q & 0x8000000000000000000000000000000000000000000000000000000000000000) | (q & 0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff)

def create_tables():
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            # db.create_tables(["t_blocks", "t_transactions", "t_events", "t_event_topics", "t_bundles"])
            db.create_tables(["t_attack_classes", "t_attack_events", "t_attacks", "t_attack_EMAs"])
            # db.create_tables(["t_attack_EMAs", "t_attacks"])
            # db.create_tables(["t_attackers", "t_event_dict"])

def web3connect2(key_file):
    with open(key_file, 'r') as f:
        k1 = f.readline()
        alchemy_url = k1.strip('\n')
        k2 = f.readline()
        alchemy_wss = k2.strip('\n')
    w3 = Web3(Web3.HTTPProvider(alchemy_url))
    latest_block = w3.eth.get_block("latest")
    return w3, latest_block, {"alchemy_url": alchemy_url, "alchemy_wss": alchemy_wss}


def process_block(block_number, run_context):
    w3 = run_context["w3"]
    block = w3.eth.get_block(block_number, full_transactions=True)

    miner = block["miner"]
    base_fee_per_gas = block["baseFeePerGas"]
    block_hash = block["hash"].hex()

    block_data = {"blockNumber": block_number,
                  "baseFeePerGas": base_fee_per_gas,
                  "blockHash": block_hash, 
                  "miner": miner}    

    if len(block["transactions"]) == 0:
        return block_data, [], [], {}

    _from_to_hashes = {}
    for transaction in block["transactions"]:
        if "to" in transaction and not transaction["to"] is None:
            if transaction["to"] in run_context["multisender_attackers"]:
                transaction_from = None
            else:
                transaction_from = transaction["from"]
            
            if (transaction_from, transaction["to"]) in run_context["attaker_status"] and run_context["attaker_status"][(transaction_from, transaction["to"])] == -1:
                continue
            if (None, transaction["to"]) in run_context["attaker_status"] and run_context["attaker_status"][(None, transaction["to"])] == -1:
                continue
            if not (transaction_from, transaction["to"]) in _from_to_hashes:
                _from_to_hashes[(transaction_from, transaction["to"])] = {"count": 1,
                                                                            "tx_counter": 0,
                                                                            "min_index": transaction["transactionIndex"],
                                                                            "max_index": transaction["transactionIndex"]}
            else:
                _from_to_hashes[(transaction_from, transaction["to"])]["count"] += 1
                _from_to_hashes[(transaction_from, transaction["to"])]["min_index"] = min(_from_to_hashes[(transaction_from, transaction["to"])]["min_index"],
                                                                                            transaction["transactionIndex"])
                _from_to_hashes[(transaction_from, transaction["to"])]["max_index"] = max(_from_to_hashes[(transaction_from, transaction["to"])]["max_index"],
                                                                                            transaction["transactionIndex"])
    from_to_hashes = {f: _from_to_hashes[f] for f in _from_to_hashes if _from_to_hashes[f]["count"] > 1}

    block_transactions = []
    block_events = []
    block_attakers = {}

    for from_to in from_to_hashes:
        from_to_events = []
        block_attakers[from_to] = {"status": 1}
        min_index = None
        max_index = None
        for ti in range(from_to_hashes[from_to]["min_index"], from_to_hashes[from_to]["max_index"]+1):
            transaction = block["transactions"][ti]
            if (transaction["from"] != from_to[0] and not from_to[0] is None) or transaction["to"] != from_to[1]:
                continue
            transaction_hash = transaction["hash"].hex()
            if transaction["transactionIndex"] != ti:
                print("something wrong with transaction index", block_number, ti, transaction["transactionIndex"])
                break
            if not transaction_hash in block_transactions:
                receipt = w3.eth.get_transaction_receipt(transaction_hash)
                if receipt["status"] != 1:
                    continue

                for e in receipt["logs"]:
                    e = dict(e)
                    e["transactionHash"] = e["transactionHash"].hex()
                    e["data"] = e["data"].hex()
                    e["topics"] = [bt.hex() for bt in e["topics"]]

                    # if not e["topics"][0] in run_context["topic_filter"]:
                    #     if not e["topics"][0] in run_context["unknown_topics"]:
                    #         run_context["unknown_topics"][e["topics"][0]] = transaction_hash
                    #     # print("unknown topic in sandwich-like transactions", transaction_hash, e["topics"][0])
                    #     tx_logs.append(e)
                    # elif not run_context["topic_filter"][e["topics"][0]]["note"] is None:
                    #     tx_logs.append(e)
                    # elif (e["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(e["topics"]) > 2 and
                    #       (e["topics"][1] == "0x0000000000000000000000000000000000000000000000000000000000000000" or 
                    #       e["topics"][2] == "0x0000000000000000000000000000000000000000000000000000000000000000")):
                    #     tx_logs.append(e) #!!! special case: Tranfer as NFT mint/burn
                    # elif (e["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(e["topics"]) > 1 and
                    #       e["topics"][2][-40:] == e["address"][-40:].lower()):
                    #     tx_logs.append(e) #!!! special case: Tranfer tax
                    from_to_events.append(e)
    
                if max_index is None or max_index < ti:
                    max_index = ti
                if min_index is None or min_index > ti:
                    min_index = ti
                block_transactions.append({
                    "hash": transaction_hash,
                    "blockNumber": block_number,
                    "fromTx": transaction["from"],
                    "toTx": transaction["to"],
                    "transactionIndex": transaction["transactionIndex"],
                    "gasUsed": receipt["gasUsed"],
                    "gasPrice": transaction["gasPrice"],
                    "maxFeePerGas": (transaction["maxFeePerGas"] if "maxFeePerGas" in transaction else None),
                    "maxPriorityFeePerGas": (transaction["maxPriorityFeePerGas"] if "maxPriorityFeePerGas" in transaction else None),
                    "gasBurnt": base_fee_per_gas * receipt["gasUsed"],
                    "gasOverpay": (receipt["effectiveGasPrice"] - base_fee_per_gas) * receipt["gasUsed"],
                    # "directBribe": ,
                    "value": transaction["value"],
                    "role": 1,
                    })
 
        from_to_hashes[from_to]["min_index"] = min_index
        from_to_hashes[from_to]["max_index"] = max_index
        if len(from_to_events) == 0:
            block_attakers[from_to] = {"status": 0}
        else:
            block_events.extend(from_to_events)


    for from_to in from_to_hashes:
        if block_attakers[from_to]["status"] == 0:
            continue
        for transaction in block["transactions"][from_to_hashes[from_to]["min_index"]+1:from_to_hashes[from_to]["max_index"]+1]:
            if (transaction["from"] == from_to[0] or from_to[0] is None) and transaction["to"] == from_to[1]:
                continue
            if "to" in transaction:
                from_to_hashes[from_to]["tx_counter"] += 1
                
    hashes_to_delete = []
    for from_to in from_to_hashes:
        if not from_to_hashes[from_to]["tx_counter"]:
            block_attakers[from_to] = {"status": 0}
            for t in block_transactions:
                if (t["fromTx"] == from_to[0] or from_to[0] is None) and t["toTx"] == from_to[1]:
                    hashes_to_delete.append(t["hash"])

    block_transactions = [t for t in block_transactions if not t["hash"] in hashes_to_delete]
    block_events = [e for e in block_events if not e["transactionHash"] in hashes_to_delete]
    
    if len([1 for a in block_attakers if block_attakers[a]["status"] == 1]):
        internal_transactions = etherscan_get_internals(etherscan_key=run_context["etherscan_key"],
                                                        block_number=block_number, address=miner)
        if not internal_transactions is None:
            for itx in internal_transactions:
                if itx["to"] == miner.lower():
                    for t in block_transactions:
                        if itx["hash"] == t["hash"]:
                            t["directBribe"] = int(itx["value"])

    block_bundles = {(block_number, from_to[0], from_to[1]):
                     {"transactions": [],
                      "a_innerTxNumber": from_to_hashes[from_to]["tx_counter"],
                      "directBribe": 0,
                      "gasBurnt": 0,
                      "gasOverpay": 0,
                      "min_index": from_to_hashes[from_to]["min_index"],
                      "max_index": from_to_hashes[from_to]["max_index"]}
                     for from_to in from_to_hashes if from_to_hashes[from_to]["tx_counter"] > 0}

    for t in block_transactions:
        if (block_number, t["fromTx"], t["toTx"]) in block_bundles:
            if (from_to_hashes[(t["fromTx"], t["toTx"])]["min_index"] <= t["transactionIndex"] and
                from_to_hashes[(t["fromTx"], t["toTx"])]["max_index"] >= t["transactionIndex"]):
                block_bundles[(block_number, t["fromTx"], t["toTx"])]["transactions"].append(t)
                block_bundles[(block_number, t["fromTx"], t["toTx"])]["directBribe"] += (t["directBribe"] if "directBribe" in t else 0)
                block_bundles[(block_number, t["fromTx"], t["toTx"])]["gasBurnt"] += (t["gasBurnt"] if "gasBurnt" in t else 0)
                block_bundles[(block_number, t["fromTx"], t["toTx"])]["gasOverpay"] += (t["gasOverpay"] if "gasOverpay" in t else 0)
        elif (block_number, None, t["toTx"]) in block_bundles:
            if (from_to_hashes[(None, t["toTx"])]["min_index"] <= t["transactionIndex"] and
                from_to_hashes[(None, t["toTx"])]["max_index"] >= t["transactionIndex"]):
                block_bundles[(block_number, None, t["toTx"])]["transactions"].append(t)
                block_bundles[(block_number, None, t["toTx"])]["directBribe"] += (t["directBribe"] if "directBribe" in t else 0)
                block_bundles[(block_number, None, t["toTx"])]["gasBurnt"] += (t["gasBurnt"] if "gasBurnt" in t else 0)
                block_bundles[(block_number, None, t["toTx"])]["gasOverpay"] += (t["gasOverpay"] if "gasOverpay" in t else 0)

    for b in block_bundles:
        block_bundles[b]["directBribe"] = block_bundles[b]["directBribe"] / 1e18
        block_bundles[b]["gasBurnt"] = block_bundles[b]["gasBurnt"] / 1e18
        block_bundles[b]["gasOverpay"] = block_bundles[b]["gasOverpay"] / 1e18

    block_data = {"blockNumber": block_number,
                  "baseFeePerGas": base_fee_per_gas,
                  "blockHash": block_hash, 
                  "miner": miner}

    return block_data, block_transactions, block_events, block_bundles

@provide_db
def write_block_transactions(block_data, block_transactions, block_events, block_bundles, db):
    db.add_block(block_data)
    db.add_bundles(block_bundles)
    for b in block_bundles:
        db.add_bundle_transactions(block_bundles[b]["bundleId"], block_bundles[b]["transactions"])
    db.add_events(block_events)
    db.commit()

@provide_db
def update_bundles(block_bundles, db):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            db.update_bundles(block_bundles)

@provide_db
def get_block_data(block_number, db):
    block_data = db.get_block(block_number)
    block_bundles = db.get_bundles(block_number)
    block_transactions = db.get_transactions(block_number)
    block_events = db.get_events(block_number)
    block_bundles = {(b["blockNumber"], b["attacker0"], b["attacker1"]): b for b in block_bundles}
    for b in block_bundles:
        if not block_bundles[b]["capitalRequirements"] is None:
            block_bundles[b]["capitalRequirements"] = json.loads(block_bundles[b]["capitalRequirements"])
        if not block_bundles[b]["saldo"] is None:
            block_bundles[b]["saldo"] = json.loads(block_bundles[b]["saldo"])
        if not block_bundles[b]["rates"] is None:
            block_bundles[b]["rates"] = {(r[0], r[1]): r[2] for r in json.loads(block_bundles[b]["rates"])}
        if not block_bundles[b]["features"] is None:
            features = json.loads(block_bundles[b]["features"])
            block_bundles[b].update(features)
    return block_data[0], block_transactions, block_events, block_bundles

@provide_db
def clean_block_data(block_number, db):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            db.clean_block_data(block_number)


STABLECOINS = {"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(): "USD Coin",
               "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(): "Tether USD",
               "0x6B175474E89094C44Da98b954EedeAC495271d0F".lower(): "Dai"}
_COIN_DECIMALS = {"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(): 1e6,
                 "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(): 1e6,
                 "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599".lower(): 1e8}
TOPICS_TO_PROCESS = {
    "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65": "withdraw",
    "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c": "deposit",
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": "a_uniswapV2",
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": "a_uniswapV3",
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83": "a_pancakeV3",
    "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde": "mint",
    "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0": "collect",
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "transfer",
    }

def coin_decimals(token):
    return (_COIN_DECIMALS[token] if token in _COIN_DECIMALS else 1e18)

def make_properties(event, trnx, bundle, token0=None, token1=None):
    if "saldo" in bundle and not bundle["saldo"] is None:
        if not "txs" in bundle or bundle["txs"] is None:
            bundle["txs"] = set()
        return
    if not event in TOPICS_TO_PROCESS:
        return
    if (TOPICS_TO_PROCESS[event] in ["withdraw", "deposit", "transfer"]):
        bundle["saldo"] = {"eth": 0}
        bundle["capitalRequirements"] = {"eth": 0}
    elif TOPICS_TO_PROCESS[event] in ["a_uniswapV2", "a_uniswapV3", "mint", "collect", "a_pancakeV3"]:
        bundle["saldo"] = {token0: 0, token1: 0, "eth": 0}
        bundle["capitalRequirements"] = {token0: 0, token1: 0, "eth": 0}
    bundle["rates"] = {}
    bundle["directBribe"] = 0
    bundle["gasBurnt"] = 0
    bundle["gasOverpay"] = 0
    bundle["txs"] = set()
    bundle["a_mintBurnV3"] = 0
    bundle["a_mintBurnNFT"] = 0
    bundle["a_uniswapV2"] = 0
    bundle["a_uniswapV3"] = 0
    bundle["a_pancakeV3"] = 0
    
def add_tokens(bundle, *tokens):
    for token in tokens:
        if not token in bundle["saldo"]:
            bundle["saldo"][token] = 0 
            bundle["capitalRequirements"][token] = 0 
            
def change_capital(bundle, *tokens):
    for token in tokens:
        if bundle["saldo"][token] < -bundle["capitalRequirements"][token]:
            bundle["capitalRequirements"][token] = -bundle["saldo"][token]

def update_rates(bundle, token0, token1, r1, r2):
    if r1 == 0 or r2 == 0:
        return
    bundle["rates"][(token0, token1)] = abs(r1 * coin_decimals(token0) / r2 / coin_decimals(token1))

def get_two_tokensV2(run_context, address):
    if not address in run_context["pairs_VXXX"]:
        try:
            contract, _ = get_contract_sync(address, w3=run_context["w3"], context=run_context, abi_type="pair")
            token0 = contract.functions.token0().call().lower()
            token1 = contract.functions.token1().call().lower()
        except:
            return (None, None)
        run_context["pairs_VXXX"][address] = (token0, token1)
    return run_context["pairs_VXXX"][address]

def get_two_tokensV3(run_context, address):
    if not address in run_context["pairs_VXXX"]:
        try:
            contract, _ = get_contract_sync(address, w3=run_context["w3"], context=run_context, abi_type="pool")
            token0 = contract.functions.token0().call().lower()
            token1 = contract.functions.token1().call().lower()
        except:
            return (None, None)
        run_context["pairs_VXXX"][address] = (token0, token1)
    return run_context["pairs_VXXX"][address]

def get_two_tokens_other(run_context, address):
    if not address in run_context["pairs_VXXX"]:
        try:
            contract, _ = get_contract_sync(address, w3=run_context["w3"], context=run_context)
            token0 = contract.functions.token0().call().lower()
            token1 = contract.functions.token1().call().lower()
        except:
            return (None, None)
        run_context["pairs_VXXX"][address] = (token0, token1)
    return run_context["pairs_VXXX"][address]

def update_gas(transaction, bundle):
    if not transaction["hash"] in bundle["txs"]:
        bundle["txs"].add(transaction["hash"])
        bundle["gasBurnt"] += int(transaction["gasBurnt"])/1e18
        bundle["saldo"]["eth"] -= int(transaction["gasBurnt"])/1e18
        bundle["gasOverpay"] += int(transaction["gasOverpay"])/1e18
        bundle["saldo"]["eth"] -= int(transaction["gasOverpay"])/1e18
        if "directBribe" in transaction:
            bundle["directBribe"] += int(transaction["directBribe"])/1e18
            bundle["saldo"]["eth"] -= int(transaction["directBribe"])/1e18
        if bundle["saldo"]["eth"] < -bundle["capitalRequirements"]["eth"]:
            bundle["capitalRequirements"]["eth"] = -bundle["saldo"]["eth"]

def revert_rate(tokenA, pair, rate):
    if tokenA == pair[0]:
        return rate
    else:
        if rate == 0:
            return 1e100
        else:
            return 1/rate

def find_rate(tokenA, tokenB, rates):
    # number of tokens B for 1 token A
    # two step max
    if tokenA == tokenB:
        return 1
    pair = (min(tokenA, tokenB), max(tokenA, tokenB))
    if pair in rates:
        return revert_rate(tokenA, pair, rates[pair])
    if tokenA in STABLECOINS and tokenB in STABLECOINS:
        return 1
    if tokenA in STABLECOINS:
        for tokenS in STABLECOINS:
            if tokenS != tokenA:
                pair1 = (min(tokenS, tokenB), max(tokenS, tokenB))
                if pair1 in rates:
                    return revert_rate(tokenS, pair1, rates[pair1])
    if tokenB in STABLECOINS:
        for tokenS in STABLECOINS:
            if tokenS != tokenB:
                pair1 = (min(tokenS, tokenA), max(tokenS, tokenA))
                if pair1 in rates:
                    return revert_rate(tokenA, pair1, rates[pair1])
    for p in rates:
        if tokenA in p and not tokenB in p:
            if tokenA == p[0]:
                pair1 = (min(p[1], tokenB), max(p[1], tokenB))
            else:
                pair1 = (min(p[0], tokenB), max(p[0], tokenB))
            if pair1 in rates:
                return revert_rate(tokenA, p, rates[p]) / revert_rate(tokenB, pair1, rates[pair1])
                # return rates[pair]
        elif tokenB in p and not tokenA in p:
            if tokenB == p[0]:
                pair1 = (min(p[1], tokenA), max(p[1], tokenA))
            else:
                pair1 = (min(p[0], tokenA), max(p[0], tokenA))
            if pair1 in rates:
                return revert_rate(tokenA, pair1, rates[pair1]) / revert_rate(tokenB, p, rates[p])
                # return rates[pair]
    return None

def process_bundles(run_context, events, transactions, bundles):
    fixed_weth_rate = run_context["eth_rate"] #!!!
    processed_bundles = {}
    for ii, e in enumerate(events):
        # if (e["transactionHash"] == "0xad20b98f98ce90a79c7f92d6879ac3d104e58394eefbdfefb890b23b450bfb5f" and
        #     e["topics"][0] == "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"):
        #     break
        # else:
        #     continue
        
        if not e["topics"][0] in TOPICS_TO_PROCESS:
            continue
        # print(ii, TOPICS_TO_PROCESS[e["topics"][0]], e["transactionHash"])
        
        for transaction in transactions:
            if transaction["hash"] == e["transactionHash"]:
                break
        else:
            continue

        if transaction["toTx"] in run_context["multisender_attackers"]:
            transaction_from = None
        else:
            transaction_from = transaction["fromTx"]
        if not (e["blockNumber"], transaction_from,
                          transaction["toTx"]) in processed_bundles:
            bundle = bundles[(e["blockNumber"], transaction_from,
                              transaction["toTx"])].copy()
            processed_bundles[(e["blockNumber"], transaction_from,
                              transaction["toTx"])] = bundle
        else:
            bundle = processed_bundles[(e["blockNumber"], transaction_from,
                              transaction["toTx"])]

        if TOPICS_TO_PROCESS[e["topics"][0]] == "transfer":
            make_properties(e["topics"][0], transaction, bundle)

            try:
                if (e["topics"][1] == "0x0000000000000000000000000000000000000000000000000000000000000000" or
                    e["topics"][2] == "0x0000000000000000000000000000000000000000000000000000000000000000"):
                    bundle["a_mintBurnNFT"] += 1
                elif  e["topics"][2][-40:] == e["address"][-40:].lower():
                    token = e["address"].lower()
                    add_tokens(bundle, token)
                    bundle["saldo"][token] -= int(e["data"],0)/coin_decimals(token)
                    change_capital(bundle, token)
            except:
                pass
                
            update_gas(transaction, bundle)

        elif TOPICS_TO_PROCESS[e["topics"][0]] == "withdraw":
            make_properties(e["topics"][0], transaction, bundle)
            add_tokens(bundle, WETH)

            try:
                amount = int(e["data"], 0) / 1e18
            except:
                try:
                    amount = int(e["topics"][2], 0) / 1e18
                except:
                    pass

            bundle["saldo"][WETH] -= amount
            bundle["saldo"]["eth"] += amount

            change_capital(bundle, WETH)
            update_gas(transaction, bundle)

        elif TOPICS_TO_PROCESS[e["topics"][0]] == "deposit":
            make_properties(e["topics"][0], transaction, bundle)
            add_tokens(bundle, WETH)

            try:
                amount = int(e["data"], 0) / 1e18
            except:
                try:
                    amount = int(e["topics"][2], 0) / 1e18
                except:
                    pass

            bundle["saldo"][WETH] += amount
            bundle["saldo"]["eth"] -= amount

            change_capital(bundle, WETH)
            update_gas(transaction, bundle)

        elif TOPICS_TO_PROCESS[e["topics"][0]] == "mint":
            (token0, token1) = get_two_tokensV3(run_context, e["address"])
            if token0 is None:
                continue
            make_properties(e["topics"][0], transaction, bundle, token0, token1)
            bundle["a_mintBurnV3"] += 1
            add_tokens(bundle, token0, token1)
            update_gas(transaction, bundle)

            bundle["saldo"][token0] -= int("0x"+e["data"][130:194],0)/coin_decimals(token0)
            bundle["saldo"][token1] -= int("0x"+e["data"][194:258],0)/coin_decimals(token1)

            change_capital(bundle, token0, token1)
            
        elif TOPICS_TO_PROCESS[e["topics"][0]] == "collect":
            (token0, token1) = get_two_tokensV3(run_context, e["address"])
            if token0 is None:
                continue
            make_properties(e["topics"][0], transaction, bundle, token0, token1)
            bundle["a_mintBurnV3"] += 1
            add_tokens(bundle, token0, token1)
            update_gas(transaction, bundle)

            bundle["saldo"][token0] += int("0x"+e["data"][66:130],0)/coin_decimals(token0)
            bundle["saldo"][token1] += int("0x"+e["data"][130:194],0)/coin_decimals(token1)

            change_capital(bundle, token0, token1)

        elif TOPICS_TO_PROCESS[e["topics"][0]] == "a_uniswapV2":
            (token0, token1) = get_two_tokensV2(run_context, e["address"])
            if token0 is None:
                continue

            make_properties(e["topics"][0], transaction, bundle, token0, token1)
            bundle[TOPICS_TO_PROCESS[e["topics"][0]]] += 1
            add_tokens(bundle, token0, token1)
            update_gas(transaction, bundle)

            bundle["saldo"][token0] -= int(e["data"][:66],0)/coin_decimals(token0)
            bundle["saldo"][token0] += int("0x"+e["data"][130:194],0)/coin_decimals(token0)
            bundle["saldo"][token1] -= int("0x"+e["data"][66:130],0)/coin_decimals(token1)
            bundle["saldo"][token1] += int("0x"+e["data"][194:258],0)/coin_decimals(token1)
            update_rates(bundle, token0, token1,
                         int("0x"+e["data"][66:130], 0) +  int("0x"+e["data"][194:258], 0),
                         int(e["data"][:66], 0) + int("0x"+e["data"][130:194], 0))
            change_capital(bundle, token0, token1)

        elif TOPICS_TO_PROCESS[e["topics"][0]] in ["a_uniswapV3", "a_pancakeV3"]:
            if TOPICS_TO_PROCESS[e["topics"][0]] == "a_uniswapV3":
                (token0, token1) = get_two_tokensV3(run_context, e["address"])
            else:
                (token0, token1) = get_two_tokens_other(run_context, e["address"])
                
            if token0 is None:
                continue

            make_properties(e["topics"][0], transaction, bundle, token0, token1)
            bundle[TOPICS_TO_PROCESS[e["topics"][0]]] += 1
            add_tokens(bundle, token0, token1, WETH)
            update_gas(transaction, bundle)

            bundle["saldo"][token0] -= s64(int(e["data"][:66],0))/coin_decimals(token0)
            bundle["saldo"][token1] -= s64(int("0x"+e["data"][66:130],0))/coin_decimals(token1)
            update_rates(bundle, token0, token1,
                         s64(int("0x"+e["data"][66:130],0)),
                         s64(int(e["data"][:66],0)))
            change_capital(bundle, token0, token1)

# calculate bundle totals
    output_bundles = {}
    for ii, b in enumerate(processed_bundles):
        bundle = processed_bundles[b]
        
        bundle["a_irreducibleTokens"] = 0
        bundle["a_baseToken"] = None
        if not "rates" in bundle or len(bundle["rates"]) == 0:
            continue
        bundle["capitalRequirements_1"] = {}
        if WETH in bundle["capitalRequirements"]:
            bundle["a_baseToken"] = WETH
        else:
            for st in STABLECOINS:
                if st in bundle["capitalRequirements"]:
                    bundle["a_baseToken"] = st
        if bundle["a_baseToken"] is None:
            continue
        
        for st in STABLECOINS:
            if WETH in bundle["capitalRequirements"] and st in bundle["capitalRequirements"]:
                if find_rate(WETH, st, bundle["rates"]) is None:
                    if WETH < st:
                        bundle["rates"][(WETH, st)] = fixed_weth_rate
                    else:
                        bundle["rates"][(st, WETH)] = 1/fixed_weth_rate
            for st1 in STABLECOINS:
                if st != st1 and st1 in bundle["capitalRequirements"] and st in bundle["capitalRequirements"]:
                    if find_rate(st1, st, bundle["rates"]) is None:
                        bundle["rates"][min(st1, st), max(st1, st)] = 1
        
        bundle["totalCapital"] = 0
        bundle["profitEstimation"] = 0
        for c in bundle["capitalRequirements"]:
            if (c == "eth" or c == WETH) and bundle["a_baseToken"] == WETH:
                rate = 1
            elif bundle["a_baseToken"] == c:
                rate = 1
            else:
                rate = find_rate(bundle["a_baseToken"], c, bundle["rates"])
                if rate is None:
                    bundle["a_irreducibleTokens"] = 1
                    continue
            bundle["capitalRequirements_1"][c] = bundle["capitalRequirements"][c] / rate
            bundle["totalCapital"] += bundle["capitalRequirements_1"][c]
            bundle["profitEstimation"] += bundle["saldo"][c] / rate

        if bundle["a_baseToken"] in STABLECOINS:
            bundle["totalCapital"] = bundle["totalCapital"] / fixed_weth_rate
            bundle["profitEstimation"] = bundle["profitEstimation"] / fixed_weth_rate
        bundle["ETHCapital"] = bundle["capitalRequirements"]["eth"]
        bundle["ETHTotal"] = bundle["saldo"]["eth"]
        max_capitalRequirements = max(list(bundle["capitalRequirements_1"].values()))
        bundle["a_startToken"] = list(bundle["capitalRequirements_1"].keys())[list(bundle["capitalRequirements_1"].values()).index(max_capitalRequirements)]
        bundle["a_complexity"] = len(bundle["txs"])
        bundle["a_N_startTokens"] = len([1 for c in bundle["capitalRequirements"] if bundle["capitalRequirements"][c] > 0 and c != 'eth'])

        bundle["beforeBribes"] = bundle['profitEstimation'] + bundle['directBribe'] + bundle['gasOverpay']
        if bundle["beforeBribes"] > 0:
            bundle["bribesRatio"] = (bundle['directBribe'] + bundle['gasOverpay']) / bundle["beforeBribes"]
        else:
            bundle["bribesRatio"] = None
        output_bundles[b] = bundle
    return output_bundles

def check_attack_class(rules, bundle):
    for r in rules:
        if r in bundle:
            value = bundle[r]
        else:
            value = 0 #!!! may be problematic with string properties
        if rules[r][0] == "EQ":
            if rules[r][1] != value:
                return False
        if rules[r][0] == "NE":
            if rules[r][1] == value:
                return False
        elif rules[r][0] == "GT":
            if rules[r][1] >= value:
                return False
        elif rules[r][0] == "GE":
            if rules[r][1] > value:
                return False
        elif rules[r][0] == "LT":
            if rules[r][1] <= value:
                return False
        elif rules[r][0] == "LE":
            if rules[r][1] < value:
                return False
    return True

@provide_db
def classes_and_emas(bundles, attakers_list, db):
# with RemoteServer(remote=REMOTE) as server:
   # with DBMySQL(port=server.local_bind_port) as db:
    attack_classes = db.get_attack_classes()
    attack_EMAs_list = db.get_attack_EMAs()
            
    attack_EMAs = {(a["attackClassId"], a["attacker"]): a for a in attack_EMAs_list}
    for c in attack_classes:
        c["rules"] = json.loads(c["rules"])

    for b in bundles:
        if not "saldo" in bundles[b] or bundles[b]["saldo"] is None:
            continue
        report_by_attackers = ["*"]
        for a in attakers_list:
            if a["tx_to"] == b[2]:
                if a["report"] in [1, 2]:
                    report_by_attackers.append(b[2])
            else:
                if a["report"] == 2:
                    report_by_attackers.append("~" + a["tx_to"])
               
        for c in attack_classes:
            if not bundles[b]["bribesRatio"] is None and check_attack_class(c["rules"], bundles[b]):
                for a in report_by_attackers:
                    if not (c["attackClassId"], a) in attack_EMAs:
                        attack_EMAs[(c["attackClassId"], a)] = {"countAttacks": 1,
                                                            "lastBlockNumber": None,
                                                           "bribesRatio": None,
                                                           "bribesRatioEMA": None
                                                           }
                    if attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] is None:
                        attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] = bundles[b]["bribesRatio"]
                    else:
                        attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] = (bundles[b]["bribesRatio"] * parameters["EMA_alpha"] + 
                                                        attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] * (1 - parameters["EMA_alpha"]))
                        attack_EMAs[(c["attackClassId"], a)]["countAttacks"] += 1
                    attack_EMAs[(c["attackClassId"], a)]["lastBlockNumber"] = b[0]
                    attack_EMAs[(c["attackClassId"], a)]["bribesRatio"] = bundles[b]["bribesRatio"]

                    db.add_attack(bundles[b]["bundleId"], c["attackClassId"], a, b[0],
                                  bundles[b]["bribesRatio"])

# with RemoteServer(remote=REMOTE) as server:
   # with DBMySQL(port=server.local_bind_port) as db:
        for c in attack_EMAs:
            db.update_attack_EMA(c[0], c[1], attack_EMAs[c]["countAttacks"],
                                 attack_EMAs[c]["lastBlockNumber"],
                                 attack_EMAs[c]["bribesRatio"],
                                 attack_EMAs[c]["bribesRatioEMA"])


def recalc_attacks(start_bundle = None):
    if start_bundle is None:
        s1 = "select blockNUmber from t_bundles"
    else:
        s1 = "select blockNUmber from t_bundles where bundleId >= " + str(start_bundle)
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            attakers_list = db.get_attackers()
            l = db.exec_sql_plain_list(s1)

    all_bundles = {}
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            for ll in l[1]:
                block_bundles = db.get_bundles(ll[0])
                block_bundles = {(b["blockNumber"], b["attacker0"], b["attacker1"]): b for b in block_bundles}
                for b in block_bundles:
                    if not block_bundles[b]["capitalRequirements"] is None:
                        block_bundles[b]["capitalRequirements"] = json.loads(block_bundles[b]["capitalRequirements"])
                    if not block_bundles[b]["saldo"] is None:
                        block_bundles[b]["saldo"] = json.loads(block_bundles[b]["saldo"])
                    if not block_bundles[b]["rates"] is None:
                        block_bundles[b]["rates"] = {(r[0], r[1]): r[2] for r in json.loads(block_bundles[b]["rates"])}
                    if not block_bundles[b]["features"] is None:
                        features = json.loads(block_bundles[b]["features"])
                        block_bundles[b].update(features)
                all_bundles.update(block_bundles)
    print("len(all_bundles)=", len(all_bundles))

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            attack_classes = db.get_attack_classes()
            for c in attack_classes:
                c["rules"] = json.loads(c["rules"])

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            if start_bundle is None:
                s1 = "delete from t_attack_EMAs"
                db.exec_sql(s1)
                attack_EMAs = {}
            else:
                attack_EMAs_list = db.get_attack_EMAs()
                attack_EMAs = {(a["attackClassId"], a["attacker"]): a for a in attack_EMAs_list}

    # recalc attacks and save all to db
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            for b in all_bundles:
                print(all_bundles[b]["bundleId"])
                if not "saldo" in all_bundles[b] or all_bundles[b]["saldo"] is None:
                    continue
                report_by_attackers = ["*"]
                for a in attakers_list:
                    if a["tx_to"] == b[2]:
                        if a["report"] in [1, 2]:
                            report_by_attackers.append(b[2])
                    else:
                        if a["report"] == 2:
                            report_by_attackers.append("~" + a["tx_to"])
                       
                s1 = "delete from t_attacks where bundleId = " + str(all_bundles[b]["bundleId"])
                db.exec_sql(s1)
                for c in attack_classes:
                    if not all_bundles[b]["bribesRatio"] is None and check_attack_class(c["rules"], all_bundles[b]):
                        for a in report_by_attackers:
                            db.add_attack(all_bundles[b]["bundleId"], c["attackClassId"], a, b[0],
                                          all_bundles[b]["bribesRatio"])
                            if not (c["attackClassId"], a) in attack_EMAs:
                                attack_EMAs[(c["attackClassId"], a)] = {"countAttacks": 1,
                                                                    "lastBlockNumber": None,
                                                                   "bribesRatio": None,
                                                                   "bribesRatioEMA": None
                                                                   }
                            if attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] is None:
                                attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] = all_bundles[b]["bribesRatio"]
                            else:
                                attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] = (all_bundles[b]["bribesRatio"] * parameters["EMA_alpha"] + 
                                                                attack_EMAs[(c["attackClassId"], a)]["bribesRatioEMA"] * (1 - parameters["EMA_alpha"]))
                                attack_EMAs[(c["attackClassId"], a)]["countAttacks"] += 1
                            attack_EMAs[(c["attackClassId"], a)]["lastBlockNumber"] = b[0]
                            attack_EMAs[(c["attackClassId"], a)]["bribesRatio"] = all_bundles[b]["bribesRatio"]
                db.commit()

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            for c in attack_EMAs:
                db.update_attack_EMA(c[0], c[1], attack_EMAs[c]["countAttacks"],
                                     attack_EMAs[c]["lastBlockNumber"],
                                     attack_EMAs[c]["bribesRatio"],
                                     attack_EMAs[c]["bribesRatioEMA"])


def recalc_bundles():
    block_number = 19356000
    max_block_number = 19360530

    w3, latest_block, uris = web3connect2(KEY_FILE)
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            attakers_list = db.get_attackers()
    attakers = {}
    multisender_attackers = []
    for a in attakers_list:
        attakers[a["tx_from"], a["tx_to"]] = a["status"]
        if a["tx_from"] is None and a["status"] == 1:
            multisender_attackers.append(a["tx_to"])
 
    abi_storage = {}
    contract_storage = {}
    pairs_VXXX = {}
    run_context = {
                    "w3": w3,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "pairs_VXXX": pairs_VXXX,
                    "contract_storage": contract_storage,
                    "attaker_status": attakers,
                    "multisender_attackers": multisender_attackers,
                    }
    run_context["eth_rate"] = float(etherscan_get_ethusd(run_context["etherscan_key"])["ethusd"])

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            while block_number <= max_block_number:
                print(block_number)
                block_data, transactions, events, bundles = get_block_data(block_number, db=db)
                # output_bundles = process_bundles(run_context, events, transactions, bundles)
                classes_and_emas(bundles, attakers_list, db=db)
                db.commit()
                block_number += 1

def process_historical_blocks(w3, latest_block):

    latest_block_number = latest_block["number"]

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            attakers_list = db.get_attackers()
            prev_block = db.get_blocks_gap(latest_block_number)
    attakers = {}
    multisender_attackers = []
    for a in attakers_list:
        attakers[a["tx_from"], a["tx_to"]] = a["status"]
        if a["tx_from"] is None and a["status"] == 1:
            multisender_attackers.append(a["tx_to"])
 
    abi_storage = {}
    contract_storage = {}
    pairs_VXXX = {}
    run_context = {
                    "w3": w3,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "pairs_VXXX": pairs_VXXX,
                    "contract_storage": contract_storage,
                    "attaker_status": attakers,
                    "multisender_attackers": multisender_attackers,
                    }
    run_context["eth_rate"] = float(etherscan_get_ethusd(run_context["etherscan_key"])["ethusd"])

    # print(latest_block_number)
    block_number = min(prev_block + 1, latest_block_number) if not prev_block is None else latest_block_number
    # block_number = latest_block_number - 5 * 60 * 24 - 38

    # clean_block_data(block_number)

    print(latest_block_number - block_number)
    while block_number <= latest_block_number:
        print(block_number)
        block_data, block_transactions, block_events, block_bundles = process_block(block_number, run_context)

        # events, transactions, bundles = block_events, block_transactions, block_bundles
        # block_data, transactions, events, bundles = get_block_data(19360531)

        output_bundles = process_bundles(run_context, block_events, block_transactions, block_bundles)
        with RemoteServer(remote=REMOTE) as server:
            with DBMySQL(port=server.local_bind_port) as db:
                write_block_transactions(block_data, block_transactions, block_events, output_bundles, db=db)
                update_bundles(output_bundles, db=db)
                classes_and_emas(output_bundles, attakers_list, db=db)
                db.commit()
        block_number += 1

def management():
    
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            db.add_attacker(None, "0xdAC17F958D2ee523a2206206994597C13D831ec7", -1, "USDT")
            
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            # db.add_attacker(None, "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD", -1, "Universal Router")
            # db.add_attacker(None, "0x6F8e33DE59EcaDae8461122b87BdbD7e0A632BeC", -1, "ErcDel")
            # # db.add_attacker(None, "0xbf1BA985CF1692CaD4f1192270f649Bf1355fBfe", -1, "")
            # db.add_attacker(None, "0xC36442b4a4522E871399CD717aBDD847Ab11FE88", -1, "Uniswap V3: Positions NFT")
            # # db.add_attacker(None, "0x364e94A1bF09Fc6498e979e7715Bb13fa6e9F807", -1, "")
                     
            # db.add_attacker("0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13", "0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80", 1, "Jared")
            # db.add_attacker("0xe93685f3bBA03016F02bD1828BaDD6195988D950", "0x902F09715B6303d4173037652FA7377e5b98089E", -1)
            # db.add_attacker("0x2C169DFe5fBbA12957Bdd0Ba47d9CEDbFE260CA7", "0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4", -1)
            # db.add_attacker(None, "0x000000d40B595B94918a28b27d1e2C66F43A51d3", 1, "libmev#1")
            # db.add_attacker(None, "0xB0000000aa4f00aF1200C8B2BefB6300853F0069", 1, "0xB00...0069")
            db.add_attacker(None, "0x00df657Aa9a100A600001700004A00359a639F47", 1, "...F47", 0)

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            db.add_attack_class("Other_start_token", {"a_startToken":["NE", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"],})


            db.add_attack_class("All", {})
            db.add_attack_class("V2_only", {"a_uniswapV2":["GT", 0],
                                            "a_uniswapV3":["EQ", 0]})
            db.add_attack_class("V3_only", {"a_uniswapV2":["EQ", 0],
                                            "a_uniswapV3":["GT", 0],
                                            "a_mintBurnV3":["EQ", 0],
                                            "a_mintBurnNFT":["EQ", 0],})
            db.add_attack_class("mintBurnV3", {"a_mintBurnV3":["GT", 0],
                                            "a_mintBurnNFT":["EQ", 0],})
            db.add_attack_class("mintBurnNFT", {"a_mintBurnV3":["EQ", 0],
                                            "a_mintBurnNFT":["GT", 0],})
            db.add_attack_class("mintBurnV3andNFT", {"a_mintBurnV3":["GT", 0],
                                            "a_mintBurnNFT":["GT", 0],})
            db.add_attack_class("V2_only_notWETH", {"a_uniswapV2":["GT", 0],
                                            "a_uniswapV3":["EQ", 0]})
            db.add_attack_class("V3_only_notWETH", {"a_uniswapV2":["EQ", 0],
                                            "a_uniswapV3":["GT", 0],
                                            "a_mintBurnV3":["EQ", 0],
                                            "a_mintBurnNFT":["EQ", 0],})

def main():
    if len(sys.argv) < 2:
        w3, latest_block, uris = web3connect2(KEY_FILE)
        process_historical_blocks(w3, latest_block)
    elif sys.argv[1] == "recalc" and sys.argv[2] == "attacks":
        recalc_attacks()
    
if __name__ == '__main__':
    # recalc_bundles()
    main()
