#!/usr/bin/env python3
# -*- coding: utf-8 -*-

token_abi = [
  {
    "constant": True,
    "inputs": [],
    "name": "name",
    "outputs": [
      {
        "name": "",
        "type": "string"
      }
    ],
    "payable": False,
    "type": "function"
  },
  {
    "constant": False,
    "inputs": [
      {
        "name": "_spender",
        "type": "address"
      },
      {
        "name": "_value",
        "type": "uint256"
      }
    ],
    "name": "approve",
    "outputs": [],
    "payable": False,
    "type": "function"
  },
  {
    "constant": True,
    "inputs": [],
    "name": "totalSupply",
    "outputs": [
      {
        "name": "",
        "type": "uint256"
      }
    ],
    "payable": False,
    "type": "function"
  },
  {
    "constant": False,
    "inputs": [
      {
        "name": "_from",
        "type": "address"
      },
      {
        "name": "_to",
        "type": "address"
      },
      {
        "name": "_value",
        "type": "uint256"
      }
    ],
    "name": "transferFrom",
    "outputs": [],
    "payable": False,
    "type": "function"
  },
  {
    "constant": True,
    "inputs": [],
    "name": "decimals",
    "outputs": [
      {
        "name": "",
        "type": "uint256"
      }
    ],
    "payable": False,
    "type": "function"
  },
  {
    "constant": True,
    "inputs": [
      {
        "name": "_who",
        "type": "address"
      }
    ],
    "name": "balanceOf",
    "outputs": [
      {
        "name": "balance",
        "type": "uint256"
      }
    ],
    "payable": False,
    "type": "function"
  },
  {
    "constant": True,
    "inputs": [],
    "name": "symbol",
    "outputs": [
      {
        "name": "",
        "type": "string"
      }
    ],
    "payable": False,
    "type": "function"
  },
  {
    "constant": False,
    "inputs": [
      {
        "name": "_to",
        "type": "address"
      },
      {
        "name": "_value",
        "type": "uint256"
      }
    ],
    "name": "transfer",
    "outputs": [],
    "payable": False,
    "type": "function"
  },
  {
    "constant": True,
    "inputs": [
      {
        "name": "_owner",
        "type": "address"
      },
      {
        "name": "_spender",
        "type": "address"
      }
    ],
    "name": "allowance",
    "outputs": [
      {
        "name": "remaining",
        "type": "uint256"
      }
    ],
    "payable": False,
    "type": "function"
  },
  {
    "anonymous": False,
    "inputs": [
      {
        "indexed": True,
        "name": "from",
        "type": "address"
      },
      {
        "indexed": True,
        "name": "to",
        "type": "address"
      },
      {
        "indexed": False,
        "name": "value",
        "type": "uint256"
      }
    ],
    "name": "Transfer",
    "type": "event"
  },
  {
    "anonymous": False,
    "inputs": [
      {
        "indexed": True,
        "name": "owner",
        "type": "address"
      },
      {
        "indexed": True,
        "name": "spender",
        "type": "address"
      },
      {
        "indexed": False,
        "name": "value",
        "type": "uint256"
      }
    ],
    "name": "Approval",
    "type": "event"
  },

   {"inputs": [],
    "name": "_maxTaxSwap",
    "outputs": [
        {"internalType": "uint256",
        "name": "",
        "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"},
    {"inputs": [],
    "name": "_maxTxAmount",
    "outputs": [
        {"internalType": "uint256",
        "name": "",
        "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"},
    {"inputs": [],
    "name": "_maxWalletSize",
    "outputs": [
        {"internalType": "uint256",
        "name": "",
        "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"},
    {"inputs": [],
    "name": "_taxSwapThreshold",
    "outputs": [
        {"internalType": "uint256",
        "name": "",
        "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"},

    {"inputs": [],
     "name": "_maxWalletToken",
     "outputs": [
         {"internalType": "uint256", 
          "name": "",
          "type": "uint256"}
    ],
    "stateMutability": "view",
    "type": "function"},
    
]
