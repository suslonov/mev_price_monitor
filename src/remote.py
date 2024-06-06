#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.

import socket
import sshtunnel

class Servers():
    rsynergy2_mysql = {"ssh_host": "ramses.mil.r-synergy.com",
                       "ssh_port": 22,
                       "ssh_username": "anton",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    rsynergy2_mysqlconnect = {"ssh_host": "ramses.mil.r-synergy.com",
                       "ssh_port": 22,
                       "ssh_username": "sqlconnect",
                       "ssh_pkey": "~/.ssh/id_rsa",
                       "local_bind_address": None,
                       "bind_address": '127.0.0.1',
                       "bind_port": 3306,
                       "SSH_TIMEOUT": 30.0}

    aws_ib = {"ssh_host": "10.0.1.153",
              "ssh_port": 22,
              "ssh_username": "ubuntu",
              "ssh_pkey": "~/.ssh/id_rsa",
              "local_bind_address": ('127.0.0.1', None),
              "bind_address": '127.0.0.1',
              "bind_port": 4002,
              "SSH_TIMEOUT": 30.0}

    aws_ib_live = {"ssh_host": "10.0.1.215",
              "ssh_port": 22,
              "ssh_username": "ubuntu",
              "ssh_pkey": "~/.ssh/id_rsa",
              "local_bind_address": ('127.0.0.1', None),
              "bind_address": '127.0.0.1',
              "bind_port": 4001,
              "SSH_TIMEOUT": 30.0}


def _open_remote_port(server_definition):
    if server_definition is None:
        return None, None
    ssh_host = server_definition["ssh_host"]
    ssh_port = server_definition["ssh_port"]
    ssh_username = server_definition["ssh_username"]
    ssh_private_key_password = ""
    ssh_pkey = server_definition["ssh_pkey"]
    sshtunnel.SSH_TIMEOUT = server_definition["SSH_TIMEOUT"]

    if not server_definition["local_bind_address"] is None and server_definition["local_bind_address"][1] is None:
        temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_socket.bind(('', 0))
        addr = temp_socket.getsockname()
        local_bind_address = (server_definition["local_bind_address"][0], addr[1])
        temp_socket.close()
    else:
        local_bind_address = server_definition["local_bind_address"]

    server = sshtunnel.SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_username,
                ssh_private_key_password=ssh_private_key_password,
                ssh_pkey=ssh_pkey,
                # logger=sshtunnel.create_logger(loglevel=1),
                local_bind_address=local_bind_address,
                remote_bind_address=(server_definition["bind_address"], server_definition["bind_port"]))

    server.start()
    if server.is_alive:
        return server, server.local_bind_port
    return None, None

def _close_remote_port(server):
    if not server is None:
        server.stop()

class RemoteServer():
    def __init__(self, server_definition=None, remote="rsynergy2_sqlconnect"):
        if server_definition:
            self.remote_server = server_definition
        elif remote == "rsynergy2_mysql":
            self.remote_server = Servers.rsynergy2_mysql
        elif remote == "aws_215_mysql":
            self.remote_server = Servers.aws_215_mysql
        elif remote == "rsynergy2_sqlconnect":
            self.remote_server = Servers.rsynergy2_mysqlconnect
        elif remote == "aws_ib":
            self.remote_server = Servers.aws_ib
        elif remote == "aws_ib_live":
            self.remote_server = Servers.aws_ib_live
        else:
            self.remote_server = None
        self.server = None
        self.local_bind_port = None

    def __enter__(self):
        self.server, self.local_bind_port = _open_remote_port(self.remote_server)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        _close_remote_port(self.server)

# obsolete functions

def open_remote_port(server_definition=None, remote="rsynergy", host=None, port=None):
    if not server_definition:
        if remote == "rsynergy":
            server_definition = Servers.rsynergy1_mysql
        elif remote == "rsynergy2":
            server_definition = Servers.rsynergy2_mysql
        elif remote == "aws_215_mysql":
            server_definition = Servers.aws_215_mysql
        elif remote == "rsynergy2_sqlconnect":
            server_definition = Servers.rsynergy2_mysqlconnect
        elif remote == "rsynergy_sqlconnect":
            server_definition = Servers.rsynergy_mysqlconnect
        elif remote == "aws_ib":
            server_definition = Servers.aws_ib
        elif remote == "aws_ib_live":
            server_definition = Servers.aws_ib_live

    if not host is None:
        server_definition["ssh_host"] = host
    if not port is None:
        server_definition["bind_port"] = port
    return _open_remote_port(server_definition)

def close_remote_port(server):
    _close_remote_port(server)
