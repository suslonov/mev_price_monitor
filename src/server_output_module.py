#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import MySQLdb

# from remote import RemoteServer
# REMOTE = "rsynergy2_sqlconnect"

class DBMySQL(object):
    db_host="127.0.0.1"
    db_user="mev_price_monitor"
    db_passwd="mev_price_monitor"
    db_name="mev_price_monitor"

    def __init__(self, port=None):
        self.port = port
        pass

    def fetch_with_description(self, cursor):
        return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]

    def start(self):
        if self.port:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name, port=self.port)
        else:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name)
        self.cursor = self.db_connection.cursor()

    def stop(self):
        self.db_connection.commit()
        self.db_connection.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def get_monitor_output(self):
        s1 = "select t_attack_EMAs.attackClassId, attackClass, attacker, countAttacks, lastBlockNumber, bribesRatio lastBribesRatio, "
        s1 += "bribesRatioEMA from t_attack_EMAs inner join t_attack_classes on t_attack_EMAs.attackClassId=t_attack_classes.attackClassId"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)
    
    def get_one_attack_history(self, attack_class_id, attacker, limit=1000):
        s1 = "select blockNumber, bribesRatio from t_attacks where attackClassId=%s and attacker=%s order by blockNumber desc limit " + str(limit)
        self.cursor.execute(s1, (attack_class_id,  attacker))
        l = list(self.cursor.fetchall())
        l.reverse()
        return l

def monitor_output1():
    with DBMySQL() as db:
        attack_summary_list = db.get_monitor_output()

    attack_classes = {}
    for a in attack_summary_list:
        if not a["attackClass"] in attack_classes:
            attack_classes[a["attackClass"]] = {}
        attack_classes[a["attackClass"]][a["attacker"]] = {'countAttacks': a['countAttacks'],
                                                           'lastBlockNumber': a['lastBlockNumber'],
                                                           'lastBribesRatio': a['lastBribesRatio'],
                                                           'bribesRatioEMA': a['bribesRatioEMA']}
    return attack_classes

def monitor_output2(row=None, limit=1000):
# with RemoteServer(remote=REMOTE) as server:
    # with DBMySQL(port=server.local_bind_port) as db:
    with DBMySQL() as db:
        attack_summary_list = db.get_monitor_output()
        attack_summary_table = [[0] + [i for i in attack_summary_list[0]][1:]]
        for i, j in enumerate(attack_summary_list):
            attack_summary_table.append([i + 1,
                                        j["attackClass"],
                                        j["attacker"],
                                        j["countAttacks"],
                                        j["lastBlockNumber"],
                                        "{:0.3%}".format(j["lastBribesRatio"]),
                                        "{:0.3%}".format(j["bribesRatioEMA"])])
        
        if row is None:
            return attack_summary_table, []

# with RemoteServer(remote=REMOTE) as server:
    # with DBMySQL(port=server.local_bind_port) as db:
        one_attack_type_line = db.get_one_attack_history(attack_summary_list[row-1]["attackClassId"],
                                                  attack_summary_list[row-1]["attacker"], limit=limit)
        return attack_summary_table, one_attack_type_line
        
        