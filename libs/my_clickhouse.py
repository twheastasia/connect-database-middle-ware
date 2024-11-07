# -*- coding: utf-8 -*-
__author__ = 'weihai'

import clickhouse_connect
from libs.my_logs import MyLog

# 基于Clickhouse数据库基础数据对象类
class MyDatabaseClickHouse(object):
    def __init__(self, config):
        self.client = clickhouse_connect.get_client(host=config['host'], port=config['port'], username=config['user'], password=config['password'], database=config['database'])
        self.log = MyLog()
        self.log.debug("Connect Clickhouse successfully!")
          
    def query(self, sql):
        result = []
        res = self.client.query(sql)
        for row in res.result_rows:
            temp_result = {}
            for index in range(len(res.column_names)):
                temp_result[res.column_names[index]] = row[index]
            result.append(temp_result)
        return result
    
    def insert_data(self, table, data, columns):
        # row1 = [1000, 'String Value 1000', 5.233]
        # row2 = [2000, 'String Value 2000', -107.04]
        # data = [row1, row2]
        # client.insert('new_table', data, column_names=['key', 'value', 'metric'])
        self.client.insert(table, data, column_names=columns)

    def execute_command(self, sql):
        self.client.command(sql)
    
    def close(self):
        self.client.close()