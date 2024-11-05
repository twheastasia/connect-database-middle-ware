#coding=utf-8
__author__ = 'weihai'

import datetime
import time
from taosrest import connect, RestClient, TaosRestConnection, TaosRestCursor
from libs.logs import MyLog

class MyDatabaseTDEngine():

    def __init__(self, db_config) -> None:
        db_url = 'http://{0}:{1}'.format(db_config['url'], db_config['port'])
        self.log = MyLog()
        self.conn = connect(url=db_url, user=db_config['username'], password=db_config['password'], timezone='utc', timeout=30)
        self.log.info('Connect TDEngine successfully!')

    # 基本查询方法
    def query(self, sql):
        result = {}
        if isinstance(sql, str) and len(sql) > 0:
            cursor = self.conn.cursor()
            # query data
            cursor.execute(sql)
            # get total rows
            result['total'] = cursor.rowcount
            # get column names from cursor
            column_names = [meta[0] for meta in cursor.description]
            result['columns'] = column_names
            # get rows
            result['data'] = []
            data = cursor.fetchall()
            for row in data:
                row_obj = {}
                for index in range(len(row)):
                    if isinstance(row[index], datetime.datetime):
                        timestamp = row[index].strftime('%Y-%m-%d %H:%M:%S')
                        row_obj[column_names[index]] = timestamp
                    else:
                        row_obj[column_names[index]] = row[index]
                result['data'].append(row_obj)
            self.log.info('Query SQL: {0}, result count is: {1}'.format(sql, result['total']))
        return result
    
    # 按时间分页查询记录
    def query_by_datetime(self, sql, start, end, page_size=200):
        count = page_size
        results = {
            'total': 0,
            'data': []
        }
        while count == page_size:
            excute_sql = """{0} and ts >= '{1}' and ts < '{2}' order by ts limit {3}""".format(sql, start, end, page_size)
            res1 = self.query(excute_sql)
            # 看一下查询的结果有多少条记录
            count = res1['total']
            results['total'] += res1['total']
            if 'columns' not in results:
                results['columns'] = res1['columns']
            if count != 0:
                results['data'].extend(res1['data'])
                # 因为查询语句里是>=，怕下一次查询时有重复
                next_time = datetime.datetime.strptime(results['data'][-1]['ts'], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(seconds=1)
                start = next_time.strftime('%Y-%m-%d %H:%M:%S')
            time.sleep(1)

        self.log.info('Total SQL: {0}, total count is: {1}'.format(sql, results['total']))
        return results
    
    def close(self):
        self.log.info('TDEngine Closed!')
        if self.conn:
            self.conn.close()
