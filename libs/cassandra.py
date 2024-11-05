# -*- coding:utf8 -*-
from time import sleep
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_NONE
from cassandra.query import SimpleStatement
from libs import utils
from libs.logs import MyLog
import datetime

class MyCassandra():
    def __init__(self, env) -> None:
        self.log = MyLog()
        config_json = utils.read_json('./config/db.json')
        self.cassandra_to_config = config_json[env]
        self.session = self._connect_cassandra(self.cassandra_to_config['urls'], self.cassandra_to_config['port'], self.cassandra_to_config['keyspace'], self.cassandra_to_config['username'], self.cassandra_to_config['password'], self.cassandra_to_config['useSSL'])
        # sql = sql.format(keyspace=cassandra_to_config['keyspace'])
        # results = query_result_by_page(session, sql)
        # session.shutdown()

    def _connect_cassandra(self, server_urls, server_port, keyspace_name, username, password, useSSL):
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        ssl_context.verify_mode = CERT_NONE
        
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        # 获取集群
        if useSSL:
            ster = Cluster(contact_points=server_urls, port=server_port, auth_provider = auth_provider, connect_timeout=6000, load_balancing_policy=RoundRobinPolicy(), ssl_context=ssl_context)
        else:
            ster = Cluster(contact_points=server_urls, port=server_port, auth_provider = auth_provider, connect_timeout=6000, load_balancing_policy=RoundRobinPolicy())
        
        ster.schema_metadata_enabled = False
        ster.token_metadata_enabled = False
        # 连接会话
        session = ster.connect(keyspace=keyspace_name)
        self.log.info('Connect successfully!')
        return session

    # 分页查询，返回所有
    def query_result_by_page(self, sql, page_size=100):
        sql = sql.format(keyspace=self.cassandra_to_config['keyspace'])
        count = page_size
        results = []
        while count == page_size:
            statement = SimpleStatement(sql, fetch_size=page_size)
            # 分页参考 https://docs.datastax.com/en/developer/python-driver/3.21/query_paging/
            try:
                rows = self.session.execute(statement, paging_state=paging_state)
            except NameError:
                # 第一次循环没有paging state时，走这里
                rows = self.session.execute(statement)
            finally:
                paging_state = rows.paging_state
                sleep(1)
            results += rows
            count = len(rows.current_rows)
        self.log.info('SQL: {0}, result count is: {1}'.format(sql, len(results)))
        return results

    # 按时间范围，分页查询，返回所有
    def query_by_datetime(self, sql, start, end, page_size=100):
        sql = sql.format(keyspace=self.cassandra_to_config['keyspace'])
        count = page_size
        results = []
        while count == page_size:
            excute_sql = """{0} and data_time >= '{1}' and data_time < '{2}' order by data_time limit {3} ALLOW FILTERING""".format(sql, start, end, page_size)
            statement = SimpleStatement(excute_sql, fetch_size=page_size)
            rows = self.session.execute(statement)
            count = len(rows.current_rows)
            self.log.info('SQL: {0}, result count is: {1}'.format(excute_sql, count))
            if count != 0:
                results.extend(rows.current_rows)
                # 因为查询语句里是>=，怕下一次查询时有重复
                next_time = results[-1].data_time + datetime.timedelta(seconds=1)
                start = next_time.strftime('%Y-%m-%d %H:%M:%S')
            sleep(1)

        self.log.info('Total SQL: {0}, total count is: {1}'.format(sql, len(results)))
        return results
    
    def close(self):
        self.log.info('Cassandra Closed!')
        if self.session:
            self.session.shutdown()

