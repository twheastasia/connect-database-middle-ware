#coding=utf-8
__author__ = 'weihai'

import time
import psycopg2
import psycopg2.extras
from libs.my_logs import MyLog

# 最基本的PostgreSQL数据库查询方法
class MyDatabasePostgreSQL():
    # 生成实例时，告诉系统要连接哪个数据库
    def __init__(self, config):
        self.db = None
        self.cursor = None
        self.log = MyLog()
        self.config = config
        self.__connectDB()

    def __connectDB(self):
        try:
            # connect to DB
            self.db = psycopg2.connect(**self.config)
            # create cursor
            self.cursor = self.db.cursor()
            self.log.info("Connect PostgreSQL DB successfully!")
        except Exception as ex:
            self.log.info(f"PostgreSQL connection error: {ex}")

    def executeSQL(self, sql, params=None):
        # executing sql
        self.cursor.execute(sql, params)
        # executing by committing to DB
        self.db.commit()
        return self.cursor
    
    def query(self, sql):
        cursor = self.executeSQL(sql)
        value = cursor.fetchall()
        index = cursor.description
        return index, value

    def closeDB(self):
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()
        self.log.info("PostgreSQL Database closed!")

    def changeTupleToList(self, tuples):
        lists = []
        if tuples and len(tuples) > 0:
            for row in tuples:
                lists.append(list(row))
        return lists

    # 传入环境变量，sql语句， 返回一个字典组成的list
    # flag表示当查询结果只有一行一列时，直接返回这个值
    def queryResults(self, sql, flag=True):
        print(sql)
        self.log.info('SQL: {0}'.format(sql))
        # 根据环境选择连接的数据库
        query_result = []
        column_names, results = self.query(sql)
        column_names_list = self.changeTupleToList(column_names)
        result_list = self.changeTupleToList(results)

        # 当查询结果只有一行一列时，直接返回这个值
        if flag:
            if len(result_list) == 1:
                if len(column_names_list) == 1:
                    return result_list[0][0]
                else:
                    return result_list[0]

        # combined data like this:
        # [{'id': 0, 'value':'xxx'},{'id': 1, 'value':'yyy'},{'id': 2, 'value':'zzz'}]
        for list_cell in result_list:
            rows = {}
            for index in range(0, len(list_cell)):
                rows[column_names_list[index][0]] = list_cell[index]
            query_result.append(rows)
        return query_result

    def batchUpdate(self, sql, update_data):
        self.log.info('template sql: {0}'.format(sql))
        try:
            psycopg2.extras.execute_batch(self.cursor, sql, update_data)
            self.db.commit()
            self.log.info(f"Batch update successful: {len(update_data)} rows")
        except Exception as e:
            self.log.info(f"Batch update error: {e}")
            self.db.rollback()

    # 批量更新
    def updateMany(self, table, header, where, value):
        start_time = time.time()

        # 使用 RealDictCursor 来获取字典结果
        with self.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            # 拼接set语句
            set_str = ",".join([f"{sql}=%s" for sql in header])
            # 拼接where条件
            where_str = " AND ".join([f"{sql}=%s" for sql in where])
            # 拼接整个sql语句
            sql = f"UPDATE {table} SET {set_str} WHERE {where_str}"

            # 执行sql语句
            psycopg2.extras.execute_batch(cursor, sql, value)
            self.db.commit()

        end_time = time.time()
        self.log.info(f"【execute_batch】批量更新:用时{end_time-start_time}")


    # 批量更新（创建临时表更新）
    def updateManyTemp(self, table, header, where, value):
        start_time = time.time()
        temp_table_name = f'{table}_{int(start_time)}_temp'
        with self.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            # 拼接set语句
            set_str = ",".join([f"{table}.{sql}={temp_table_name}.{sql}" for sql in header])
            # 拼接where条件
            where_str = " AND ".join([f"{table}.{sql}={temp_table_name}.{sql}" for sql in where])
            # 拼接整个sql语句

            # 创建临时表 (PostgreSQL使用TEMP TABLE)
            sql_temp = f"""
            CREATE TEMP TABLE {temp_table_name} AS SELECT {','.join(where + header)} FROM {table} LIMIT 0
            """
            # 插入数据到临时表
            # PostgreSQL需要使用不同的语法
            placeholders = ','.join(['%s'] * len(header + where))
            sql_insert = f"""
            INSERT INTO {temp_table_name} ({','.join(header + where)}) VALUES ({placeholders})
            """
            # 连表更新正式表
            sql_update = f"""
            UPDATE {table} SET {set_str} FROM {temp_table_name} WHERE {where_str}
            """

            drop_table = f"""DROP TABLE IF EXISTS {temp_table_name}"""

            # 执行sql语句
            cursor.execute(sql_temp)
            psycopg2.extras.execute_batch(cursor, sql_insert, value)
            cursor.execute(sql_update)
            cursor.execute(drop_table)
            self.db.commit()

        end_time = time.time()
        self.log.info(f"【创建临时表 】批量更新:用时{end_time-start_time}")
