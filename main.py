#coding=utf-8
__author__ = 'weihai'

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, APIRouter
from pydantic import BaseModel, Field
from libs.my_mysql import MyDatabaseMysql
from libs.my_tdengine import MyDatabaseTDEngine
from libs.my_clickhouse import MyDatabaseClickHouse
from libs.my_redis import MyDatabaseRedis
from libs.my_mongodb import MyDatabaseMongodb
from libs.my_logs import MyLog
from libs import utils
from enum import Enum
import datetime

app = FastAPI()
router = APIRouter()
app.add_middleware(
	CORSMiddleware,
	# 允许跨域的源列表，例如 ['http://www.example.org'] 等等，['*'] 表示允许任何源
	allow_origins=['*'],
	# 跨域请求是否支持 cookie，默认是 False，如果为 True，allow_origins 必须为具体的源，不可以是 ['*']
	allow_credentials=False,
	# 允许跨域请求的 HTTP 方法列表，默认是 ['GET']
	allow_methods=['*'],
	# 允许跨域请求的 HTTP 请求头列表，默认是 []，可以使用 ['*'] 表示允许所有的请求头
	# 当然 Accept、Accept-Language、Content-Language 以及 Content-Type 总之被允许的
	allow_headers=['*'],
	# 可以被浏览器访问的响应头, 默认是 []，一般很少指定
	# expose_headers=['*']
	# 设定浏览器缓存 CORS 响应的最长时间，单位是秒。默认为 600，一般也很少指定
	# max_age=1000
)

RES_MSG_SUCCESS = { 'code': 200, 'message': 'Success', 'data': {} }
RES_MSG_FAIL = { 'code': 500, 'message': 'Fail', 'data': {} }
ENV = {
    'dev': 'dev',
    'test': 'test',
    'staging-cn': 'staging-cn',
    'staging-sg': 'staging-sg',
    'staging-fr': 'staging-fr',
    'staging-us': 'staging-us',
    'prod-cn': 'prod-cn',
    'prod-sg': 'prod-sg',
    'prod-fr': 'prod-fr',
    'prod-us': 'prod-us',
}
fastapi_log = MyLog()

# ----------------- model -----------------
class RouterTags(str, Enum):
    DATABASE = 'database'
    MYSQL = 'mysql'
    CLICKHOUSE = 'clickhouse'
    TDENGINE = 'tdengine'
    REDIS = 'redis'
    MONGODB = 'mongodb'

class MysqlConfig(BaseModel):
    host: str = Field(
        default='localhost', title="Mysql host", max_length=500
    )
    port: int = Field(
        default=3306, title="Mysql port"
    )
    user: str = Field(
        default='root', title="Mysql user", max_length=500
    )
    password: str = Field(
        default='root', title="Mysql password", max_length=500
    )
    db: str = Field(
        default=None, title="Mysql database", max_length=500
    )

class TDEngineConfig(BaseModel):
    host: str = Field(
        default='localhost', title="TDEngine host", max_length=500
    )
    port: int = Field(
        default=6041, title="TDEngine port"
    )
    user: str = Field(
        default='', title="TDEngine user", max_length=500
    )
    password: str = Field(
        default='root', title="TDEngine password", max_length=500
    )

class RedisConfig(BaseModel):
    host: str = Field(
        default='localhost', title="Redis host", max_length=500
    )
    port: int = Field(
        default=6379, title="Redis port"
    )
    db: int = Field(
        default=0, title="Redis db"
    )
    password: str = Field(
        default=None, title="Redis password", max_length=500
    )

class mongoDBConfig(BaseModel):
    host: str = Field(
        default='localhost', title="MongoDB host", max_length=500
    )
    port: int = Field(
        default=27017, title="MongoDB port"
    )
    username: str = Field(
        default=None, title="MongoDB username", max_length=500
    )
    password: str = Field(
        default=None, title="MongoDB password", max_length=500
    )
    database: str = Field(
        default='productname', title="MongoDB database", max_length=500
    )

class MysqlRequestBody(BaseModel):
    config: MysqlConfig = Field(
        default=None, title="Mysql config",
    )
    sql: str = Field(
        default=None, title="Mysql sql",
    )

class TDEngineRequestBody(BaseModel):
    config: TDEngineConfig = Field(
        default=None, title="TDEngine config",
    )
    sql: str = Field(
        default=None, title="TDEngine sql",
    )

class ClickHouseDatetimeColumn(BaseModel):
    index: int = Field(
        default=None, title="index",
    )
    format: str = Field(
        default=None, title="format",
    )

class ClickHouseInsertConfig(BaseModel):
    config: MysqlConfig = Field(
        default=None, title="Mysql config",
    )
    table: str = Field(
        default=None, title="table",
    )
    data: list = Field(
        default=None, title="data",
    )
    columns: list = Field(
        default=None, title="columns",
    )
    # datetime_index: ClickHouseDatetimeColumn = Field(
    #     default=None, title="datetime_index",
    # )
    datetime_index: list = Field(
        default=None, title="datetime_index",
    )

class RedisRequestBody(BaseModel):
    config: RedisConfig = Field(
        default=None, title="Redis config",
    )
    sql: str = Field(
        default=None, title="Redis sql",
    )

class RedisGetValueByKeyRequestBody(BaseModel):
    config: RedisConfig = Field(
        default=None, title="Redis config",
    )
    key: str = Field(
        default=None, title="Redis key",
    )

class RedisSetValueRequestBody(BaseModel):
    config: RedisConfig = Field(
        default=None, title="Redis config",
    )
    key: str = Field(
        default=None, title="Redis key",
    )
    value: str = Field(
        default=None, title="Redis value",
    )

class MongodbFindOneRequestBody(BaseModel):
    config: mongoDBConfig = Field(
        default=None, title="MongoDB config",
    )
    table: str = Field(
        default=None, title="MongoDB table",
    )
    query: dict = Field(
        default=None, title="MongoDB sql",
    )
    projection: dict = Field(
        default=None, title="MongoDB projection",
    )
    columns_need_convert_to_str: list = Field(
        default=['_id'], title="MongoDB which columns need convert to str",
    )

class MongodbFindManyRequestBody(BaseModel):
    config: mongoDBConfig = Field(
        default=None, title="MongoDB config",
    )
    table: str = Field(
        default=None, title="MongoDB table",
    )
    query: dict = Field(
        default=None, title="MongoDB sql",
    )
    projection: dict = Field(
        default=None, title="MongoDB projection",
    )
    sort: list = Field(
        default=None, title="MongoDB sort",
    )
    limit: int = Field(
        default=0, title="MongoDB limit",
    )
    skip: int = Field(
        default=0, title="MongoDB skip",
    )
    columns_need_convert_to_str: list = Field(
        default=[], title="MongoDB which columns need convert to str",
    )

class MongodbInsertRequestBody(BaseModel):
    config: mongoDBConfig = Field(
        default=None, title="MongoDB config",
    )
    table: str = Field(
        default=None, title="MongoDB table",
    )
    data: dict = Field(
        default=None, title="MongoDB data",
    )

class MongodbInsertManyRequestBody(BaseModel):
    config: mongoDBConfig = Field(
        default=None, title="MongoDB config",
    )
    table: str = Field(
        default=None, title="MongoDB table",
    )
    data: list = Field(
        default=None, title="MongoDB data",
    )

class MongodbUpdateOneRequestBody(BaseModel):
    config: mongoDBConfig = Field(
        default=None, title="MongoDB config",
    )
    table: str = Field(
        default=None, title="MongoDB table",
    )
    query: dict = Field(
        default=None, title="MongoDB query",
    )
    data: dict = Field(
        default=None, title="MongoDB data",
    )

# ----------------- basic logic -----------------
def response_success(data):
    res = RES_MSG_SUCCESS
    res['data'] = data
    return res

def response_fail(data):
    res = RES_MSG_FAIL
    res['data'] = data
    return res

# ----------------- business logic -----------------
@router.post('/api/v1/database/mysql/query', description='Get Data From Mysql', name='Mysql Middleware API', tags=[RouterTags.MYSQL])
async def query_mysql(body: MysqlRequestBody):
    fastapi_log.debug(body)
    m_mysql = MyDatabaseMysql({
        'host': body.config.host,
        'port': body.config.port,
        'user': body.config.user,
        'password': body.config.password,
        'db': body.config.db,
        'charset': 'utf8'
    })
    try:
        result = m_mysql.queryResults(body.sql)
    finally:
        m_mysql.closeDB()
    return response_success({ 'sql': body.sql, 'result': result })

@router.post('/api/v1/database/td/query', description='Get Data From TDEngine', name='TDEngine Middleware API', tags=[RouterTags.TDENGINE])
async def query_td(body: TDEngineRequestBody):
    fastapi_log.debug(body)
    td = MyDatabaseTDEngine({
        'url': body.config.host,
        'port': body.config.port,
        'username': body.config.user,
        'password': body.config.password
    })
    try:
        result = td.query(body.sql)
    finally:
        td.close()
    return response_success({ 'sql': body.sql, 'result': result })

@router.post('/api/v1/database/clickhouse/query', description='Get Data From Clickhouse', name='Clickhouse Middleware API', tags=[RouterTags.CLICKHOUSE])
async def query_clickhouse(body: MysqlRequestBody):
    fastapi_log.debug(body)
    my_clickhouse = MyDatabaseClickHouse({
        'host': body.config.host,
        'port': body.config.port,
        'user': body.config.user,
        'password': body.config.password,
        'database': body.config.db
    })
    try:
        result = my_clickhouse.query(body.sql)
    finally:
        my_clickhouse.close()
    return response_success({ 'sql': body.sql, 'result': result })

@router.post('/api/v1/database/clickhouse/insert', description='Insert Data From Clickhouse', name='Clickhouse Middleware Insert API', tags=[RouterTags.CLICKHOUSE])
async def query_clickhouse(body: ClickHouseInsertConfig):
    fastapi_log.debug(body)
    my_clickhouse = MyDatabaseClickHouse({
        'host': body.config.host,
        'port': body.config.port,
        'user': body.config.user,
        'password': body.config.password,
        'database': body.config.db
    })
    try:
        # 转化时间格式
        for row in body.data:
            for index in range(len(body.datetime_index)):
                row[body.datetime_index[index]['index']] = datetime.datetime.strptime(row[body.datetime_index[index]['index']], body.datetime_index[index]['format'])
        result = my_clickhouse.insert_data(body.table, body.data, body.columns)
    finally:
        my_clickhouse.close()
    return response_success({ 'result': result })

# dropped
# @router.post('/api/v1/database/cassandra', description='Get Data From Cassandra', name='Cassandra Middleware API', tags=[RouterTags['database']])
# async def query_cassandra(env: str, sql: str):
#     return response_success({ 'env': env, 'sql': sql })

@router.post('/api/v1/database/redis/query', description='Execute SQL From Redis', name='Redis Middleware Query API', tags=[RouterTags.REDIS])
async def query_redis(body: RedisRequestBody):
    fastapi_log.debug(body)
    fastapi_log.debug(body.config)
    fastapi_log.debug(body.config.host)
    my_redis = MyDatabaseRedis(host=body.config.host, port=body.config.port, db=body.config.db, password=body.config.password)
    try:
        result = my_redis.query(body.sql)
    finally:
        my_redis.close()
    return response_success({ 'sql': body.sql, 'result': result })

@router.post('/api/v1/database/redis/get-value', description='Get Data From Redis', name='Redis Middleware Get Value API', tags=[RouterTags.REDIS])
async def query_redis_get_value_by_key(body: RedisGetValueByKeyRequestBody):
    fastapi_log.debug(body)
    my_redis = MyDatabaseRedis(host=body.config.host, port=body.config.port, db=body.config.db, password=body.config.password)
    try:
        result = my_redis.get_value(body.key)
    finally:
        my_redis.close()
    return response_success({ 'key': body.key, 'result': result })

@router.post('/api/v1/database/redis/set-value', description='Set Data From Redis', name='Redis Middleware Set Value API', tags=[RouterTags.REDIS])
async def query_redis_set_value(body: RedisSetValueRequestBody):
    fastapi_log.debug(body)
    my_redis = MyDatabaseRedis(host=body.config.host, port=body.config.port, db=body.config.db, password=body.config.password)
    try:
        result = my_redis.set_value(body.key, body.value)
    finally:
        my_redis.close()
    return response_success({ 'key': body.key, 'result': result })

@router.post('/api/v1/database/redis/delete-key', description='Delete Key From Redis', name='Redis Middleware Delete Key API', tags=[RouterTags.REDIS])
async def query_redis_delete_key(body: RedisGetValueByKeyRequestBody):
    fastapi_log.debug(body)
    my_redis = MyDatabaseRedis(host=body.config.host, port=body.config.port, db=body.config.db, password=body.config.password)
    try:
        result = my_redis.delete_key(body.key)
    finally:
        my_redis.close()
    return response_success({ 'key': body.key, 'result': result })

@router.post('/api/v1/database/mongodb/find-one', description='Find One From MongoDB', name='MongoDB Middleware Find One API', tags=[RouterTags.MONGODB])
async def query_mongodb_find_one(body: MongodbFindOneRequestBody):
    fastapi_log.debug(body)
    my_mongodb = MyDatabaseMongodb(host=body.config.host, port=body.config.port, username=body.config.username, password=body.config.password, database=body.config.database)
    try:
        result = my_mongodb.find_one(body.table, body.query, body.projection)
        for column in body.columns_need_convert_to_str:
            result[column] = str(result[column])
    finally:
        my_mongodb.close()
    return response_success({ 'table': body.table, 'query': body.query, 'projection': body.projection, 'result': result })

@router.post('/api/v1/database/mongodb/find-many', description='Find Many From MongoDB', name='MongoDB Middleware Find Many API', tags=[RouterTags.MONGODB])
async def query_mongodb_find_many(body: MongodbFindManyRequestBody):
    fastapi_log.debug(body)
    my_mongodb = MyDatabaseMongodb(host=body.config.host, port=body.config.port, username=body.config.username, password=body.config.password, database=body.config.database)
    try:
        result = my_mongodb.find_many(body.table, body.query, body.projection, sort=body.sort, limit=body.limit, skip=body.skip)
        for column in body.columns_need_convert_to_str:
            for row in result:
                row[column] = str(row[column])
    finally:
        my_mongodb.close()
    return response_success({ 'table': body.table, 'query': body.query, 'projection': body.projection, 'result': result })

@router.post('/api/v1/database/mongodb/insert-one', description='Insert One From MongoDB', name='MongoDB Middleware Insert One API', tags=[RouterTags.MONGODB])
async def query_mongodb_insert_one(body: MongodbInsertRequestBody):
    fastapi_log.debug(body)
    my_mongodb = MyDatabaseMongodb(host=body.config.host, port=body.config.port, username=body.config.username, password=body.config.password, database=body.config.database)
    try:
        data = utils.deep_copy(body.data)
        result = my_mongodb.insert_one(body.table, data)
    finally:
        my_mongodb.close()
    print(body.data)
    return response_success({ 'table': body.table, 'data': body.data, 'result': result })

@router.post('/api/v1/database/mongodb/insert-many', description='Insert Many From MongoDB', name='MongoDB Middleware Insert Many API', tags=[RouterTags.MONGODB])
async def query_mongodb_insert_many(body: MongodbInsertManyRequestBody):
    fastapi_log.debug(body)
    my_mongodb = MyDatabaseMongodb(host=body.config.host, port=body.config.port, username=body.config.username, password=body.config.password, database=body.config.database)
    try:
        data = utils.deep_copy(body.data)
        result = my_mongodb.insert_many(body.table, data)
    finally:
        my_mongodb.close()
    print(body.data)
    return response_success({ 'table': body.table, 'data': body.data, 'result': result })

@router.post('/api/v1/database/mongodb/update-one', description='Update One From MongoDB', name='MongoDB Middleware Update One API', tags=[RouterTags.MONGODB])
async def query_mongodb_update_one(body: MongodbUpdateOneRequestBody):
    fastapi_log.debug(body)
    my_mongodb = MyDatabaseMongodb(host=body.config.host, port=body.config.port, username=body.config.username, password=body.config.password, database=body.config.database)
    try:
        data = utils.deep_copy(body.data)
        result = my_mongodb.update_one(body.table, body.query, data)
    finally:
        my_mongodb.close()
    return response_success({ 'table': body.table, 'query': body.query, 'data': body.data, 'result': result })

@router.post('/api/v1/database/mongodb/update-many', description='Update Many From MongoDB', name='MongoDB Middleware Update Many API', tags=[RouterTags.MONGODB])
async def query_mongodb_update_many(body: MongodbUpdateOneRequestBody):
    fastapi_log.debug(body)
    my_mongodb = MyDatabaseMongodb(host=body.config.host, port=body.config.port, username=body.config.username, password=body.config.password, database=body.config.database)
    try:
        data = utils.deep_copy(body.data)
        result = my_mongodb.update_many(body.table, body.query, data)
    finally:
        my_mongodb.close()
    return response_success({ 'table': body.table, 'query': body.query, 'data': body.data, 'result': result })

@router.post('/api/v1/database/mongodb/delete-many', description='Delete Many From MongoDB', name='MongoDB Middleware Delete Many API', tags=[RouterTags.MONGODB])
async def query_mongodb_delete_many(body: MongodbInsertRequestBody):
    fastapi_log.debug(body)
    my_mongodb = MyDatabaseMongodb(host=body.config.host, port=body.config.port, username=body.config.username, password=body.config.password, database=body.config.database)
    try:
        data = utils.deep_copy(body.data)
        result = my_mongodb.delete_many(body.table, data)
    finally:
        my_mongodb.close()
    return response_success({ 'table': body.table, 'data': body.data, 'result': result })


app.include_router(router)
