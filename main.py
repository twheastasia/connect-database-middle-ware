#coding=utf-8
__author__ = 'weihai'

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, APIRouter
from pydantic import BaseModel, Field
from libs import mysql
from libs import tdengine
from libs.logs import MyLog
from libs import clickhouse
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
ROUTER_TAGS = {
    'database': 'database'
}
fastapi_log = MyLog()

# ----------------- model -----------------
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
        default='a3d', title="TDEngine user", max_length=500
    )
    password: str = Field(
        default='root', title="TDEngine password", max_length=500
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
@router.post('/api/v1/database/mysql', description='Get Data From Mysql', name='Mysql Middleware API', tags=[ROUTER_TAGS['database']])
async def query_mysql(body: MysqlRequestBody):
    fastapi_log.debug(body)
    m_mysql = mysql.MyDatabaseMysql({
        'host': body.config.host,
        'port': body.config.port,
        'user': body.config.user,
        'password': body.config.password,
        'db': body.config.db,
        'charset': 'utf8'
    })
    result = m_mysql.queryResults(body.sql)
    m_mysql.closeDB()
    return response_success({ 'sql': body.sql, 'result': result })

@router.post('/api/v1/database/td', description='Get Data From TDEngine', name='TDEngine Middleware API', tags=[ROUTER_TAGS['database']])
async def query_td(body: TDEngineRequestBody):
    fastapi_log.debug(body)
    td = tdengine.MyDatabaseTDEngine({
        'url': body.config.host,
        'port': body.config.port,
        'username': body.config.user,
        'password': body.config.password
    })
    result = td.query(body.sql)
    td.close()
    return response_success({ 'sql': body.sql, 'result': result })

@router.post('/api/v1/database/clickhouse/query', description='Get Data From Clickhouse', name='Clickhouse Middleware API', tags=[ROUTER_TAGS['database']])
async def query_clickhouse(body: MysqlRequestBody):
    fastapi_log.debug(body)
    my_clickhouse = clickhouse.MyDatabaseClickHouse({
        'host': body.config.host,
        'port': body.config.port,
        'user': body.config.user,
        'password': body.config.password,
        'database': body.config.db
    })
    result = my_clickhouse.query(body.sql)
    my_clickhouse.close()
    return response_success({ 'sql': body.sql, 'result': result })

@router.post('/api/v1/database/clickhouse/insert', description='Insert Data From Clickhouse', name='Clickhouse Middleware Insert API', tags=[ROUTER_TAGS['database']])
async def query_clickhouse(body: ClickHouseInsertConfig):
    fastapi_log.debug(body)
    my_clickhouse = clickhouse.MyDatabaseClickHouse({
        'host': body.config.host,
        'port': body.config.port,
        'user': body.config.user,
        'password': body.config.password,
        'database': body.config.db
    })
    # 转化时间格式
    for row in body.data:
        for index in range(len(body.datetime_index)):
            row[body.datetime_index[index]['index']] = datetime.datetime.strptime(row[body.datetime_index[index]['index']], body.datetime_index[index]['format'])
    result = my_clickhouse.insert_data(body.table, body.data, body.columns)
    my_clickhouse.close()
    return response_success({ 'result': result })

@router.post('/api/v1/database/cassandra', description='Get Data From Cassandra', name='Cassandra Middleware API', tags=[ROUTER_TAGS['database']])
async def query_cassandra(env: str, sql: str):
    return response_success({ 'env': env, 'sql': sql })

app.include_router(router)
