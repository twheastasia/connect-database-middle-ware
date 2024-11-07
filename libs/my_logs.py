#-*- coding:utf-8 -*-
__author__ = 'twh'

import logging
import logging.config
import datetime
import os

standard_format = '[%(asctime)s][task_id:%(name)s][%(filename)s:%(lineno)d]' \
                  '[%(levelname)s]: %(message)s' #其中name为getlogger指定的名字
simple_format = '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s'

LOGGING_DIC = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {'format': standard_format, 'datefmt': '%Y-%m-%d %H:%M:%S'},
        'simple': {'format': simple_format, 'datefmt': '%Y-%m-%d %H:%M:%S'},
    },
    'filters': {},  # 过滤日志
    'handlers': {
        #打印到终端的日志
        'console': {
            'level': logging.INFO,
            'class': 'logging.StreamHandler',  # 打印到屏幕
            'formatter': 'simple'
        },
        #打印到文件的日志,收集DEBUG及以上的日志
        'default': {
            'level': logging.DEBUG,
            'class': 'logging.handlers.RotatingFileHandler',  # 保存到文件
            'formatter': 'standard',
            'filename': '',  # 日志文件
            'maxBytes': 1024*1024*50,  # 日志大小 50M
            'backupCount': 5,
            'encoding': 'utf-8',  # 日志文件的编码，再也不用担心中文log乱码了
        },
    },
    'loggers': {
        # logging.getLogger(__name__)拿到的logger配置
        '': {
            'handlers': ['default', 'console'],  # 这里把上面定义的两个handler都加上，即log数据既写入文件又打印到屏幕
            'level': logging.DEBUG,
            'propagate': True,  # 向上（更高level的logger）传递
        },  # 当键不存在的情况下 (key设为空字符串)默认都会使用该k:v配置
        # '注册记录': {
        #     'handlers': ['console'],  # 这里把上面定义的两个handler都加上，即log数据既写入文件又打印到屏幕
        #     'level': 'WARNING',
        #     'propagate': True,  # 向上（更高level的logger）传递
        # },  # 当键不存在的情况下 (key设为空字符串)默认都会使用该k:v配置
    },
}


class MyLog():
    _instance = None
    def __new__(cls, *args, **kw):
        if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kw)
        return cls._instance

    def __init__(self, log_path='./logs'):
        self._create_dir(log_path)
        if os.getenv('log_filename') is None:
            log_filename = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".log"
            os.environ['log_filename'] = log_filename
        LOGGING_DIC['handlers']['default']['filename'] = log_path + '/' + os.environ['log_filename']
        # 记录日志
        logging.config.dictConfig(LOGGING_DIC)  # 自动加载字典中的配置
        self.logger = logging.getLogger('common')
    
    def _create_dir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)