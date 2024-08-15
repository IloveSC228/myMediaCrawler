# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/4/6 14:54
# @Desc    : mediacrawler db 管理
import asyncio
from typing import Dict
from urllib.parse import urlparse

import aiofiles
import aiomysql
from motor.motor_asyncio import AsyncIOMotorClient

import config
from async_db import AsyncMongoDB, AsyncMysqlDB
from tools import utils
from var import media_crawler_mongodb_var, mongodb_conn_client_var, media_crawler_mysqldb_var, mysqldb_conn_pool_var


def parse_mysql_url(mysql_url) -> Dict:
    """
    从配置文件中解析db链接url，给到aiomysql用，因为aiomysql不支持直接以URL的方式传递链接信息。
    Args:
        mysql_url: mysql://root:{RELATION_DB_PWD}@localhost:3306/media_crawler

    Returns:

    """
    parsed_url = urlparse(mysql_url)
    db_params = {
        'host': parsed_url.hostname,
        'port': parsed_url.port or 3306,
        'user': parsed_url.username,
        'password': parsed_url.password,
        'db': parsed_url.path.lstrip('/')
    }
    return db_params


async def init_mediacrawler_db():
    """
    初始化数据库客户端对象，并将该对象塞给media_crawler_db_var上下文变量
    Returns:

    """
    db_conn_params = parse_mysql_url(config.RELATION_DB_URL)
    pool = await aiomysql.create_pool(
        autocommit=True,
        **db_conn_params
    )
    async_db_obj = AsyncMysqlDB(pool)
    # 将连接池对象和封装的CRUD sql接口对象放到上下文变量中
    mysqldb_conn_pool_var.set(pool)
    media_crawler_mysqldb_var.set(async_db_obj)

    client = AsyncIOMotorClient('mongodb://root:cqu1701@10.242.187.124:27017')
    async_db_obj = AsyncMongoDB(client)
    # 将客户端对象和封装的CRUD接口对象放到上下文变量中
    mongodb_conn_client_var.set(client)
    media_crawler_mongodb_var.set(async_db_obj)

async def init_db():
    """
    初始化mongodb客户端
    Returns:

    """
    logger = utils.get_logger(config.PLATFORM)
    logger.info("数据库连接初始化开始")
    # utils.logger.info("[init_db] start init mediacrawler db connect object")
    await init_mediacrawler_db()
    # utils.logger.info("[init_db] end init mediacrawler db connect object") 
    logger.info("数据库连接初始化结束")

async def close():
    """
    关闭客户端
    Returns:

    """
    logger = utils.get_logger(config.PLATFORM)
    # utils.logger.info("[close] close mediacrawler db client")
    db_pool: aiomysql.Pool = mysqldb_conn_pool_var.get()
    if db_pool is not None:
        db_pool.close()

    client: AsyncIOMotorClient = mongodb_conn_client_var.get()
    if client is not None:
        client.close()
    logger.info("数据库连接关闭")

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete()
