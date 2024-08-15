from asyncio.tasks import Task
from contextvars import ContextVar
from typing import List

import aiomysql

from async_db import AsyncMongoDB, AsyncMysqlDB
from motor.motor_asyncio import AsyncIOMotorClient

request_keyword_var: ContextVar[str] = ContextVar("request_keyword", default="")
crawler_type_var: ContextVar[str] = ContextVar("crawler_type", default="")
comment_tasks_var: ContextVar[List[Task]] = ContextVar("comment_tasks", default=[])
media_crawler_mongodb_var: ContextVar[AsyncMongoDB] = ContextVar("media_crawler_mongodb_var")
mongodb_conn_client_var: ContextVar[AsyncIOMotorClient] = ContextVar("mongodb_conn_client_var")
media_crawler_mysqldb_var: ContextVar[AsyncMysqlDB] = ContextVar("media_crawler_mysqldb_var")
mysqldb_conn_pool_var: ContextVar[aiomysql.Pool] = ContextVar("mysqldb_conn_pool_var")