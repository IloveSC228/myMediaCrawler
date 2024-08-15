# -*- coding: utf-8 -*-
# @Author  : 
# @Time    : 2024/7/14 14:21
# @Desc    : 异步AsyncIOMotorClient的增删改查封装
from typing import Any, Dict, List, Union, Optional

from motor.motor_asyncio import AsyncIOMotorClient
import aiomysql

class AsyncMongoDB: 
    def __init__(self, client: AsyncIOMotorClient) -> None:
        self.client = client

    async def insert(self, collection_name: str, item: Dict):
        db = self.client["social_monitor"]
        col = db[collection_name]
        res = await col.insert_one(item)
        return str(res.inserted_id)

    async def query(self, collection_name: str, myquery: Optional[Dict] = None, myprojection: Optional[Dict] = None, mysort: Optional[Dict] = None, mylimit: int = None) -> List[Dict]:
        if myquery is None:
            myquery = {}
        if myprojection is None:
            myprojection = {} 
        db = self.client["social_monitor"]
        col = db[collection_name]
        res = []
        async for document in col.find(myquery, myprojection, limit=mylimit, sort=mysort):
            res.append(document)
        return res
    
class AsyncMysqlDB:
    def __init__(self, pool: aiomysql.Pool) -> None:
        self.__pool = pool

    async def query(self, sql: str, *args: Union[str, int]) -> List[Dict[str, Any]]:
        """
        从给定的 SQL 中查询记录，返回的是一个列表
        :param sql: 查询的sql
        :param args: sql中传递动态参数列表
        :return:
        """
        async with self.__pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                data = await cur.fetchall()
                return data or []

    async def get_first(self, sql: str, *args: Union[str, int]) -> Union[Dict[str, Any], None]:
        """
        从给定的 SQL 中查询记录，返回的是符合条件的第一个结果
        :param sql: 查询的sql
        :param args:sql中传递动态参数列表
        :return:
        """
        async with self.__pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                data = await cur.fetchone()
                return data

    async def item_to_table(self, table_name: str, item: Dict[str, Any]) -> int:
        """
        表中插入数据
        :param table_name: 表名
        :param item: 一条记录的字典信息
        :return:
        """
        fields = list(item.keys())
        values = list(item.values())
        fields = [f'`{field}`' for field in fields]
        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        sql = "INSERT INTO %s (%s) VALUES(%s)" % (table_name, fieldstr, valstr)
        async with self.__pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, values)
                lastrowid = cur.lastrowid
                return lastrowid

    async def update_table(self, table_name: str, updates: Dict[str, Any], 
                           where_conditions: Dict[str, Union[str, int, float]]) -> int:
        """
        更新指定表的记录
        :param table_name: 表名
        :param updates: 需要更新的字段和值的 key - value 映射
        :param where_conditions: where 条件中的字段键值对列表
        :return:
        """
        upsets = []
        values = []
        for k, v in updates.items():
            s = '`%s`=%%s' % k
            upsets.append(s)
            values.append(v)
        upsets = ','.join(upsets)

        where_clauses = []
        for field, value in where_conditions.items():
            where_clauses.append(f'{field}=%s')
            values.append(value)
        where_clause = ' AND '.join(where_clauses)

        sql = f"UPDATE {table_name} SET {upsets} WHERE {where_clause}"
        async with self.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                rows = await cur.execute(sql, values)
                return rows

    async def execute(self, sql: str, *args: Union[str, int]) -> int:
        """
        需要更新、写入等操作的 excute 执行语句
        :param sql:
        :param args:
        :return:
        """
        async with self.__pool.acquire() as conn:
            async with conn.cursor() as cur:
                rows = await cur.execute(sql, args)
                return rows