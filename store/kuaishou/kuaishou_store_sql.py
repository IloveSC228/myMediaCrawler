# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/4/6 15:30
# @Desc    : sql接口集合

from typing import Dict, List

from db import AsyncMysqlDB
from var import media_crawler_mysqldb_var

async def query_hotlist_by_hot_title(hot_title: str) -> Dict:
    """
    查询一条热榜记录
    Args:
        hotlist_id:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    # 第一种
    # sql: str = f"select * from hotlist where hot_title = '{hot_title}'"
    # 第二种
    sql: str = f"select * from hotlist where hot_title = %s"
    args = (hot_title)
    rows: List[Dict] = await async_db_conn.query(sql, args)
    if len(rows) > 0:
        return rows[0]
    return dict()

async def add_new_hotlist(hotlist_item: Dict) -> int:
    """
    新增一条热榜记录
    Args:
        hotlist_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    last_row_id: int = await async_db_conn.item_to_table("hotlist", hotlist_item)
    return last_row_id

async def update_hotlist_by_hot_title(hot_title: str, hotlist_item: Dict) -> int:
    """
    更新一条热榜记录
    Args:
        hotlist_id:
        hotlist_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    effect_row: int = await async_db_conn.update_table("hotlist", hotlist_item, {"hot_title": hot_title})
    return effect_row

async def query_content_by_content_id(content_id: str) -> Dict:
    """
    查询一条内容记录(xhszh的帖子 | 微博 | 快手视频 ...)
    Args:
        content_id:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    sql: str = f"select * from content where content_id = '{content_id}'"
    rows: List[Dict] = await async_db_conn.query(sql)
    if len(rows) > 0:
        return rows[0]
    return dict()

async def add_new_content(content_item: Dict) -> int:
    """
    新增一条内容记录(xhszh的帖子 | 微博 | 快手视频 ...)
    Args:
        content_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    last_row_id: int = await async_db_conn.item_to_table("content", content_item)
    return last_row_id

async def update_content_by_content_id(content_id: str, content_item: Dict) -> int:
    """
    更新一条记录(xhszh的帖子 | 微博 | 快手视频 ...)
    Args:
        content_id:
        content_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    effect_row: int = await async_db_conn.update_table("content", content_item, {"content_id": content_id})
    return effect_row

async def query_comment_by_comment_id(comment_id: str) -> Dict:
    """
    查询一条评论内容
    Args:
        comment_id:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    sql: str = f"select * from comment where comment_id = '{comment_id}'"
    rows: List[Dict] = await async_db_conn.query(sql)
    if len(rows) > 0:
        return rows[0]
    return dict()

async def add_new_comment(comment_item: Dict) -> int:
    """
    新增一条评论记录
    Args:
        comment_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    last_row_id: int = await async_db_conn.item_to_table("comment", comment_item)
    return last_row_id

async def update_comment_by_comment_id(comment_id: str, comment_item: Dict) -> int:
    """
    更新增一条评论记录
    Args:
        comment_id:
        comment_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    effect_row: int = await async_db_conn.update_table("comment", comment_item, {"comment_id": comment_id})
    return effect_row

async def query_extract_info(content_id, content_type, content_source) -> Dict:
    """
    查询一条信息内容
    Args:
        content_id: 
        content_type:
    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    sql: str = f"select * from extract_info where content_id = {content_id} and content_type = {content_type} and content_source = '{content_source}'"
    rows: List[Dict] = await async_db_conn.query(sql)
    if len(rows) > 0:
        return rows[0]
    return dict()

async def add_new_extract_info(extract_info_item: Dict) -> int:
    """
    新增一条信息记录
    Args:
        extract_info_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    last_row_id: int = await async_db_conn.item_to_table("extract_info", extract_info_item)
    return last_row_id

async def update_extract_info(content_id, content_type, content_source, extract_info_item: Dict) -> int:
    """
    更新增一条信息记录
    Args:
        content_id:
        extract_info_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
    effect_row: int = await async_db_conn.update_table("extract_info", extract_info_item, {"content_id": content_id, "content_type": content_type, "content_source": content_source})
    return effect_row
