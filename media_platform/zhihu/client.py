import asyncio
import json
import re
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urlencode
import execjs
import csv
from datetime import datetime

import httpx, requests
from playwright.async_api import BrowserContext, Page

import config
from base.base_crawler import AbstractApiClient
from tools import utils, time_util

from .exception import DataFetchError, IPBlockError
from .field import SearchType
from var import media_crawler_mysqldb_var
from db import AsyncMysqlDB

class ZhiHuClient(AbstractApiClient):
    def __init__(
            self,
            timeout=10,
            proxies=None,
            *,
            headers: Dict[str, str],
            playwright_page: Page,
            cookie_dict: Dict[str, str],
    ):
        self.proxies = proxies
        self.timeout = timeout
        self.headers = headers
        self._host = "https://www.zhihu.com"
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict
        self.logger = utils.get_logger("zh")

    async def _pre_headers(self, url: str) -> Dict:
        """
        请求头参数签名x-zse-96
        Args:
            url:
            data:

        Returns:

        """
        with open('media_platform/zhihu/algorithm.js','r',encoding='utf-8') as f:
            func = f.read()
        x_zse = execjs.compile(func).call('ed',url.replace('https://www.zhihu.com',""))
        headers = {
            "x-zse-96": f"2.0_{x_zse}",
        }
        self.headers.update(headers)
        return self.headers

    async def request(self, method, url, headers) -> Any:
        async with httpx.AsyncClient(proxies=self.proxies) as client:
            response = await client.request(
                method, url, timeout=self.timeout,
                headers=headers, cookies=self.cookie_dict
            )
        data: Dict = response.json()
        if data.get("is_pass"):
            return data
        if ('ok' in data and data.get("ok") != 1) or ('is_pass' in data and data.get("is_pass") != "True"):
            self.logger.error(f"请求 {method}:{url} 错误")
            # utils.logger.error(f"[ZhiHuClient.request] request {method}:{url} err, res:{data}")
            raise DataFetchError(data.get("msg", "unkonw error"))
        elif "data" in data:
            if "paging" not in data:
                return data.get("data", {})
            elif data.get("paging").get("next"):
                return data
            else:
                return data.get("data", {})
        else:
            return data

    async def get(self, uri: str, params=None, headers=None, flag: bool = True) -> Dict:
        final_uri = uri
        if isinstance(params, dict):
            final_uri = (f"{uri}?"
                         f"{urlencode(params)}")
        if flag:
            await self._pre_headers(final_uri)
        return await self.request(method="GET", url=f"{self._host}{final_uri}", headers=self.headers)

    async def post(self, uri: str, data: dict) -> Dict:
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        return await self.request(method="POST", url=f"{self._host}{uri}",
                                  data=json_str, headers=self.headers)
    # 这里没改
    async def pong(self) -> bool:
        """get a note to check if login state is ok"""
        self.logger.info("开始测试能否连接上知乎")
        # utils.logger.info("[ZhuHuClient.pong] Begin to pong zh...")
        ping_flag = False
        try:
            uri = "/api/account/prod/token/refresh"
            resp_data: Dict = await self.request(method="POST", url=f"{self._host}{uri}", headers=self.headers)
            if len(resp_data):
                ping_flag = True
                self.logger.info("成功连接上知乎")
        except Exception as e:
            self.logger.error(f"尝试连接知乎失败: {e}, 请重新登录...")
            # utils.logger.error(f"[ZhiHuClient.pong] Ping zh failed: {e}, and try to login again...")
            ping_flag = False
        return ping_flag

    async def update_cookies(self, browser_context: BrowserContext):
        """
        API客户端提供的更新cookies方法，一般情况下登录成功后会调用此方法
        Args:
            browser_context: 浏览器上下文对象

        Returns:

        """
        cookie_str, cookie_dict = utils.convert_cookies(await browser_context.cookies())
        self.headers["Cookie"] = cookie_str
        self.cookie_dict = cookie_dict

    async def get_note_by_keyword( 
            self, q: str,
            gk_version: str = "gz-gaokao",
            t: SearchType = SearchType.CONTENT,
            correction: int = 1,
            offset: int = 0,
            limit: int = 20,
            filter_fields: str = "",
            lc_idx: int = 0,
            show_all_topics: int = 0,
            search_source: str = "Normal",
            # next: str = 
    ) -> Dict:
        """
        根据关键词搜索笔记
        Args:
            keyword: 关键词参数
            page: 分页第几页
            search_type: 搜索的笔记类型

        Returns:

        """
        url = "/api/v4/search_v3"
        params = {
            "gk_version": gk_version,
            "t": t,
            "q": q, 
            "correction": correction,
            "offset": offset,
            "limit": limit,
            "filter_fields": filter_fields,
            "lc_idx": lc_idx,
            "show_all_topics": show_all_topics,
            "search_source": search_source
        }
        self.logger.info(f"发送关键词搜索请求: {url}?{urlencode(params)}")
        return await self.get(url, params,flag=False)

    # async def get_note_by_id(self, note_id: str) -> Dict:
    #     """
    #     获取笔记详情API
    #     Args:
    #         note_id:笔记ID

    #     Returns:

    #     """
    #     data = {"source_note_id": note_id}
    #     uri = "/api/sns/web/v1/feed"
    #     res = await self.post(uri, data)
    #     if res and res.get("items"):
    #         res_dict: Dict = res["items"][0]["note_card"]
    #         return res_dict
    #     utils.logger.error(f"[ZhiHuClient.get_note_by_id] get note empty and res:{res}")
    #     return dict()

    async def get_note_comments(self, note_id: str, note_type: str, next: str = "", order: str = "score") -> Dict:
        """
        获取一级评论的API
        Args:
            note_id: 笔记ID
            cursor: 分页游标

        Returns:

        """
        if next:
            offset = next.split('offset=')[1].split('&')[0]
        else:
            offset = ""
        url = f"/api/v4/comment_v5/{note_type}/{note_id}/root_comment"
        params = {
            "order_by": order,
            "limit": 20,
            "offset": offset,
        }
        self.logger.info(f"发送获取评论请求: {url}?{urlencode(params)}")
        return await self.get(url, params)

    async def get_note_sub_comments(self, root_comment_id: str, next: str = ""):
        """
        获取指定父评论下的子评论的API
        Args:
            root_comment_id: 根评论ID
            cursor: 分页游标

        Returns:

        """
        if next:
            offset = next.split('offset=')[1].split('&')[0]
        else:
            offset = ""
        url = f"/api/v4/comment_v5/comment/{root_comment_id}/child_comment"
        params = {
            "order_by": "ts",
            "limit": 20,
            "offset": offset,
        }
        self.logger.info(f"发送获取二级评论请求: {url}?{urlencode(params)}")
        return await self.get(url, params)

    async def get_note_all_comments(self, note_id: str, note_type: str, crawl_interval: float = 1.0, 
                                    callback: Optional[Callable] = None):
        """
        获取指定笔记下的所有一级评论，该方法会一直查找一个帖子下的所有评论信息
        Args:
            note_id: 笔记ID
            crawl_interval: 爬取一次笔记的延迟单位（秒）
            callback: 一次笔记爬取结束后

        Returns:

        """
        comment_is_end = False
        comment_next = ""
        config.ZH_CRAWLER_COMMENT_CNT = 0
        while not comment_is_end and config.ZH_CRAWLER_COMMENT_CNT < config.CRAWLER_MAX_COMMENT_COUNT: 
            comments_res = await self.get_note_comments(note_id, note_type, comment_next)
            if "data" not in comments_res:
                self.logger.error(f"未能爬取到{note_id}的{comment_next}一级评论")
                # utils.logger.info(
                #     f"[ZhiHuClient.get_note_all_comments] No 'comments' key found in response: {comments_res}")
                break
            if "paging" not in comments_res:
                self.logger.error(f"帖子{note_id}的{comment_next}后没有评论")
                # utils.logger.error(
                #     f"[ZhiHuClient.get_note_all_comments] there is no more comments in id : {note_id}")
                break

            comment_is_end = comments_res.get("paging").get("is_end")
            comment_next = comments_res.get("paging").get("next")
            comments = comments_res["data"]
                
            if callback:
                await callback(note_id, comments, note_type)
            config.ZH_CRAWLER_COMMENT_CNT += len(comments)
            await asyncio.sleep(crawl_interval)
            await self.get_comments_all_sub_comments(comments, note_id, note_type, crawl_interval, callback)

    async def get_comments_all_sub_comments(self, comments: List[Dict], note_id: str, note_type: str, crawl_interval: float = 1.0,
                                    callback: Optional[Callable] = None) -> List[Dict]:
        """
        获取指定一级评论下的所有二级评论, 该方法会一直查找一级评论下的所有二级评论信息
        Args:
            comments: 评论列表
            note_id: 问题或回答id
            note_type: 是问题还是回答
            crawl_interval: 爬取一次评论的延迟单位（秒）
            callback: 一次评论爬取结束后

        Returns:
        
        """
        if not config.ENABLE_GET_SUB_COMMENTS:
            # utils.logger.info(f"[ZhiHuCrawler.get_comments_all_sub_comments] Crawling sub_comment mode is not enabled")
            return 
        
        for comment in comments:
            child_comment_count = comment.get("child_comment_count")
            if child_comment_count == 0:
                continue

            par_comment_id = comment.get("id")
            sub_comment_is_end = False
            sub_comment_next = ""
         
            while not sub_comment_is_end:
                comments_res = await self.get_note_sub_comments(par_comment_id, sub_comment_next)
                if "data" not in comments_res:
                    self.logger.error(f"未能爬取到{note_id}的{sub_comment_next}二级评论")
                    # utils.logger.info(
                    #     f"[ZhiHuClient.get_comments_all_sub_comments] No 'comments' key found in response: {comments_res}")
                    break
                if "paging" not in comments_res:
                    self.logger.error(f"帖子{note_id}的{sub_comment_next}后没有评论")
                    # utils.logger.error(
                    #     f"[ZhiHuClient.get_comments_all_sub_comments] there is no more sub comments in id : {par_comment_id}")
                    break

                sub_comment_is_end = comments_res['paging']['is_end']
                sub_comment_next = comments_res['paging']['next']
                comments = comments_res["data"]
                if callback:
                    await callback(note_id, comments, note_type)
                await asyncio.sleep(crawl_interval)

    async def update_note_comments(self, note_id: str, note_type: str, updated_time: str, crawl_interval: float = 1.0, 
                                    callback: Optional[Callable] = None, ):
        """
        获取新的一级评论，
        Args:
            comments: 评论列表
            note_id: 问题或回答id
            note_type: 是问题还是回答
            updated_time: 上次爬取的最新评论时间
            crawl_interval: 爬取一次评论的延迟单位（秒）
            callback: 一次评论爬取结束后

        Returns:
        
        """
        comment_is_end = False
        comment_next = ""
        while not comment_is_end:
            comments_res = await self.get_note_comments(note_id, note_type, comment_next, "ts")
            if "data" not in comments_res:
                self.logger.error(f"未能爬取到{note_id}的{comment_next}一级评论")
                # utils.logger.info(
                #     f"[ZhiHuClient.get_note_all_comments] No 'comments' key found in response: {comments_res}")
                break
            if "paging" not in comments_res:
                self.logger.error(f"帖子{note_id}的{comment_next}后没有评论")
                # utils.logger.error(
                #     f"[ZhiHuClient.get_note_all_comments] there is no more comments in id : {note_id}")
                break

            comment_is_end = comments_res.get("paging").get("is_end")
            comment_next = comments_res.get("paging").get("next")
            comments = comments_res["data"]

            index = 0
            for comment in comments:
                if comment["created_time"] > updated_time:
                    index += 1
                else:
                    comment_is_end = True
                    break

            if callback:
                await callback(note_id, comments[:index], note_type)
            await asyncio.sleep(crawl_interval)

    async def get_hotlist(self) -> Dict:
        """
        获取搜索框下的热点信息
        Args:

        Returns:

        """
        url = "/api/v3/feed/topstory/hot-lists/total"
        params = {
            "limit": "50",
            "desktop": "true"
        }
        self.logger.info(f"发送获取热榜请求: {url}?{urlencode(params)}")
        return await self.get(url, params)
    
    async def get_hotlist_questionIds_csv(self):
        self.logger.info("从csv文件获取热榜信息")
        csv_file_path = f'data/zhihu/zh_1_hotlist_title_{datetime.now().date()}.csv'
        hot_questionIds = []
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            # 创建一个csv.DictReader对象
            reader = csv.reader(csvfile)
            # 遍历CSV文件中的每一行
            for i, row in enumerate(reader):  
                if i == 0:
                    continue
                hot_questionIds.append(row[5])
        return hot_questionIds
    
    async def get_hotlist_questionIds_db(self):
        """
        获取最近一次热榜爬取的问题id, 数据库
        Args:
        Returns:

        """
        self.logger.info("从数据库获取热榜信息")
        async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
        sql: str = f"select id, hot_question_id from hotlist where hot_source = 'zh' order by hot_time desc limit 30"
        hotlists: List[Dict] = await async_db_conn.query(sql)
        if len(hotlists) > 0:
            hot_questionIds = {}
            for item in hotlists[::-1]:
                hot_questionIds[item['id']] = item['hot_question_id']
            return hot_questionIds
        return dict()
    
    async def get_update_noteIds_db(self):
        """
        获取需要更新的对应帖子ID
        Args:

        Returns:

        """
        if config.UPDATE_DATE == "":
            begindate = time_util.get_current_date()
            begints = time_util.get_unix_time_from_time_str(f"{begindate} 00:00:00")
        else:
            begindate = config.UPDATE_DATE
            begints = time_util.get_unix_time_from_time_str(f"{begindate} 00:00:00")
        self.logger.info(f"从数据库获取需要更新时段{begindate} 00:00:00 - {time_util.get_current_time()} 的帖子id")
        
        async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
        sql: str = f"select content_id, question_id from content where content_crawl_time > {begints}000 and content_source = 'zh' order by content_crawl_time desc"
        queryres: List[Dict] = await async_db_conn.query(sql)
        if len(queryres) > 0:
            noteIds = {}
            for q in queryres:
                if q['question_id'] in noteIds:
                    noteIds[q['question_id']].append(q['content_id'])
                else:
                    noteIds[q['question_id']] = [q['content_id']]
            return noteIds
        self.logger.info("该时间段微博爬虫无爬取数据")
        return dict()
    
    async def get_latest_comment_time(self, note_id, note_type):
        """
        获取某id爬取的最新评论时间, 数据库
        Args:
            note_id: 问题或回答id
            note_type: 问题或回答
        Returns:

        """
        async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
        if note_type == "questions":
            sql: str = f"select comment_time from comment where comment_source = 'zh' and content_id like %s order by comment_time desc limit 1"
            value = ('q%')
            res = await async_db_conn.query(sql, value)
        else:
            sql: str = f"select comment_time from comment where comment_source = 'zh' and content_id like %s order by comment_time desc limit 1"
            value = ('a%')
            res = await async_db_conn.query(sql, value)
        
        return res[0]["comment_time"]

    async def get_answers(self, question_id: str, offset: int, cursor="", session_id=""):
        """
        获取单个问题id下的所有回答
        Args:

        Returns:

        """
        uri = f"/api/v4/questions/{question_id}/feeds"
        params = {
            "cursor": cursor,
            "include": "content,voteup_count",
            "limit": 5,
            "offset": offset,
            "session_id": session_id
        }
        res = await self.get(uri, params, flag=False)
        if "paging" in res:
            return res.get("data"), res.get("paging").get("next")
        else:
            return res, None