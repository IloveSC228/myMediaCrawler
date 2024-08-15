# -*- coding: utf-8 -*-
import asyncio
import json
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlencode
import csv
from datetime import datetime
import re
import ast

import httpx
from playwright.async_api import BrowserContext, Page

import config
from base.base_crawler import AbstractApiClient
from tools import utils, time_util

from .exception import DataFetchError
from .graphql import KuaiShouGraphQL
from var import media_crawler_mysqldb_var
from db import AsyncMysqlDB

class KuaiShouClient(AbstractApiClient):
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
        self._host = "https://www.kuaishou.com/graphql"
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict
        self.graphql = KuaiShouGraphQL()
        self.logger = utils.get_logger("ks")

    async def request(self, method, url, **kwargs) -> Any:
        async with httpx.AsyncClient(proxies=self.proxies) as client:
            response = await client.request(
                method, url, timeout=self.timeout,
                **kwargs
            )
        data: Dict = response.json()
        if data.get("errors"):
            self.logger.error(f"请求{method}: {url}?{kwargs} 数据获取错误")
            raise DataFetchError(data.get("errors", "unkonw error"))
        else:
            if data.get("data"):
                return data.get("data", {})
            else:
                self.logger.error(f"请求{method}: {url}?{kwargs} 未能获取到数据")

    async def get(self, uri: str, params=None) -> Dict:
        final_uri = uri
        if isinstance(params, dict):
            final_uri = (f"{uri}?"
                         f"{urlencode(params)}")
        return await self.request(method="GET", url=f"{self._host}{final_uri}", headers=self.headers)

    async def post(self, uri: str, data: dict) -> Dict:
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        return await self.request(method="POST", url=f"{self._host}{uri}",
                                  data=json_str, headers=self.headers)

    async def pong(self) -> bool:
        """get a note to check if login state is ok"""
        # utils.logger.info("[KuaiShouClient.pong] Begin pong kuaishou...")
        self.logger.info("开始测试能否连接上快手")
        ping_flag = False
        try:
            post_data = {
                "operationName": "visionProfileUserList",
                "variables": {
                    "ftype": 1,
                },
                "query": self.graphql.get("vision_profile_user_list")
            }
            res = await self.post("", post_data)
            if res.get("visionProfileUserList", {}).get("result") == 1:
                ping_flag = True
        except Exception as e:
            # utils.logger.error(f"[KuaiShouClient.pong] Pong kuaishou failed: {e}, and try to login again...")
            self.logger.error(f"尝试连接快手失败: {e}, 请重新登录...")
            ping_flag = False
        if ping_flag:
            self.logger.info("成功连接上快手")
        else:
            self.logger.info("尝试连接快手失败, 请重新登录...")
        return ping_flag

    async def update_cookies(self, browser_context: BrowserContext):
        cookie_str, cookie_dict = utils.convert_cookies(await browser_context.cookies())
        self.headers["Cookie"] = cookie_str
        self.cookie_dict = cookie_dict

    async def search_info_by_keyword(self, keyword: str, pcursor: str):
        """
        KuaiShou web search api
        :param keyword: search keyword
        :param pcursor: limite page curson
        :return:
        """
        post_data = {
            "operationName": "visionSearchPhoto",
            "variables": {
                "keyword": keyword,
                "pcursor": pcursor,
                "page": "search"
            },
            "query": self.graphql.get("search_query")
        }
        self.logger.info(f"发送关键词搜索请求: {post_data}")
        return await self.post("", post_data)

    async def get_video_info(self, photo_id: str) -> Dict:
        """
        Kuaishou web video detail api
        :param photo_id:
        :return:
        """
        post_data = {
            "operationName": "visionVideoDetail",
            "variables": {
                "photoId": photo_id,
                "page": "search"
            },
            "query": self.graphql.get("video_detail")
        }
        self.logger.info(f"发送通过id获取视频信息请求: {post_data}")
        return await self.post("", post_data)

    async def get_video_comments(self, photo_id: str, pcursor: str = "") -> Dict:
        """get video comments
        :param photo_id: photo id you want to fetch
        :param pcursor: last you get pcursor, defaults to ""
        :return:
        """
        post_data = {
            "operationName": "commentListQuery",
            "variables": {
                "photoId": photo_id,
                "pcursor": pcursor
            },
            "query": self.graphql.get("comment_list")
        }
        self.logger.info(f"发送获取评论请求: {post_data}")
        return await self.post("", post_data)

    async def get_video_sub_comments(
        self, photo_id: str, rootCommentId: str, pcursor: str = ""
    ) -> Dict:
        """get video sub comments
        :param photo_id: photo id you want to fetch
        :param pcursor: last you get pcursor, defaults to ""
        :return:
        """
        post_data = {
            "operationName": "visionSubCommentList",
            "variables": {
                "photoId": photo_id,
                "pcursor": pcursor,
                "rootCommentId": rootCommentId,
            },
            "query": self.graphql.get("vision_sub_comment_list"),
        }
        self.logger.info(f"发送获取二级评论请求: {post_data}")
        return await self.post("", post_data)

    async def get_video_all_comments(
        self,
        photo_id: str,
        crawl_interval: float = 1.0,
        callback: Optional[Callable] = None,
    ):
        """
        get video all comments include sub comments
        :param photo_id:
        :param crawl_interval:
        :param callback:
        :return:
        """

        result = []
        pcursor = ""
        # 不超过max_comment_cnt条评论
        config.KS_CRAWLER_COMMENT_CNT = 0
        while pcursor != "no_more" and config.KS_CRAWLER_COMMENT_CNT < config.CRAWLER_MAX_COMMENT_COUNT:
            comments_res = await self.get_video_comments(photo_id, pcursor)
            vision_commen_list = comments_res.get("visionCommentList", {})
            pcursor = vision_commen_list.get("pcursor", "")
            comments = vision_commen_list.get("rootComments", [])

            if callback:  # 如果有回调函数，就执行回调函数
                await callback(photo_id, comments)

            result.extend(comments)
            config.KS_CRAWLER_COMMENT_CNT += len(comments)
            await asyncio.sleep(crawl_interval)
            # sub_comments = await self.get_comments_all_sub_comments(
            #     comments, photo_id, crawl_interval, callback
            # )
            # result.extend(sub_comments)
        return result

    async def get_comments_all_sub_comments(
        self,
        comments: List[Dict],
        photo_id,
        crawl_interval: float = 1.0,
        callback: Optional[Callable] = None,
    ) -> List[Dict]:
        """
        获取指定一级评论下的所有二级评论, 该方法会一直查找一级评论下的所有二级评论信息
        Args:
            comments: 评论列表
            photo_id: 视频id
            crawl_interval: 爬取一次评论的延迟单位（秒）
            callback: 一次评论爬取结束后
        Returns:

        """
        if not config.ENABLE_GET_SUB_COMMENTS:
            # utils.logger.info(
            #     f"[KuaiShouClient.get_comments_all_sub_comments] Crawling sub_comment mode is not enabled"
            # )
            return []

        result = []
        for comment in comments:
            sub_comments = comment.get("subComments")
            if sub_comments and callback:
                await callback(photo_id, sub_comments)

            sub_comment_pcursor = comment.get("subCommentsPcursor")
            if sub_comment_pcursor == "no_more":
                continue

            root_comment_id = comment.get("commentId")
            sub_comment_pcursor = ""

            while sub_comment_pcursor != "no_more":
                comments_res = await self.get_video_sub_comments(
                    photo_id, root_comment_id, sub_comment_pcursor
                )
                vision_sub_comment_list = comments_res.get("visionSubCommentList",{})
                sub_comment_pcursor = vision_sub_comment_list.get("pcursor", "no_more")

                comments = vision_sub_comment_list.get("subComments", {})
                if callback:
                    await callback(photo_id, comments)
                await asyncio.sleep(crawl_interval)
                result.extend(comments)
        return result

    async def get_rank(self, url, params):
        with httpx.Client() as client:
            response = client.get(url, headers=self.headers, params=params)
            return response.text
    
    async def get_hotlist(self) -> Dict:
        """
        获取静态主页热点信息
        Args:

        Returns:

        """
        url = "https://www.kuaishou.com/"
        params = {
            "isHome": "1"
        }
        self.logger.info(f"发送获取热榜请求: {url}?{urlencode(params)}")
        html = await self.get_rank(url, params)
        result_list = []
        json_str = re.findall(r'<script>window.__APOLLO_STATE__=(.*?);\(function\(\).*;</script>', html, re.S)[0]
        json_dict = json.loads(json_str)
        for key in json_dict['defaultClient']:
            if re.findall(r'VisionHotRankItem.*', key):
                result_dict = {}
                result_dict['hot_title'] = json_dict['defaultClient'][key]['name']
                result_dict['hot_rank'] = json_dict['defaultClient'][key]['rank'] + 1
                result_dict['hot_score'] = json_dict['defaultClient'][key]['hotValue']
                result_dict['hot_videoIds'] = json_dict['defaultClient'][key]['photoIds']['json']
                result_list.append(result_dict)
        return result_list

    async def get_hotlist_videoIds_csv(self):
        """
        获取热点对应视频ID
        Args:

        Returns:

        """
        hotlist_videoIds = {}
        self.logger.info("从csv文件获取热榜信息")
        csv_file_path = f'data/kuaishou/ks_1_hotlist_title_{datetime.now().date()}.csv'
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            # 创建一个csv.DictReader对象
            reader = csv.reader(csvfile)
            # 遍历CSV文件中的每一行
            for i, row in enumerate(reader):  
                if i == 0:
                    continue
                hotlist_videoIds[row[0]] = row[5].replace("'","").split(', ')
        return hotlist_videoIds
    
    async def get_hotlist_videoIds_db(self):
        """
        获取热点对应视频ID
        Args:

        Returns:

        """
        self.logger.info("从数据库获取热榜信息")
        async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
        sql: str = f"select id, hot_video_ids from hotlist where hot_source = 'ks' order by hot_time desc limit 51"
        hotlists: List[Dict] = await async_db_conn.query(sql)
        if len(hotlists) > 0:
            hotlist_videoIds = {}
            for item in hotlists[::-1]:
                hotlist_videoIds[item['id']] = item['hot_video_ids'].replace("'","").split(', ')
            return hotlist_videoIds
        return dict()
    
    async def get_update_videoIds_db(self):
        """
        获取需要更新的对应视频ID
        Args:

        Returns:

        """
        if config.UPDATE_DATE == "":
            begindate = time_util.get_current_date()
            begints = time_util.get_unix_time_from_time_str(f"{begindate} 00:00:00")
        else:
            begindate = config.UPDATE_DATE
            begints = time_util.get_unix_time_from_time_str(f"{begindate} 00:00:00")
        self.logger.info(f"从数据库获取需要更新时段{begindate} 00:00:00 - {time_util.get_current_time()} 的视频id")
        
        async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
        sql: str = f"select hot_id, content_id from content where content_crawl_time > {begints}000 and content_source = 'ks' order by content_crawl_time desc"
        queryres: List[Dict] = await async_db_conn.query(sql)
        if len(queryres) > 0:
            videoIds = {}
            for q in queryres:
                videoIds[q['content_id']] = q['hot_id']
            return videoIds
        self.logger.info("该时间段快手爬虫无爬取数据")
        return dict()