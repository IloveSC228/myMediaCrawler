# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2023/12/23 15:40
# @Desc    : 微博爬虫 API 请求 client

import asyncio
import copy
import json
import re
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlencode
import csv
from datetime import datetime

import httpx
from playwright.async_api import BrowserContext, Page

import config
from tools import utils, time_util

from .exception import DataFetchError
from .field import SearchType
from var import media_crawler_mysqldb_var
from db import AsyncMysqlDB

class WeiboClient:
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
        self._host = "https://m.weibo.cn"
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict
        self._image_agent_host = "https://i1.wp.com/"
        self.logger = utils.get_logger("wb")

    async def request(self, method, url, **kwargs) -> Any:
        async with httpx.AsyncClient(proxies=self.proxies) as client:
            response = await client.request(
                method, url, timeout=self.timeout,
                **kwargs
            )
        data: Dict = response.json()
        if data.get("ok") != 1:
            self.logger.error(f"请求{method}: {url}?{kwargs} 数据获取错误")
            # utils.logger.error(f"[WeiboClient.request] request {method}:{url} err, res:{data}")
            # raise DataFetchError(data.get("msg", "unkonw error"))
        else:
            return data.get("data", {})

    async def get(self, uri: str, params=None, headers=None) -> Dict:
        final_uri = uri
        if isinstance(params, dict):
            final_uri = (f"{uri}?"
                         f"{urlencode(params)}")

        if headers is None:
            headers = self.headers
        return await self.request(method="GET", url=f"{self._host}{final_uri}", headers=headers)

    async def post(self, uri: str, data: dict) -> Dict:
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        return await self.request(method="POST", url=f"{self._host}{uri}",
                                  data=json_str, headers=self.headers)

    async def pong(self) -> bool:
        """get a note to check if login state is ok"""
        self.logger.info("开始测试能否连接上微博")
        # utils.logger.info("[WeiboClient.pong] Begin pong weibo...")
        ping_flag = False
        try:
            uri = "/api/config"
            resp_data: Dict = await self.request(method="GET", url=f"{self._host}{uri}", headers=self.headers)
            if resp_data.get("login"):
                ping_flag = True
                self.logger.info("成功连接上快手")
            else:
                self.logger.error(f"尝试连接微博失败, 请重新登录...")
                # utils.logger.error(f"[WeiboClient.pong] cookie may be invalid and again login...")
        except Exception as e:
            self.logger.error(f"尝试连接微博失败: {e}, 请重新登录...")
            # utils.logger.error(f"[WeiboClient.pong] Pong weibo failed: {e}, and try to login again...")
            ping_flag = False
        return ping_flag

    async def update_cookies(self, browser_context: BrowserContext):
        cookie_str, cookie_dict = utils.convert_cookies(await browser_context.cookies())
        self.headers["Cookie"] = cookie_str
        self.cookie_dict = cookie_dict

    async def get_note_by_keyword(
            self,
            keyword: str,
            page: int = 1,
            search_type: SearchType = SearchType.DEFAULT
    ) -> Dict:
        """
        search note by keyword
        :param keyword: 微博搜搜的关键词
        :param page: 分页参数 -当前页码
        :param search_type: 搜索的类型，见 weibo/filed.py 中的枚举SearchType
        :return:
        """
        url = "/api/container/getIndex"
        containerid = f"100103type={search_type.value}&q=#{keyword}#"
        params = {
            "containerid": containerid,
            "page_type": "searchall",
            "page": page,
        }
        self.logger.info(f"发送关键词搜索请求: {url}?{urlencode(params)}")
        return await self.get(url, params)

    async def get_hotlist(self) -> Dict:
        """
        获取搜索框下的热点信息
        Args:

        Returns:

        """
        url = "/api/container/getIndex"
        params = {
            "containerid": "106003type=25&t=3&disable_hot=1&filter_type=realtimehot",
            "page_type": "08"
        }
        self.logger.info(f"发送获取热榜请求: {url}?{urlencode(params)}")
        return await self.get(url, params)

    async def get_note_comments(self, mid_id: str, max_id: int) -> Dict:
        """get notes comments
        :param mid_id: 微博ID
        :param max_id: 分页参数ID
        :return:
        """
        url = "/comments/hotflow"
        params = {
            "id": mid_id,
            "mid": mid_id,
            "max_id_type": 0,
        }
        if max_id > 0:
            params.update({"max_id": max_id})

        referer_url = f"https://m.weibo.cn/detail/{mid_id}"
        headers = copy.copy(self.headers)
        headers["Referer"] = referer_url
        self.logger.info(f"发送获取评论请求: {url}?{urlencode(params)}")
        return await self.get(url, params, headers=headers)

    async def get_note_all_comments(self, note_id: str, crawl_interval: float = 1.0, is_fetch_sub_comments=False,
                                    callback: Optional[Callable] = None, ):
        """
        get note all comments include sub comments
        :param note_id:
        :param crawl_interval:
        :param is_fetch_sub_comments:
        :param callback:
        :return:
        """

        result = []
        is_end = False
        max_id = -1
        config.WB_CRAWLER_COMMENT_CNT = 0
        while not is_end and config.WB_CRAWLER_COMMENT_CNT < config.CRAWLER_MAX_COMMENT_COUNT:
            comments_res = await self.get_note_comments(note_id, max_id)
            if not comments_res:
                self.logger.error(f"未能爬取到{note_id}的{max_id}数据")
                break
            max_id: int = comments_res.get("max_id")
            comment_list: List[Dict] = comments_res.get("data", [])
            is_end = max_id == 0
            if callback:  # 如果有回调函数，就执行回调函数
                await callback(note_id, comment_list)
            config.WB_CRAWLER_COMMENT_CNT += len(comment_list)
            await asyncio.sleep(crawl_interval)
            if not is_fetch_sub_comments:
                result.extend(comment_list)
                continue
            # todo handle get sub comments
        return result

    async def get_note_info_by_id(self, note_id: str) -> Dict:
        """
        根据帖子ID获取详情
        :param note_id:
        :return:
        """
        url = f"{self._host}/detail/{note_id}"
        async with httpx.AsyncClient(proxies=self.proxies) as client:
            self.logger.info(f"发送根据帖子ID获取信息请求: {url}")
            response = await client.request(
                "GET", url, timeout=self.timeout, headers=self.headers
            )
            if response.status_code != 200:
                self.logger.error(f"请求url: {url} 失败")
                raise DataFetchError(f"get weibo detail err: {response.text}")
            match = re.search(r'var \$render_data = (\[.*?\])\[0\]', response.text, re.DOTALL)
            if match:
                render_data_json = match.group(1)
                render_data_dict = json.loads(render_data_json)
                note_detail = render_data_dict[0].get("status")
                note_item = {
                    "mblog": note_detail
                }
                return note_item
            else:
                self.logger.error("未找到$render_data的值")
                # utils.logger.info(f"[WeiboClient.get_note_info_by_id] 未找到$render_data的值")
                return dict()

    # async def get_note_image(self, image_url: str) -> bytes: # 原本返回的是图片
    async def get_note_image(self, image_url: str):
        image_url = image_url[8:] # 去掉 https://
        sub_url = image_url.split("/")
        image_url = ""
        for i in range(len(sub_url)):
            if i == 1:
                image_url += "large/" #都获取高清大图
            elif i == len(sub_url) - 1:
                image_url += sub_url[i]
            else:
                image_url += sub_url[i] + "/"
        # 微博图床对外存在防盗链，所以需要代理访问
        # 由于微博图片是通过 i1.wp.com 来访问的，所以需要拼接一下
        final_uri = (f"{self._image_agent_host}" f"{image_url}")
        return final_uri
        # async with httpx.AsyncClient(proxies=self.proxies) as client:
        #     response = await client.request("GET", final_uri, timeout=self.timeout)
        #     if not response.reason_phrase == "OK":
        #         utils.logger.error(f"[WeiboClient.get_note_image] request {final_uri} err, res:{response.text}")
        #         return None
        #     else:
        #         return response.content
            
    async def get_hotlist_keyword_csv(self):
        self.logger.info("从csv文件获取热榜信息")
        csv_file_path = f'data/weibo/wb_1_hotlist_title_{datetime.now().date()}.csv'
        hot_keywords = ""
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            # 创建一个csv.DictReader对象
            reader = csv.reader(csvfile)
            # 遍历CSV文件中的每一行
            for i, row in enumerate(reader):  
                if i == 0:
                    continue
                if i > 1:
                    hot_keywords += ','
                hot_keywords += f"#{row[0]}#"
        return hot_keywords
    
    async def get_hotlist_keyword_db(self):
        """
        获取热点对应标题
        Args:

        Returns:

        """
        self.logger.info("从数据库获取热榜信息")
        async_db_conn: AsyncMysqlDB = media_crawler_mysqldb_var.get()
        sql: str = f"select id, hot_title, hot_rank from hotlist where hot_source = 'wb' order by hot_time desc limit 55"
        hotlists: List[Dict] = await async_db_conn.query(sql)
        if len(hotlists) > 0:
            hot_keywords = {}
            hot_rank = 1
            for item in hotlists[::-1]:
                if item["hot_rank"] != hot_rank:
                    continue
                hot_keywords[item['id']] = item['hot_title']
                hot_rank += 1
            return hot_keywords
        return dict()

    async def get_full_text(self, note_id): # 获取完整的帖子正文
        url = "/statuses/extend"
        params = {
            "id": note_id
        }
        self.logger.info(f"请求获取完整帖子{note_id}信息: {url}?{urlencode(params)}")
        data: Dict = await self.get(url, params)
        text = data.get('longTextContent')
        clean_text = re.sub(r"<.*?>", "", text)
        return clean_text
    
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
        sql: str = f"select content_id from content where content_crawl_time > {begints}000 and content_source = 'wb' order by content_crawl_time desc"
        queryres: List[Dict] = await async_db_conn.query(sql)
        if len(queryres) > 0:
            noteIds = [id['content_id'] for id in  queryres]
            return noteIds
        self.logger.info("该时间段微博爬虫无爬取数据")
        return dict()