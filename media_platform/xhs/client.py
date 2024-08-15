import asyncio
import json
import re
import csv
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urlencode
from datetime import datetime

import httpx
from playwright.async_api import BrowserContext, Page

import config
from base.base_crawler import AbstractApiClient
from tools import utils, time_util

from .exception import DataFetchError, IPBlockError
from .field import SearchNoteType, SearchSortType
from .help import get_search_id, sign
from var import media_crawler_mysqldb_var
from db import AsyncMysqlDB

class XiaoHongShuClient(AbstractApiClient):
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
        self._host = "https://edith.xiaohongshu.com"
        self._domain = "https://www.xiaohongshu.com"
        self.IP_ERROR_STR = "网络连接异常，请检查网络设置或重启试试"
        self.IP_ERROR_CODE = 300012
        self.NOTE_ABNORMAL_STR = "笔记状态异常，请稍后查看"
        self.NOTE_ABNORMAL_CODE = -510001
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict
        self.logger = utils.get_logger("xhs")

    async def _pre_headers(self, url: str, data=None) -> Dict:
        """
        请求头参数签名
        Args:
            url:
            data:

        Returns:

        """
        encrypt_params = await self.playwright_page.evaluate("([url, data]) => window._webmsxyw(url,data)", [url, data])
        local_storage = await self.playwright_page.evaluate("() => window.localStorage")
        signs = sign(
            a1=self.cookie_dict.get("a1", ""),
            b1=local_storage.get("b1", ""),
            x_s=encrypt_params.get("X-s", ""),
            x_t=str(encrypt_params.get("X-t", ""))
        )

        headers = {
            "X-S": signs["x-s"],
            "X-T": signs["x-t"],
            "x-S-Common": signs["x-s-common"],
            "X-B3-Traceid": signs["x-b3-traceid"]
        }
        self.headers.update(headers)
        return self.headers

    async def request(self, method, url, **kwargs) -> Union[str, Any]:
        """
        封装httpx的公共请求方法，对请求响应做一些处理
        Args:
            method: 请求方法
            url: 请求的URL
            **kwargs: 其他请求参数，例如请求头、请求体等

        Returns:

        """
        # return response.text
        return_response = kwargs.pop('return_response', False)

        async with httpx.AsyncClient(proxies=self.proxies) as client:
            response = await client.request(
                method, url, timeout=self.timeout,
                **kwargs
            )

        if return_response:
            return response.text

        data: Dict = response.json()
        if data["success"]:
            return data.get("data", data.get("success", {}))
        elif data["code"] == self.IP_ERROR_CODE:
            self.logger.error(f"请求{method}: {url}?{kwargs} 被反爬")
            raise IPBlockError(self.IP_ERROR_STR)
        else:
            self.logger.error(f"请求{method}: {url}?{kwargs} 数据获取错误")
            raise DataFetchError(data.get("msg", None))

    async def get(self, uri: str, params=None) -> Dict:
        """
        GET请求，对请求头签名
        Args:
            uri: 请求路由
            params: 请求参数

        Returns:

        """
        final_uri = uri
        if isinstance(params, dict):
            final_uri = (f"{uri}?"
                         f"{urlencode(params)}")
        headers = await self._pre_headers(final_uri)
        return await self.request(method="GET", url=f"{self._host}{final_uri}", headers=headers)

    async def post(self, uri: str, data: dict) -> Dict:
        """
        POST请求，对请求头签名
        Args:
            uri: 请求路由
            data: 请求体参数

        Returns:

        """
        headers = await self._pre_headers(uri, data)
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        return await self.request(method="POST", url=f"{self._host}{uri}",
                                  data=json_str, headers=headers)

    async def pong(self) -> bool:
        """
        用于检查登录态是否失效了
        Returns:

        """
        """get a note to check if login state is ok"""
        # utils.logger.info("[XiaoHongShuClient.pong] Begin to pong xhs...")
        self.logger.info("开始测试能否连接上小红书")
        ping_flag = False
        try:
            note_card: Dict = await self.get_note_by_keyword(keyword="小红书")
            if note_card.get("items"):
                ping_flag = True
                self.logger.info("成功连接上小红书")
        except Exception as e:
            self.logger.error(f"尝试连接小红书失败: {e}, 请重新登录...")
            # utils.logger.error(f"[XiaoHongShuClient.pong] Ping xhs failed: {e}, and try to login again...")
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
            self, keyword: str,
            page: int = 1, page_size: int = 20,
            sort: SearchSortType = SearchSortType.GENERAL,
            note_type: SearchNoteType = SearchNoteType.ALL
    ) -> Dict:
        """
        根据关键词搜索笔记
        Args:
            keyword: 关键词参数
            page: 分页第几页
            page_size: 分页数据长度
            sort: 搜索结果排序指定
            note_type: 搜索的笔记类型

        Returns:

        """
        url = "/api/sns/web/v1/search/notes"
        data = {
            "keyword": keyword,
            "page": page,
            "page_size": page_size,
            "search_id": get_search_id(),
            "sort": sort.value,
            "note_type": note_type.value
        }
        self.logger.info(f"发送关键词搜索请求: {url}?{urlencode(data)}")
        return await self.post(url, data)

    async def get_note_by_id(self, note_id: str) -> Dict:
        """
        获取笔记详情API
        Args:
            note_id:笔记ID

        Returns:

        """
        data = {"source_note_id": note_id}
        url = "/api/sns/web/v1/feed"
        self.logger.info(f"发送通过id获取帖子请求: {url}?{urlencode(data)}")
        res = await self.post(url, data)
        if res and res.get("items"):
            res_dict: Dict = res["items"][0]["note_card"]
            return res_dict
        self.logger.error(f"未能抓取到帖子{note_id}数据")
        # utils.logger.error(f"[XiaoHongShuClient.get_note_by_id] get note empty and res:{res}")
        return dict()

    async def get_note_comments(self, note_id: str, cursor: str = "") -> Dict:
        """
        获取一级评论的API
        Args:
            note_id: 笔记ID
            cursor: 分页游标

        Returns:

        """
        url = "/api/sns/web/v2/comment/page"
        params = {
            "note_id": note_id,
            "cursor": cursor,
            "top_comment_id": "",
            "image_formats": "jpg,webp,avif"
        }
        self.logger.info(f"发送获取评论请求: {url}?{urlencode(params)}")
        return await self.get(url, params)

    async def get_hotlist(self) -> Dict:
        """
        获取搜索框下的热点信息,暂用wb的热搜
        Args:

        Returns:

        """
        # uri = "/api/sns/web/v1/search/hotlist"
        # params = {
        #     "source": "search_box"
        # }
        uri = "/api/container/getIndex"
        params = {
            "containerid": "106003type=25&t=3&disable_hot=1&filter_type=realtimehot",
            "page_type": "08"
        }
        # return await self.get(uri, params)
        
        if isinstance(params, dict):
            final_uri = (f"https://m.weibo.cn{uri}?"
                         f"{urlencode(params)}")
        self.logger.info(f"发送获取热榜请求: {final_uri}")
        async with httpx.AsyncClient(proxies=self.proxies) as client:
            response = await client.request(
                "GET", final_uri, timeout=self.timeout,
                headers=self.headers
            )
        data: Dict = response.json()
        data = data["data"]["cards"][0]["card_group"]
        return data

    async def get_note_sub_comments(self, note_id: str, root_comment_id: str, num: int = 10, cursor: str = ""):
        """
        获取指定父评论下的子评论的API
        Args:
            note_id: 子评论的帖子ID
            root_comment_id: 根评论ID
            num: 分页数量
            cursor: 分页游标

        Returns:

        """
        url = "/api/sns/web/v2/comment/sub/page"
        params = {
            "note_id": note_id,
            "root_comment_id": root_comment_id,
            "num": num,
            "cursor": cursor,
        }
        self.logger.info(f"发送获取二级评论请求: {url}?{urlencode(params)}")
        return await self.get(url, params)

    async def get_note_all_comments(self, note_id: str, crawl_interval: float = 1.0,
                                    callback: Optional[Callable] = None) -> List[Dict]:
        """
        获取指定笔记下的所有一级评论，该方法会一直查找一个帖子下的所有评论信息
        Args:
            note_id: 笔记ID
            crawl_interval: 爬取一次笔记的延迟单位（秒）
            callback: 一次笔记爬取结束后

        Returns:

        """
        result = []
        comments_has_more = True
        comments_cursor = ""
        config.XHS_CRAWLER_COMMENT_CNT = 0
        while comments_has_more and config.XHS_CRAWLER_COMMENT_CNT < config.CRAWLER_MAX_COMMENT_COUNT:
            comments_res = await self.get_note_comments(note_id, comments_cursor)
            if "comments" not in comments_res:
                self.logger.error(f"未能爬取到{note_id}的{comments_cursor}一级评论")
                # utils.logger.info(
                #     f"[XiaoHongShuClient.get_note_all_comments] No 'comments' key found in response: {comments_res}")
                # break
            comments_has_more = comments_res.get("has_more", False)
            comments_cursor = comments_res.get("cursor", "")
            
            comments = comments_res["comments"]
            config.XHS_CRAWLER_COMMENT_CNT += len(comments)
            if callback:
                await callback(note_id, comments)
            await asyncio.sleep(crawl_interval)
            result.extend(comments)
            sub_comments = await self.get_comments_all_sub_comments(comments, crawl_interval, callback)
            result.extend(sub_comments)
        return result
    
    async def get_comments_all_sub_comments(self, comments: List[Dict], crawl_interval: float = 1.0,
                                    callback: Optional[Callable] = None) -> List[Dict]:
        """
        获取指定一级评论下的所有二级评论, 该方法会一直查找一级评论下的所有二级评论信息
        Args:
            comments: 评论列表
            crawl_interval: 爬取一次评论的延迟单位（秒）
            callback: 一次评论爬取结束后

        Returns:
        
        """
        if not config.ENABLE_GET_SUB_COMMENTS:
            # utils.logger.info(f"[XiaoHongShuCrawler.get_comments_all_sub_comments] Crawling sub_comment mode is not enabled")
            return []
        
        result = []
        for comment in comments:
            note_id = comment.get("note_id")
            sub_comments = comment.get("sub_comments")
            if sub_comments and callback:
                await callback(note_id, sub_comments)
                
            sub_comment_has_more = comment.get("sub_comment_has_more")
            if not sub_comment_has_more:
                continue

            root_comment_id = comment.get("id")
            sub_comment_cursor = comment.get("sub_comment_cursor")
        
            while sub_comment_has_more:
                comments_res = await self.get_note_sub_comments(note_id, root_comment_id, 10, sub_comment_cursor)
                sub_comment_has_more = comments_res.get("has_more", False)
                sub_comment_cursor = comments_res.get("cursor", "")
                if "comments" not in comments_res:
                    self.logger.error(f"未能爬取到{note_id}的{sub_comment_cursor}二级评论数据")
                    # utils.logger.info(
                    #     f"[XiaoHongShuClient.get_comments_all_sub_comments] No 'comments' key found in response: {comments_res}")
                    break
                comments = comments_res["comments"]
                if callback:
                    await callback(note_id, comments)
                await asyncio.sleep(crawl_interval)
                result.extend(comments)
        return result

    async def get_hotlist_keyword_csv(self):
        self.logger.info("从csv文件获取热榜信息")
        csv_file_path = f'data/xhs/xhs_1_hotlist_title_{datetime.now().date()}.csv'
        hot_keywords = ""
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            # 创建一个csv.DictReader对象
            reader = csv.reader(csvfile)
            # 遍历CSV文件中的每一行
            async for i, row in enumerate(reader):  
                if i == 0:
                    continue
                if i > 1:
                    hot_keywords += ','
                hot_keywords += row[0]
        return hot_keywords
    
    async def get_hotlist_keyword_db(self):
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
        sql: str = f"select content_id from content where content_crawl_time > {begints}000 and content_source = 'xhs' order by content_crawl_time desc"
        noteIds: List[Dict] = await async_db_conn.query(sql)
        if len(noteIds) > 0:
            return noteIds
        self.logger.info("该时间段小红书爬虫无爬取数据")
        return dict()