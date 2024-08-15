# -*- coding: utf-8 -*-
# @Author  : syj
# @Time    : 2024/7/14 15:41
# @Desc    : 知乎爬虫主流程代码

import asyncio
import os
import random
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

import config
from base.base_crawler import AbstractCrawler
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import zhihu as zh_store
from tools import utils
from var import crawler_type_var

from .client import ZhiHuClient
from .exception import DataFetchError
from .field import SearchType
from .login import ZhiHuLogin

class ZhiHuCrawler(AbstractCrawler):
    context_page: Page
    zh_client: ZhiHuClient
    browser_context: BrowserContext

    def __init__(self):
        self.index_url = "https://www.zhihu.com"
        self.user_agent = utils.get_user_agent() # 获取随机代理
        self.logger = utils.get_logger("zh")

    async def start(self) -> None:
        # 存储Playwright和httpx库所需的代理格式
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY: # 如果启用代理
            # 调用创建ip池函数
            ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            # Launch a browser context.
            chromium = playwright.chromium # 调用chorm浏览器
            # 设置浏览器上下文
            self.browser_context = await self.launch_browser(  
                chromium,
                None,
                self.user_agent,
                headless=config.HEADLESS
            )
            # stealth.min.js is a js script to prevent the website from detecting the crawler.
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            # add a cookie attribute webId to avoid the appearance of a sliding captcha on the webpage 爬小红书要cookie爬知乎不知道要不要
            # await self.browser_context.add_cookies([{
            #     'name': "webId",
            #     'value': "xxx123",  # any value
            #     'domain': ".zhihu.com",
            #     'path': "/"
            # }])
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)
            
            # 创建一个客户端zh_client，用于与知乎网站交互。这个客户端可能使用之前设置的HTTP代理格式。
            # Create a client to interact with the zhihu website.
            self.zh_client = await self.create_zh_client(httpx_proxy_format)
            if not await self.zh_client.pong(): # 检查能否登陆上
                login_obj = ZhiHuLogin(
                    login_type=config.LOGIN_TYPE,
                    login_phone="",  # input your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES
                )
                await login_obj.begin()
                await self.zh_client.update_cookies(browser_context=self.browser_context) # 这里更新上下文cookies，可无需重复多次登录
            # 设置爬虫类型
            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                # Search for notes and retrieve their comment information. 搜索关键词
                self.logger.info("调用知乎爬虫关键词搜索功能")
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # Get the information and comments of the specified post
                # await self.get_specified_notes()
                pass
            elif config.CRAWLER_TYPE == "hotlist":
                self.logger.info("调用知乎爬虫获取热榜功能")
                await self.get_hotlist()
            elif config.CRAWLER_TYPE == "hotlist_detail":
                self.logger.info("调用知乎爬虫获取热榜具体信息功能")
                await self.get_hotlist_detail()
            elif config.CRAWLER_TYPE == "update":
                self.logger.info("调用知乎爬虫获取热榜更新功能")
                await self.update_all_comments()
            else:
                pass
            self.logger.info("知乎爬取结束")
            # utils.logger.info("[ZhiHuCrawler.start] Zh Crawler finished ...")

    async def search(self) -> None:
        """Search for notes and retrieve their comment information."""
        
        # utils.logger.info("[ZhiHuCrawler.search] Begin search ZhiHu keywords")
        zh_limit_count = 20  # zh limit page fixed value
        if config.CRAWLER_MAX_NOTES_COUNT < zh_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = zh_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            self.logger.info(f"搜索关键词{keyword}")
            # utils.logger.info(f"[ZhiHuCrawler.search] Current search keyword: {keyword}")
            page = 1
            next_url = '' # 获取到的下一个page的请求url
            config.ZH_ANSWER_ID_LIST = []
            while (page - start_page + 1) * zh_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                self.logger.info(f"知乎爬虫搜索关键词{keyword}页面{page}")
                if page < start_page:
                    # utils.logger.info(f"[ZhiHuCrawler.search] Skip page {page}")
                    page += 1
                    continue
                try:
                    # 根据关键词搜索笔记
                    from .search import search_by_keyword
                    search_notes, paging = await search_by_keyword(q=keyword, next=next_url)
                    # utils.logger.info(f"[ZhiHuCrawler.search] Search notes res:{search_notes}")
                    if search_notes:
                        # 创建并发任务列表
                        for note in search_notes:
                            if note.get("type") == "search_result" and note.get("object").get("type") == "answer":
                                config.ZH_ANSWER_ID_LIST.append(note.get("object").get("id"))
                                await zh_store.update_zhihu_search_note(note.get("object"))
                        page += 1
                        await self.batch_get_note_comments(config.ZH_ANSWER_ID_LIST, "answers")
                        if paging.get("is_end", ""):
                            self.logger.info(f"知乎搜索关键词{keyword}页面{page}没有更多数据")
                            break
                        else:
                            next_url = paging.get("next", "")
                    else:
                        break
                except DataFetchError:
                    self.logger.error(f"知乎爬虫搜索关键词{keyword}数据获取失败")
                    # utils.logger.error("[ZhiHuCrawler.search] Get note detail error")
                    break


    # async def get_specified_notes(self):
    #     """Get the information and comments of the specified post"""
    #     if config.CRAWLER_TYPE == "hotlist_detail":
    #         config.ZH_ANSWER_ID_LIST = []
    #         for question_id in config.ZH_QUESTION_ID_LIST:
    #             config.ZH_ANSWER_ID_LIST.extend(await self.zh_client.get_answerIds(question_id))
    #     semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
    #     # task_list = [
    #     #     self.get_question_detail(question_id, semaphore=semaphore) 
    #     #     for question_id in config.ZH_QUESTION_ID_LIST
    #     # ] 
    #     # question_details = await asyncio.gather(*task_list)
    #     # for question_detail in question_details:
    #     #     if question_detail is not None:
    #     #         await zh_store.update_zh_question(question_detail)
    #     task_list1 = [
    #         self.get_answer_detail(note_id=answer_id, semaphore=semaphore)
    #         for answer_id in config.ZH_ANSWER_ID_LIST
    #     ]
    #     answer_details = await asyncio.gather(*task_list1)
    #     for answer_detail in answer_details:
    #         if answer_detail is not None:
    #             await zh_store.update_zhihu_answer(answer_detail)
    #     await self.batch_get_note_comments(config.ZH_QUESTION_ID_LIST, "question")
    #     await self.batch_get_note_comments(config.ZH_ANSWER_ID_LIST, "answer")

    async def batch_get_note_comments(self, note_list: List[str], note_type: str):
        """Batch get note comments"""
        if not config.ENABLE_GET_COMMENTS:
            # utils.logger.info(f"[ZhiHuCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return
        self.logger.info(f"获取帖子列表{note_list}评论")
        # utils.logger.info(
        #     f"[ZhiHuCrawler.batch_get_note_comments] Begin batch get note comments, note list: {note_list}")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for note_id in note_list:
            task = asyncio.create_task(self.get_comments(note_id, semaphore, note_type), name=note_id)
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_comments(self, note_id: str, semaphore: asyncio.Semaphore, note_type: str):
        """Get note comments with keyword filtering and quantity limitation"""
        async with semaphore:
            self.logger.info(f"获取帖子{note_id}评论")
            # utils.logger.info(f"[ZhiHuCrawler.get_comments] Begin get note id comments {note_id}")
            await self.zh_client.get_note_all_comments(
                note_id=note_id,
                note_type=note_type,
                crawl_interval=random.random(),
                callback=zh_store.batch_update_zhihu_note_comments
            )

    async def get_hotlist(self):
        """Get hotlist"""
        self.logger.info("获取知乎热榜数据")
        # utils.logger.info("[ZhiHuCrawler.get_hotlist] Begin get zhihu hotlist")
        try:
            hotlist_res = await self.zh_client.get_hotlist()
            # utils.logger.info(f"[ZhiHuCrawler.get_hotlist] Hotlist res:{hotlist_res}")
            rank = 1
            for hotlist_item in hotlist_res:
                if hotlist_item is not None:
                    await zh_store.update_zhihu_hotlist(hotlist_item, rank)
                    rank += 1
        except DataFetchError as ex:
            self.logger.error(f"获取知乎热榜数据错误: {ex}")
            # utils.logger.error(f"[ZhiHuCrawler.get_hotlist] Get hotlist error: {ex}")
            return None
        except KeyError as ex:
            self.logger.error(f"获取知乎热榜错误: {ex}")
            # utils.logger.error(
            #     f"[ZhiHuCrawler.get_hotlist] have not found hotlist, err: {ex}")
            return None

    async def get_hotlist_detail(self):
        """Get hotlist detail 获取热榜问题下的所有回答，同时获取问题和回答的评论"""
        self.logger.info("获取热榜具体信息")
        # utils.logger.info("[ZhiHuCrawler.get_hotlist_video] Begin get zhihu hotlist")
        if config.SAVE_DATA_OPTION == "csv":
            config.ZH_QUESTION_ID_LIST = await self.zh_client.get_hotlist_questionIds_csv()
        else: 
            config.ZH_QUESTION_ID_LIST = await self.zh_client.get_hotlist_questionIds_db()
        config.ZH_ANSWER_ID_LIST = []
        await self.zh_client.update_cookies(browser_context=self.browser_context)
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for hot_id, question_id in config.ZH_QUESTION_ID_LIST.items():
            task = asyncio.create_task(self.get_answers_by_question(question_id, semaphore, hot_id), name=question_id)
            task_list.append(task)
        await asyncio.gather(*task_list)
        await self.batch_get_note_comments(config.ZH_QUESTION_ID_LIST.values(), "questions")
        await self.batch_get_note_comments(config.ZH_ANSWER_ID_LIST, "answers")

    async def get_answers_by_question(self, question_id, semaphore: asyncio.Semaphore, hot_id: None):
        async with semaphore:
            self.logger.info(f"获取问题{question_id}下问题信息")
            # utils.logger.info(f"[ZhiHuCrawler.get_answers_by_question] Begin get answers by question_id: {question_id}")
            if config.CRAWLER_MAX_NOTES_COUNT < 5:
                config.CRAWLER_MAX_NOTES_COUNT = 5
            next = ""
            cursor = ""
            session_id = ""
            for i in range(0, int(config.CRAWLER_MAX_NOTES_COUNT / 5)):
                answer_res = []
                answer_res, next = await self.zh_client.get_answers(
                    question_id=question_id,
                    cursor=cursor,
                    offset=i,
                    session_id=session_id
                )
                if next is not None:
                    cursor = next.split('cursor=')[1].split('&')[0]
                    session_id = next.split('&session_id=')[1]
                else:
                    self.logger.info(f"问题{question_id}没有更多问题")
                    # utils.logger.info("No more answers!")
                    break
                for answer_item in answer_res:
                    if answer_item is not None:
                        await zh_store.update_zhihu_answer(answer_item, hot_id)

    async def update_all_comments(self):
        """update_all_comments 更新id列表下帖子的评论"""
        update_noteIds = await self.zh_client.get_update_noteIds_db()
        # 按时间更新
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for question_id, value in update_noteIds.items():
            task = asyncio.create_task(self.update_comments(question_id, "questions", semaphore), name=question_id)
            task_list.append(task)
            for answer_id in value:
                task = asyncio.create_task(self.update_comments(answer_id, "answers", semaphore), name=answer_id)
                task_list.append(task)
        await asyncio.gather(*task_list)
        # 按默认更新
        # config.ZH_QUESTION_ID_LIST = []
        # config.ZH_ANSWER_ID_LIST = []
        # for question_id, value in update_noteIds.items():
        #     config.ZH_QUESTION_ID_LIST.append(question_id)
        #     config.ZH_ANSWER_ID_LIST.extend(value)
        # await self.batch_get_note_comments(config.ZH_QUESTION_ID_LIST, "questions")
        # await self.batch_get_note_comments(config.ZH_ANSWER_ID_LIST, "answers")

    async def update_comments(self, note_id, note_type, semaphore: asyncio.Semaphore):
        async with semaphore:
            self.logger.info(f"更新帖子{note_id}下评论")
            latest_time = await self.zh_client.get_latest_comment_time(note_id, note_type)
            await self.zh_client.update_note_comments(
                note_id=note_id, 
                note_type=note_type, 
                updated_time=latest_time, 
                crawl_interval=random.random(),
                callback=zh_store.batch_update_zhihu_note_comments
            )

    @staticmethod
    def format_proxy_info(ip_proxy_info: IpInfoModel) -> Tuple[Optional[Dict], Optional[Dict]]:
        """format proxy info for playwright and httpx"""
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",
            "username": ip_proxy_info.user,
            "password": ip_proxy_info.password,
        }
        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"
        }
        return playwright_proxy, httpx_proxy

    async def create_zh_client(self, httpx_proxy: Optional[str]) -> ZhiHuClient:
        """Create zh client"""
        # utils.logger.info("[ZhiHuCrawler.create_zh_client] Begin create zhihu API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        zh_client_obj = ZhiHuClient(
            proxies=httpx_proxy,
            headers={
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "priority": "u=1, i",
                "referer": "https://www.zhihu.com",
                "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Google Chrome\";v=\"126\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                "x-api-version": "3.0.91",
                "x-app-za": "OS=Web",
                "x-requested-with": "fetch",
                "x-zse-93": "101_3_3.0",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return zh_client_obj

    async def launch_browser(
            self,
            chromium: BrowserType,
            playwright_proxy: Optional[Dict],
            user_agent: Optional[str],
            headless: bool = True
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        # utils.logger.info("[ZhiHuCrawler.launch_browser] Begin create browser context ...")
        if config.SAVE_LOGIN_STATE:
            # feat issue #14
            # we will save login state to avoid login every time
            user_data_dir = os.path.join(os.getcwd(), "browser_data",
                                         config.USER_DATA_DIR % config.PLATFORM)  # type: ignore
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                accept_downloads=True,
                headless=headless,
                proxy=playwright_proxy,  # type: ignore
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context
        else:
            browser = await chromium.launch(headless=headless, proxy=playwright_proxy)  # type: ignore
            browser_context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context

    async def close(self):
        """Close browser context"""
        await self.browser_context.close()
        # utils.logger.info("[ZhiHuCrawler.close] Browser context closed ...")