import asyncio
import os
import random
import time
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

import config
from base.base_crawler import AbstractCrawler
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import kuaishou as kuaishou_store
from tools import utils
from var import comment_tasks_var, crawler_type_var

from .client import KuaiShouClient
from .exception import DataFetchError
from .login import KuaishouLogin


class KuaishouCrawler(AbstractCrawler):
    context_page: Page
    ks_client: KuaiShouClient
    browser_context: BrowserContext

    def __init__(self):
        self.index_url = "https://www.kuaishou.com"
        self.user_agent = utils.get_user_agent()
        self.logger = utils.get_logger("ks")

    async def start(self):
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY:
            ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            # Launch a browser context.
            chromium = playwright.chromium
            self.browser_context = await self.launch_browser(
                chromium,
                None,
                self.user_agent,
                headless=config.HEADLESS
            )
            # stealth.min.js is a js script to prevent the website from detecting the crawler.
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(f"{self.index_url}?isHome=1")

            # Create a client to interact with the kuaishou website.
            self.ks_client = await self.create_ks_client(httpx_proxy_format)
            if not await self.ks_client.pong():
                login_obj = KuaishouLogin(
                    login_type=config.LOGIN_TYPE,
                    login_phone=httpx_proxy_format,
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES
                )
                await login_obj.begin()
                await self.ks_client.update_cookies(browser_context=self.browser_context)

            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                # Search for videos and retrieve their comment information.
                self.logger.info("调用快手爬虫关键词搜索功能")
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # Get the information and comments of the specified post
                await self.get_specified_videos()
            elif config.CRAWLER_TYPE == "hotlist":
                # Get hot title
                self.logger.info("调用快手爬虫热榜获取功能")
                await self.get_hotlist()
            elif config.CRAWLER_TYPE == "hotlist_detail": 
                # Get hotlist contents
                self.logger.info("调用快手爬虫获取热榜具体信息功能")
                await self.get_hotlist_video()
            elif config.CRAWLER_TYPE == "update":
                # Update contents and comments between chosen date
                self.logger.info("调用快手爬虫更新功能")
                await self.update()
            else:
                pass
            self.logger.info("快手爬取结束")
            # utils.logger.info("[KuaishouCrawler.start] Kuaishou Crawler finished ...")

    async def search(self):
        # utils.logger.info("[KuaishouCrawler.search] Begin search kuaishou keywords")
        ks_limit_count = 20  # kuaishou limit page fixed value
        if config.CRAWLER_MAX_NOTES_COUNT < ks_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = ks_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            # utils.logger.info(f"[KuaishouCrawler.search] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * ks_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                self.logger.info(f"快手爬虫搜索关键词{keyword}页面{page}")
                if page < start_page:
                    # utils.logger.info(f"[KuaishouCrawler.search] Skip page: {page}")
                    page += 1
                    continue
                
                video_id_list: List[str] = []
                videos_res = await self.ks_client.search_info_by_keyword(
                    keyword=keyword,
                    pcursor=str(page),
                )
                if not videos_res:
                    self.logger.error(f"快手爬虫搜索关键词{keyword}页面{page}无法搜索到数据")
                    # utils.logger.error(f"[KuaishouCrawler.search] search info by keyword:{keyword} not found data")
                    continue

                vision_search_photo: Dict = videos_res.get("visionSearchPhoto")
                if vision_search_photo.get("result") != 1:
                    self.logger.error(f"快手爬虫搜索关键词{keyword}页面{page}无法搜索到数据")
                    # utils.logger.error(f"[KuaishouCrawler.search] search info by keyword:{keyword} not found data ")
                    continue

                for video_detail in vision_search_photo.get("feeds"):
                    video_id_list.append(video_detail.get("photo", {}).get("id"))
                    await kuaishou_store.update_kuaishou_video(video_item=video_detail)

                # batch fetch video comments
                page += 1
                await self.batch_get_video_comments(video_id_list)

    async def get_specified_videos(self, hot_id=None):
        """Get the information and comments of the specified post"""
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        video_details = []
        task_list = [
            self.get_video_info_task(video_id=video_id, semaphore=semaphore) for video_id in config.KS_SPECIFIED_ID_LIST
        ]
        video_details = await asyncio.gather(*task_list)
        for video_detail in video_details:
            if video_detail is not None:
                await kuaishou_store.update_kuaishou_video(video_detail, hot_id)
        await self.batch_get_video_comments(config.KS_SPECIFIED_ID_LIST)

    async def get_video_info_task(self, video_id: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """Get video detail task"""
        async with semaphore:
            try:
                self.logger.info(f"获取视频{video_id}信息")
                result = await self.ks_client.get_video_info(video_id)
                # utils.logger.info(f"[KuaishouCrawler.get_video_info_task] Get video_id:{video_id} info result: {result} ...")
                if result:
                    res = result.get("visionVideoDetail", "")
                    return res
                return None
            except DataFetchError as ex:
                self.logger.error(f"获取视频{video_id}数据出错: {ex}")
                # utils.logger.error(f"[KuaishouCrawler.get_video_info_task] Get video detail error: {ex}")
                return None
            except KeyError as ex:
                self.logger.error(f"视频{video_id}数据字典键值对错误: {ex}")
                # utils.logger.error(f"[KuaishouCrawler.get_video_info_task] have not fund video detail video_id:{video_id}, err: {ex}")
                return None

    async def batch_get_video_comments(self, video_id_list: List[str]):
        """
        batch get video comments
        :param video_id_list:
        :return:
        """
        if not config.ENABLE_GET_COMMENTS:
            # utils.logger.info(f"[KuaishouCrawler.batch_get_video_comments] Crawling comment mode is not enabled")
            return
        # utils.logger.info(f"[KuaishouCrawler.batch_get_video_comments] video ids:{video_id_list}")
        self.logger.info(f"获取视频列表{video_id_list}评论")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for video_id in video_id_list:
            task = asyncio.create_task(self.get_comments(video_id, semaphore), name=video_id)
            task_list.append(task)

        comment_tasks_var.set(task_list)
        await asyncio.gather(*task_list)

    async def get_comments(self, video_id: str, semaphore: asyncio.Semaphore):
        """
        get comment for video id
        :param video_id:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                self.logger.info(f"获取视频{video_id}评论")
                # utils.logger.info(f"[KuaishouCrawler.get_comments] begin get video_id: {video_id} comments ...")
                await self.ks_client.get_video_all_comments(
                    photo_id=video_id,
                    crawl_interval=random.random(),
                    callback=kuaishou_store.batch_update_ks_video_comments
                )
            except DataFetchError as ex:
                self.logger.error(f"获取视频{video_id}评论数据出错: {ex}")
                # utils.logger.error(f"[KuaishouCrawler.get_comments] get video_id: {video_id} comment error: {ex}")
            except Exception as e:
                self.logger.error(f"获取视频{video_id}评论错误: {ex}")
                # utils.logger.error(f"[KuaishouCrawler.get_comments] may be been blocked, err:{e}")
                # use time.sleeep block main coroutine instead of asyncio.sleep and cacel running comment task
                # maybe kuaishou block our request, we will take a nap and update the cookie again
                current_running_tasks = comment_tasks_var.get()
                for task in current_running_tasks:
                    task.cancel()
                time.sleep(20)
                await self.context_page.goto(f"{self.index_url}?isHome=1")
                await self.ks_client.update_cookies(browser_context=self.browser_context)

    async def get_hotlist(self):
        """Get hotlist"""
        self.logger.info("获取快手热榜数据")
        # utils.logger.info("[KuaishouCrawler.get_hotlist] Begin get kuaishou hotlist")
        try:
            hotlist_res = await self.ks_client.get_hotlist()
            # utils.logger.info(f"[KuaishouCrawler.get_hotlist] Hotlist res:{hotlist_res}")
            for hotlist_item in hotlist_res:
                if hotlist_item is not None:
                    await kuaishou_store.update_ks_hotlist(hotlist_item)
        except DataFetchError as ex:
            self.logger.error(f"获取快手热榜数据错误: {ex}")
            # utils.logger.error(f"[KuaishouCrawler.get_hotlist] Get hotlist error: {ex}")
            return None
        except Exception as ex:
            self.logger.error(f"获取快手热榜错误: {ex}")
            # utils.logger.error(
                # f"[KuaishouCrawler.get_hotlist] have not found hotlist, err: {ex}")
            return None 
    
    async def get_hotlist_video(self):
        """Get hotlist video"""
        self.logger.info("获取热榜具体信息")
        # utils.logger.info("[KuaishouCrawler.get_hotlist_video] Begin get kuaishou hotlist")
        hotlist_videoIds = []
        if config.SAVE_DATA_OPTION == "csv":
            hotlist_videoIds = await self.ks_client.get_hotlist_videoIds_csv()
        else: 
            hotlist_videoIds = await self.ks_client.get_hotlist_videoIds_db()
        semaphore = asyncio.Semaphore(1)
        task_list = [] 
        for key, value in hotlist_videoIds.items(): # key是热榜标题，value是热榜对应的视频ID列表
            task = asyncio.create_task(self.get_hotlist_video_task(key, value, semaphore), name=key)
            task_list.append(task)
        await asyncio.gather(*task_list)
    
    async def get_hotlist_video_task(self, key, value, semaphore):
        async with semaphore:
            self.logger.info(f"获取热榜下#{value}#视频信息")
            config.KS_SPECIFIED_ID_LIST = value
            await self.get_specified_videos(key)

    async def update(self):
        update_videoIds = await self.ks_client.get_update_videoIds_db()
        self.logger.info(f"快手爬虫需要更新的视频数: {len(update_videoIds)}")
        semaphore = asyncio.Semaphore(1)
        task_list = [] 
        for key, value in update_videoIds.items(): # key是热榜标题，value是热榜对应的视频ID列表
            task = asyncio.create_task(self.get_hotlist_video_task(value, [key], semaphore), name=key)
            task_list.append(task)
        await asyncio.gather(*task_list)

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

    async def create_ks_client(self, httpx_proxy: Optional[str]) -> KuaiShouClient:
        """Create ks client"""
        # utils.logger.info("[KuaishouCrawler.create_ks_client] Begin create kuaishou API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        ks_client_obj = KuaiShouClient(
            proxies=httpx_proxy,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Pragma": "no-cache",
                "Origin": self.index_url,
                "Referer": self.index_url,
                # "Referer": "https://www.kuaishou.com/new-reco",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": self.user_agent,
                "sec-ch-ua": "\"Microsoft Edge\";v=\"113\", \"Chromium\";v=\"113\", \"Not-A.Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "Cookie": cookie_str,
                "Content-Type": "application/json;charset=UTF-8"
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return ks_client_obj

    async def launch_browser(
            self,
            chromium: BrowserType,
            playwright_proxy: Optional[Dict],
            user_agent: Optional[str],
            headless: bool = True
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        # utils.logger.info("[KuaishouCrawler.launch_browser] Begin create browser context ...")
        if config.SAVE_LOGIN_STATE:
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
        # utils.logger.info("[KuaishouCrawler.close] Browser context closed ...")

    