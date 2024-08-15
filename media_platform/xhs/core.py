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
from store import xhs as xhs_store
from tools import utils
from var import crawler_type_var

from .client import XiaoHongShuClient
from .exception import DataFetchError
from .field import SearchSortType
from .login import XiaoHongShuLogin


class XiaoHongShuCrawler(AbstractCrawler):
    context_page: Page
    xhs_client: XiaoHongShuClient
    browser_context: BrowserContext

    def __init__(self) -> None:
        self.index_url = "https://www.xiaohongshu.com"
        self.user_agent = utils.get_user_agent() # 获取随机代理
        self.logger = utils.get_logger("xhs")

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
            # 该脚本用于防止检测为爬虫
            # stealth.min.js is a js script to prevent the website from detecting the crawler.
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            # 添加cookie绕过滑动验证码，先登录后验证，不然读不到登录二维码
            # add a cookie attribute webId to avoid the appearance of a sliding captcha on the webpage
            await self.browser_context.add_cookies([{
                'name': "webId",
                'value': "xxx123",  # any value
                'domain': ".xiaohongshu.com",
                'path': "/"
            }])
            self.context_page = await self.browser_context.new_page() # 创建新的浏览器页面
            await self.context_page.goto(self.index_url) # 导航到设置页面"https://www.xiaohongshu.com"
            # 创建一个客户端xhs_client，用于与小红书网站交互。这个客户端可能使用之前设置的HTTP代理格式。
            # Create a client to interact with the xiaohongshu website.
            self.xhs_client = await self.create_xhs_client(httpx_proxy_format)
            if not await self.xhs_client.pong(): # 检查能否登陆上
                login_obj = XiaoHongShuLogin(
                    login_type=config.LOGIN_TYPE,
                    login_phone="",  # input your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES
                )
                await login_obj.begin()
                await self.xhs_client.update_cookies(browser_context=self.browser_context) # 这里更新上下文cookies，可无需重复多次登录
            # 设置爬虫类型
            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                # Search for notes and retrieve their comment information. 搜索关键词
                self.logger.info("调用小红书爬虫关键词搜索功能")
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # Get the information and comments of the specified post
                await self.get_specified_notes()
            elif config.CRAWLER_TYPE == "hotlist":
                # Get hot title
                # await self.get_hotlist()
                pass
            elif config.CRAWLER_TYPE == "hotlist_detail":
                # Get hotlist cotents
                # xhs 无热榜，故调用wb的热榜
                self.logger.info("调用小红书爬虫获取热榜具体信息功能")
                await self.get_hotlist_detail() 
            elif config.CRAWLER_TYPE == "update":
                # Update contents and comments between chosen date
                self.logger.info("调用小红书爬虫更新功能")
                await self.update()
            else:
                pass
            self.logger.info("小红书爬取结束")
            # utils.logger.info("[XiaoHongShuCrawler.start] Xhs Crawler finished ...")

    async def search(self, hot_titles: Dict = None) -> None:
        """Search for notes and retrieve their comment information."""
        # utils.logger.info("[XiaoHongShuCrawler.search] Begin search xiaohongshu keywords")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        if config.CRAWLER_TYPE == "hotlist_detail" and hot_titles is not None:
            task_list = [
                self.search_task(hot_title, semaphore, hot_id) for hot_id, hot_title in hot_titles.items()
            ]
            # for hot_id, hot_title in hot_titles.items():
            #     await self.search_task(hot_title, semaphore, hot_id)
        else:
            task_list = [
                self.search_task(keyword, semaphore) for keyword in config.KEYWORDS.split(",")
            ]
        await asyncio.gather(*task_list)

    async def search_task(self, keyword, semaphore: asyncio.Semaphore, hot_id=None):
        async with semaphore:
            xhs_limit_count = 20  # xhs limit page fixed value
            if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
                config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
            start_page = config.START_PAGE
            # utils.logger.info(f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * xhs_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                self.logger.info(f"小红书爬虫搜索关键词{keyword}页面{page}")
                if page < start_page:
                    # utils.logger.info(f"[XiaoHongShuCrawler.search] Skip page {page}")
                    page += 1
                    continue
                try:
                    note_id_list: List[str] = [] # 存储账户ID
                    notes_res = await self.xhs_client.get_note_by_keyword( # 根据关键词搜索笔记
                        keyword=keyword,
                        page=page,
                        sort=SearchSortType(config.SORT_TYPE) if config.SORT_TYPE != '' else SearchSortType.GENERAL,
                    )
                    # utils.logger.info(f"[XiaoHongShuCrawler.search] Search notes res:{notes_res[:20]}")
                    if(not notes_res or not notes_res.get('has_more', False)):
                        self.logger.info(f"小红书爬虫搜索关键词{keyword}页面{page}没有更多数据")
                        # utils.logger.info("No more content!")
                        break
                    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM) # 控制并发数量
                    # 创建并发任务列表
                    task_list = [
                        self.get_note_detail(post_item.get("id"), semaphore)
                        for post_item in notes_res.get("items", {})
                        if post_item.get('model_type') not in ('rec_query', 'hot_query')
                    ]
                    note_details = await asyncio.gather(*task_list) # 异步并发执行任务列表
                    for note_detail in note_details:
                        if note_detail is not None:
                            content_comment = note_detail.get("interact_info", {}).get("comment_count", "0")
                            if content_comment[-1] == '万':
                                content_comment_cnt = int(float(content_comment[:-1]) * 10000)
                            else:
                                content_comment_cnt = int(content_comment)
                            if content_comment_cnt > 0:
                                note_id_list.append(note_detail.get("note_id"))
                            pic_id = await self.get_note_images(note_detail) # 把存到mongoDB里的图片id存到mysql里去
                            await xhs_store.update_xhs_note(note_detail, hot_id, pic_id)
                    page += 1
                    # utils.logger.info(f"[XiaoHongShuCrawler.search] Note details: {note_details}")
                    await self.batch_get_note_comments(note_id_list)
                except DataFetchError:
                    self.logger.error(f"小红书爬虫搜索关键词{keyword}数据获取失败")
                    # utils.logger.error("[XiaoHongShuCrawler.search] Get note detail error")
                    break

    async def get_specified_notes(self):
        """Get the information and comments of the specified post"""
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_detail(note_id=note_id, semaphore=semaphore) for note_id in config.XHS_SPECIFIED_ID_LIST
        ]
        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail is not None:
                pic_id = self.get_note_images(note_detail)
                await xhs_store.update_xhs_note(note_detail, pic_id=pic_id)
        await self.batch_get_note_comments(config.XHS_SPECIFIED_ID_LIST)

    async def get_note_detail(self, note_id: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """Get note detail"""
        async with semaphore:
            try:
                self.logger.info(f"获取帖子{note_id}信息")
                return await self.xhs_client.get_note_by_id(note_id)
            except DataFetchError as ex:
                self.logger.error(f"获取视帖子{note_id}信息数据出错: {ex}")
                # utils.logger.error(f"[XiaoHongShuCrawler.get_note_detail] Get note detail error: {ex}")
                return None
            except KeyError as ex:
                self.logger.error(f"获取帖子{note_id}信息出错: {ex}")
                # utils.logger.error(
                #     f"[XiaoHongShuCrawler.get_note_detail] have not fund note detail note_id:{note_id}, err: {ex}")
                return None

    async def batch_get_note_comments(self, note_list: List[str]):
        """Batch get note comments"""
        if not config.ENABLE_GET_COMMENTS:
            # utils.logger.info(f"[XiaoHongShuCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return
        self.logger.info(f"获取帖子列表{note_list}评论")
        # utils.logger.info(
        #     f"[XiaoHongShuCrawler.batch_get_note_comments] Begin batch get note comments, note list: {note_list}")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for note_id in note_list:
            task = asyncio.create_task(self.get_comments(note_id, semaphore), name=note_id)
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_comments(self, note_id: str, semaphore: asyncio.Semaphore):
        """Get note comments with keyword filtering and quantity limitation"""
        async with semaphore:
            self.logger.info(f"获取帖子{note_id}评论")
            # utils.logger.info(f"[XiaoHongShuCrawler.get_comments] Begin get note id comments {note_id}")
            await self.xhs_client.get_note_all_comments(
                note_id=note_id,
                crawl_interval=random.random(),
                callback=xhs_store.batch_update_xhs_note_comments
            )

    async def get_note_images(self, note_detail: Dict):
        """
        get note images
        :param mblog:
        :return:
        """
        if not config.ENABLE_GET_IMAGES:
            # utils.logger.info(f"[XiaoHongShu.get_note_images] Crawling image mode is not enabled")
            return
        
        pics: Dict = note_detail.get("image_list", [])
        if not pics:
            return
        pic_urls = []
        from .help import (get_trace_id, get_img_url_by_trace_id)
        for pic in pics:
            url = pic.get("url")
            if not url:
                continue
            pic_url = get_img_url_by_trace_id(get_trace_id(url))
            if pic_url != None:
                pic_urls.append(pic_url)
        pic_id = await xhs_store.update_xhs_note_image(pic_urls)
        return pic_id

    async def get_hotlist(self):
        """Get hotlist"""
        self.logger.info("获取小红书热榜数据")
        # utils.logger.info("[XiaoHongShuCrawler.get_hotlist] Begin get xiaohongshu hotlist")
        try:
            hotlist_res = await self.xhs_client.get_hotlist()
            # utils.logger.info(f"[XiaoHongShuCrawler.get_hotlist] Hotlist res:{hotlist_res}")
            rank = 1
            for hotlist_item in hotlist_res['items']:
                if hotlist_item is not None:
                    await xhs_store.update_xhs_hotlist(hotlist_item, rank)
                    rank += 1
        except DataFetchError as ex:
            self.logger.error(f"获取小红书热榜数据错误: {ex}")
            # utils.logger.error(f"[XiaoHongShuCrawler.get_hotlist] Get hotlist error: {ex}")
            return None
        except KeyError as ex:
            self.logger.error(f"获取小红书热榜错误: {ex}")
            # utils.logger.error(
            #     f"[XiaoHongShuCrawler.get_hotlist] have not found hotlist, err: {ex}")
            return None

    async def get_hotlist_detail(self):
        """Get hotlist video"""
        self.logger.info("获取热榜具体信息")
        # utils.logger.info("[XiaoHongShuCrawler.get_hotlist_content] Begin get xiaohongshu hotlist")
        if config.SAVE_DATA_OPTION == "csv":
            config.KEYWORDS = await self.xhs_client.get_hotlist_keyword_csv()
            await self.search()
        else:
            hot_titles = await self.xhs_client.get_hotlist_keyword_db()
            await self.search(hot_titles)    

    async def update(self):
        update_list = await self.xhs_client.get_update_noteIds_db()
        config.XHS_SPECIFIED_ID_LIST = [item['content_id'] for item in update_list]
        self.logger.info(f"小红书爬虫需要更新的帖子数: {len(config.WEIBO_SPECIFIED_ID_LIST)}")
        await self.batch_get_note_comments(config.XHS_SPECIFIED_ID_LIST)

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

    async def create_xhs_client(self, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        """Create xhs client"""
        # utils.logger.info("[XiaoHongShuCrawler.create_xhs_client] Begin create xiaohongshu API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        xhs_client_obj = XiaoHongShuClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": cookie_str,
                "Origin": "https://www.xiaohongshu.com",
                "Referer": "https://www.xiaohongshu.com",
                "Content-Type": "application/json;charset=UTF-8"
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return xhs_client_obj

    async def launch_browser(
            self,
            chromium: BrowserType,
            playwright_proxy: Optional[Dict],
            user_agent: Optional[str],
            headless: bool = True
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        # utils.logger.info("[XiaoHongShuCrawler.launch_browser] Begin create browser context ...")
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
        # utils.logger.info("[XiaoHongShuCrawler.close] Browser context closed ...")