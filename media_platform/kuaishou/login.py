import asyncio
import functools
import sys
from typing import Optional

from playwright.async_api import BrowserContext, Page
from tenacity import (RetryError, retry, retry_if_result, stop_after_attempt,
                      wait_fixed)

import config
from base.base_crawler import AbstractLogin
from tools import utils


class KuaishouLogin(AbstractLogin):
    def __init__(self,
                 login_type: str,
                 browser_context: BrowserContext,
                 context_page: Page,
                 login_phone: Optional[str] = "",
                 cookie_str: str = ""
                 ):
        config.LOGIN_TYPE = login_type
        self.browser_context = browser_context
        self.context_page = context_page
        self.login_phone = login_phone
        self.cookie_str = cookie_str
        self.logger = utils.get_logger("ks")

    async def begin(self):
        """Start login xiaohongshu"""
        self.logger.info("快手爬虫开始登录")
        # utils.logger.info("[KuaishouLogin.begin] Begin login kuaishou ...")
        if config.LOGIN_TYPE == "qrcode":
            await self.login_by_qrcode()
        elif config.LOGIN_TYPE == "phone":
            await self.login_by_mobile()
        elif config.LOGIN_TYPE == "cookie":
            await self.login_by_cookies()
        else:
            raise ValueError("[KuaishouLogin.begin] Invalid Login Type Currently only supported qrcode or phone or cookie ...")

    @retry(stop=stop_after_attempt(600), wait=wait_fixed(1), retry=retry_if_result(lambda value: value is False))
    async def check_login_state(self) -> bool:
        """
            Check if the current login status is successful and return True otherwise return False
            retry decorator will retry 20 times if the return value is False, and the retry interval is 1 second
            if max retry times reached, raise RetryError
        """
        current_cookie = await self.browser_context.cookies()
        _, cookie_dict = utils.convert_cookies(current_cookie)
        kuaishou_pass_token = cookie_dict.get("passToken")
        if kuaishou_pass_token:
            return True
        return False

    async def login_by_qrcode(self):
        """login kuaishou website and keep webdriver login state"""
        self.logger.info("快手平台二维码登录")
        # utils.logger.info("[KuaishouLogin.login_by_qrcode] Begin login kuaishou by qrcode ...")

        # click login button
        login_button_ele = self.context_page.locator(
            "xpath=//p[text()='登录']"
        )
        await login_button_ele.click()

        # find login qrcode
        qrcode_img_selector = "//div[@class='qrcode-img']//img"
        base64_qrcode_img = await utils.find_login_qrcode(
            self.context_page,
            selector=qrcode_img_selector
        )
        if not base64_qrcode_img:
            self.logger.error("快手爬虫未能找到登录二维码")
            # utils.logger.info("[KuaishouLogin.login_by_qrcode] login failed , have not found qrcode please check ....")
            sys.exit()


        # show login qrcode
        partial_show_qrcode = functools.partial(utils.show_qrcode, base64_qrcode_img)
        asyncio.get_running_loop().run_in_executor(executor=None, func=partial_show_qrcode)

        # utils.logger.info(f"[KuaishouLogin.login_by_qrcode] waiting for scan code login, remaining time is 20s")
        try:
            await self.check_login_state()
        except RetryError:
            self.logger.error("快手爬虫二维码登录失败")
            # utils.logger.info("[KuaishouLogin.login_by_qrcode] Login kuaishou failed by qrcode login method ...")
            sys.exit()

        wait_redirect_seconds = 5
        self.logger.info("快手爬虫登录成功")
        # utils.logger.info(f"[KuaishouLogin.login_by_qrcode] Login successful then wait for {wait_redirect_seconds} seconds redirect ...")
        await asyncio.sleep(wait_redirect_seconds)

    async def login_by_mobile(self):
        pass

    async def login_by_cookies(self):
        self.logger.info("快手爬虫cookie登录")
        # utils.logger.info("[KuaishouLogin.login_by_cookies] Begin login kuaishou by cookie ...")
        for key, value in utils.convert_str_cookie_to_dict(self.cookie_str).items():
            await self.browser_context.add_cookies([{
                'name': key,
                'value': value,
                'domain': ".kuaishou.com",
                'path': "/"
            }])
        self.logger.info("快手爬虫cookie登录成功")