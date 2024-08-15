import argparse

import config
from tools.utils import str2bool


async def parse_cmd():
    # 读取command arg
    parser = argparse.ArgumentParser(description='Media crawler program.')
    parser.add_argument('--platform', type=str, help='Media platform select (xhs | ks | wb | zh)', # 平台选择
                        choices=["xhs", "ks", "wb", "zh"], default=config.PLATFORM)
    parser.add_argument('--lt', type=str, help='Login type (qrcode | phone | cookie)', # 登陆方式
                        choices=["qrcode", "phone", "cookie"], default=config.LOGIN_TYPE)
    parser.add_argument('--type', type=str, help='crawler type (search | detail | hotlist | hotlist_detail | update)', # 爬取类型，search搜索关键字
                        choices=["search", "detail", "hotlist", "hotlist_detail", "update"], default=config.CRAWLER_TYPE)
    parser.add_argument('--start', type=int,
                        help='number of start page', default=config.START_PAGE)
    parser.add_argument('--keywords', type=str,
                        help='please input keywords', default=config.KEYWORDS)
    parser.add_argument('--get_comment', type=str2bool, # 获取评论
                        help='''whether to crawl level one comment, supported values case insensitive ('yes', 'true', 't', 'y', '1', 'no', 'false', 'f', 'n', '0')''', default=config.ENABLE_GET_COMMENTS)
    parser.add_argument('--get_sub_comment', type=str2bool, # 获取子评论
                        help=''''whether to crawl level two comment, supported values case insensitive ('yes', 'true', 't', 'y', '1', 'no', 'false', 'f', 'n', '0')''', default=config.ENABLE_GET_SUB_COMMENTS)
    parser.add_argument('--save_data_option', type=str, # 保存数据形式
                        help='where to save the data (csv or db or json)', choices=['csv', 'db', 'json'], default=config.SAVE_DATA_OPTION)
    parser.add_argument('--cookies', type=str, 
                        help='cookies used for cookie login type', default=config.COOKIES)
    parser.add_argument('--update_date', type=str,
                        help='beginning date used for update ex. "20240807"', default=config.UPDATE_DATE)
    args = parser.parse_args()

    # override config
    config.PLATFORM = args.platform
    config.LOGIN_TYPE = args.lt
    config.CRAWLER_TYPE = args.type
    config.START_PAGE = args.start
    config.KEYWORDS = args.keywords
    config.ENABLE_GET_COMMENTS = args.get_comment
    config.ENABLE_GET_SUB_COMMENTS = args.get_sub_comment
    config.SAVE_DATA_OPTION = args.save_data_option
    config.COOKIES = args.cookies
    config.UPDATE_DATE = args.update_date
