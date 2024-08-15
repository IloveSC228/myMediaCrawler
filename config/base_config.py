# 基础配置
PLATFORM = "zh"
KEYWORDS = "奥运会"
LOGIN_TYPE = "qrcode"  # qrcode or phone or cookie
UPDATE_DATE = "2024-08-08" # 更新起止时间，默认更新今天爬取的帖子
COOKIES = ""
# 具体值参见media_platform.xxx.field下的枚举值，暂时只支持小红书
SORT_TYPE = "popularity_descending"
# 具体值参见media_platform.xxx.field下的枚举值，暂时只支持抖音
PUBLISH_TIME_TYPE = 0
CRAWLER_TYPE = "update"  # 爬取类型，search(关键词搜索) | detail(帖子详情) | hotlist(热榜标题) 
# | hotlist_detail(爬取热榜相关帖子或视频) | update(更新指定时间内爬取到的帖子以及其评论)

# 是否开启 IP 代理
ENABLE_IP_PROXY = False

# 代理IP池数量
IP_PROXY_POOL_COUNT = 2

# 代理IP提供商名称
IP_PROXY_PROVIDER_NAME = "kuaidaili"

# 设置为True不会打开浏览器（无头浏览器）
# 设置False会打开一个浏览器
# 小红书如果一直扫码登录不通过，打开浏览器手动过一下滑动验证码
# 抖音如果一直提示失败，打开浏览器看下是否扫码登录之后出现了手机号验证，如果出现了手动过一下再试。
HEADLESS = False

# 是否保存登录状态
SAVE_LOGIN_STATE = True

# 数据保存类型选项配置,支持三种类型：csv、db、json
SAVE_DATA_OPTION = "db"  # csv or db or json

# 用户浏览器缓存的浏览器文件配置
USER_DATA_DIR = "%s_user_data_dir"  # %s will be replaced by platform name

# 爬取开始页数 默认从第一页开始
START_PAGE = 1

# 爬取视频/帖子的数量控制，也限制了知乎每个问题的获取回答数，
CRAWLER_MAX_NOTES_COUNT = 40

# 爬取评论数量控制
CRAWLER_MAX_COMMENT_COUNT = 100

# 并发爬虫数量控制
MAX_CONCURRENCY_NUM = 4

# 是否开启爬图片模式, 默认不开启爬图片
ENABLE_GET_IMAGES = True

# 是否开启爬评论模式, 默认不开启爬评论
ENABLE_GET_COMMENTS = True

# 是否开启爬二级评论模式, 默认不开启爬二级评论, 目前仅支持 xhs, bilibili
# 老版本项目使用了 db, 则需参考 schema/tables.sql line 287 增加表字段
ENABLE_GET_SUB_COMMENTS = False

# 指定小红书需要爬虫的笔记ID列表
XHS_SPECIFIED_ID_LIST = [
    "65198434000000001d014907",
    "66934d18000000000d00e940",
    # ........................
]
# 小红书已爬取评论数
XHS_CRAWLER_COMMENT_CNT = 0

# 指定快手平台需要爬取的ID列表
KS_SPECIFIED_ID_LIST = [
    "3xf8enb8dbj6uig",
    "3x6zz972bchmvqe"
]

# 快手已爬取评论数
KS_CRAWLER_COMMENT_CNT = 0

# 指定微博平台需要爬取的帖子列表
WEIBO_SPECIFIED_ID_LIST = [
    "4982041758140155",
    # ........................
]

# 微博已爬取评论数
WB_CRAWLER_COMMENT_CNT = 0

# 知乎的指定id爬取
ZH_QUESTION_ID_LIST = {
    
}
ZH_ANSWER_ID_LIST = [
    "3571282924",
    "3571511470",
    "3571314569",
    "3519438100",
    "3571599152",
    "3571106356",
    "3569474595",
    "3569446065",
    "3572094246"
]

# 知乎已爬取评论数
ZH_CRAWLER_COMMENT_CNT = 0

#词云相关
#是否开启生成评论词云图
ENABLE_GET_WORDCLOUD = False
# 自定义词语及其分组
#添加规则：xx:yy 其中xx为自定义添加的词组，yy为将xx该词组分到的组名。
CUSTOM_WORDS = {
    '零几': '年份',  # 将“零几”识别为一个整体
    '高频词': '专业术语'  # 示例自定义词
}

#停用(禁用)词文件路径
STOP_WORDS_FILE = "./docs/hit_stopwords.txt"

#中文字体文件路径
FONT_PATH= "./docs/STZHONGS.TTF"
