from enum import Enum
from typing import NamedTuple


class FeedType(Enum):
    # 关注
    FOLLOW = "/follow"
    # 推荐
    RECOMMEND = ""
    # 热榜
    HOT = "/hot"
    # 视频
    VIDEO = "/zvideo"

class SearchSourceType(Enum):
    NORMAL = "normal"
    Filter = "filter"


# class SearchSortType(Enum):
#     """search sort type"""
#     # default
#     GENERAL = "general"
#     # most popular
#     MOST_POPULAR = "popularity_descending"
#     # Latest
#     LATEST = "time_descending"


class SearchType(Enum):
    """search type
    """
    # 默认综合
    CONTENT = "general"
    # 用户
    PEOPLE = "people"
    # 话题
    TOPIC = "topic"
    # 视频
    ZVIDEO = "zvideo"
    # 专栏
    COLUMN = "column"
    # 盐选内容
    KM_GENERAL = "km_general"
    # 电子书
    PUBLICATION = "publication"


# class Note(NamedTuple):
#     """note tuple"""
#     note_id: str
#     title: str
#     desc: str
#     type: str
#     user: dict
#     img_urls: list
#     video_url: str
#     tag_list: list
#     at_user_list: list
#     collected_count: str
#     comment_count: str
#     liked_count: str
#     share_count: str
#     time: int
#     last_update_time: int
