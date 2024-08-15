# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 20:03
# @Desc    :
from typing import List

import config

from .kuaishou_store_impl import *


class KuaishouStoreFactory:
    STORES = {
        "csv": KuaishouCsvStoreImplement,
        "db": KuaishouDbStoreImplement,
        "json": KuaishouJsonStoreImplement
    }

    @staticmethod
    def create_store() -> AbstractStore:
        store_class = KuaishouStoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError(
                "[KuaishouStoreFactory.create_store] Invalid save option only supported csv or db or json ...")
        return store_class()

async def update_ks_hotlist(hotlist_item: Dict):
    save_hotlist_item = {
        "hot_title": hotlist_item.get("hot_title"),
        "hot_time": utils.get_current_timestamp(), 
        "hot_rank": hotlist_item.get("hot_rank"),
        "hot_score": int(float(hotlist_item.get("hot_score")[:-1]) * 10000),
        "hot_source": "ks",    
        "hot_video_ids": str(hotlist_item.get("hot_videoIds"))[1:-1]
    }
    # utils.logger.info(f"[store.xhs.update_xhs_hotlist] ks hotlist: {hotlist_item}")
    ksF = KuaishouStoreFactory.create_store()
    content_id = await ksF.store_hotlist(save_hotlist_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 1,
        "content_source": "ks",
        "domain": ""
    }
    await ksF.store_extract_info(extract_info_item)

async def update_kuaishou_video(video_item: Dict, hot_id: None):
    photo_info: Dict = video_item.get("photo", {})
    if not photo_info:
        return 
    video_id = photo_info.get("id")
    if not video_id:
        return
    user_info = video_item.get("author", {})
    # 先存视频链接到mongoDB
    ksF = KuaishouStoreFactory.create_store()
    content_download_id = await ksF.store_video({"video_download_url": photo_info.get("photoUrl", "")})
    save_content_item = {
        "hot_id": hot_id,
        "content_source": "ks",
        "content_id": video_id,
        "content_crawl_time": utils.get_current_timestamp(),
        "content_desc": photo_info.get("caption", ""),
        "content_time": photo_info.get("timestamp"),
        "content_user_id": user_info.get("id"),
        "content_user_nickname": user_info.get("name"),
        "content_viewd_cnt": int(float(photo_info.get("viewCount")[:-1]) * 10000),
        "content_liked_cnt": photo_info.get("realLikeCount"),
        "content_url": f"https://www.kuaishou.com/short-video/{video_id}",
        "content_videos": content_download_id,
    }
    # utils.logger.info(
        # f"[store.kuaishou.update_kuaishou_video] Kuaishou video id:{video_id}, title:{save_content_item.get('content_desc')[:50]}")
    content_id = await ksF.store_content(save_content_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 2,
        "content_source": "ks",
        "domain": ""
    }
    await ksF.store_extract_info(extract_info_item)

async def batch_update_ks_video_comments(video_id: str, comments: List[Dict]):
    if not comments:
        return
    # utils.logger.info(f"[store.kuaishou.batch_update_ks_video_comments] video_id:{video_id}, comments:{comments}")
    for comment_item in comments:
        await update_ks_video_comment(video_id, comment_item)

async def update_ks_video_comment(video_id: str, comment_item: Dict):
    comment_id = comment_item.get("commentId")
    if comment_item.get("subCommentCount", 0) == None:
        subCommentCount = 0
    else:
        subCommentCount = comment_item.get("subCommentCount", 0)
    save_comment_item = {
        "content_id": video_id,
        "comment_id": comment_id,
        "par_comment_id": comment_item.get("replyTo", "0"),
        "comment_crawl_time": utils.get_current_timestamp(),
        "comment_time": comment_item.get("timestamp"),
        "comment_desc": comment_item.get("content"),
        "sub_comment_cnt": subCommentCount,
        "comment_liked_cnt": comment_item.get("realLikedCount"),
        "comment_user_id": comment_item.get("authorId"),
        "comment_user_nickname": comment_item.get("authorName"),
        "comment_source": "ks"
    }
    # utils.logger.info(
        # f"[store.kuaishou.update_ks_video_comment] Kuaishou video comment: {comment_id}, content: {comment_item.get('comment_desc')}")
    ksF = KuaishouStoreFactory.create_store()
    content_id = await ksF.store_comment(save_comment_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 3,
        "content_source": "ks",
        "domain": ""
    }
    await ksF.store_extract_info(extract_info_item)
