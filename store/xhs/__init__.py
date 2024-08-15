# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 17:34
# @Desc    :
from typing import List

import config

from . import xhs_store_impl
from .xhs_store_impl import *


class XhsStoreFactory:
    STORES = {
        "csv": XhsCsvStoreImplement,
        "db": XhsDbStoreImplement,
        "json": XhsJsonStoreImplement,
    }

    @staticmethod
    def create_store() -> AbstractStore:
        store_class = XhsStoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError("[XhsStoreFactory.create_store] Invalid save option only supported csv or db or json ...")
        return store_class()

async def update_xhs_hotlist(hotlist_item: Dict, rank: int):
    save_hotlist_item = {
        "hot_title": hotlist_item.get("title"),
        "hot_time": utils.get_current_timestamp(),
        "hot_rank": rank,
        "hot_score": int(float(hotlist_item.get("score")[:-1]) * 10000),
        # "hot_type": hotlist_item.get("word_type"),
        "hot_source": "xhs",
    }
    # utils.logger.info(f"[store.xhs.update_xhs_hotlist] xhs hotlist: {save_hotlist_item}")
    xhsF = XhsStoreFactory.create_store()
    content_id = await xhsF.store_hotlist(save_hotlist_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 1,
        "content_source": "xhs",
        "domain": ""
    }
    await xhsF.store_extract_info(extract_info_item)

async def update_xhs_note(note_item: Dict, hot_id: None, pic_id):
    note_id = note_item.get("note_id")
    user_info = note_item.get("user", {})
    interact_info = note_item.get("interact_info", {})
    # 处理视频
    video_id = None
    if note_item.get('type') == 'video':
        videos = note_item.get('video').get('media').get('stream').get('h264')
        if type(videos).__name__ == 'list':
            video_url = ','.join([v.get('master_url') for v in videos])
        video_id = await XhsStoreFactory.create_store().store_video({"video_download_url": video_url})
    if interact_info.get("liked_count")[-1] == '万':
        content_liked_cnt = int(float(interact_info.get("liked_count")[:-1]) * 10000)
    else:
        content_liked_cnt = int(interact_info.get("liked_count"))
    if interact_info.get("collected_count")[-1] == '万':
        content_collected_cnt = int(float(interact_info.get("collected_count")[:-1]) * 10000)
    else:
        content_collected_cnt = int(interact_info.get("collected_count"))
    if interact_info.get("comment_count")[-1] == '万':
        content_comment_cnt = int(float(interact_info.get("comment_count")[:-1]) * 10000)
    else:
        content_comment_cnt = int(interact_info.get("comment_count"))
    if interact_info.get("share_count")[-1] == '万':
        content_shared_cnt = int(float(interact_info.get("share_count")[:-1]) * 10000)
    else:
        content_shared_cnt = int(interact_info.get("share_count"))
    save_content_item = {
        "hot_id": hot_id,
        "content_source": "xhs",
        "content_id": note_item.get("note_id"),
        # "type": note_item.get("type"),
        "content_crawl_time": utils.get_current_timestamp(),
        "content_title": note_item.get("title") or note_item.get("desc", "")[:255],
        "content_desc": note_item.get("desc", ""),
        # "video_url": video_url,
        "content_time": note_item.get("time"),
        "content_user_id": user_info.get("user_id"),
        "content_user_nickname": user_info.get("nickname"),
        "content_liked_cnt": content_liked_cnt,
        "content_collected_cnt": content_collected_cnt,
        "content_comment_cnt": content_comment_cnt,
        "content_shared_cnt": content_shared_cnt,
        "content_ip": note_item.get("ip_location", ""),
        "content_url": f"https://www.xiaohongshu.com/explore/{note_id}",
        "content_pics": pic_id,
        "content_videos": video_id
    }
    # utils.logger.info(f"[store.xhs.update_xhs_note] xhs note: {save_content_item}")
    xhsF = XhsStoreFactory.create_store()
    content_id = await xhsF.store_content(save_content_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 2,
        "content_source": "xhs",
        "domain": ""
    }
    await xhsF.store_extract_info(extract_info_item)

async def batch_update_xhs_note_comments(note_id: str, comments: List[Dict]):
    if not comments:
        return
    for comment_item in comments:
        await update_xhs_note_comment(note_id, comment_item)

async def update_xhs_note_comment(note_id: str, comment_item: Dict):
    user_info = comment_item.get("user_info", {})
    comment_id = comment_item.get("id")
    # comment_pictures = [item.get("url_default", "") for item in comment_item.get("pictures", [])]
    target_comment = comment_item.get("target_comment", {})
    save_comment_item = {
        "content_id": note_id,
        "comment_id": comment_id,
        "par_comment_id": target_comment.get("id", "0"),
        "comment_crawl_time": utils.get_current_timestamp(),
        "comment_time": comment_item.get("create_time"),
        "comment_desc": comment_item.get("content"),
        "sub_comment_cnt": int(comment_item.get("sub_comment_count", 0)),
        "comment_ip": comment_item.get("ip_location"),
        "comment_user_id": user_info.get("user_id"),
        "comment_user_nickname": user_info.get("nickname"),
        "comment_source": "xhs",
        # "comment_pic": ",".join(comment_pictures),
    }
    # utils.logger.info(f"[store.xhs.update_xhs_note_comment] xhs note comment:{local_db_item}")
    xhsF = XhsStoreFactory.create_store()
    content_id = await xhsF.store_comment(save_comment_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 3,
        "content_source": "xhs",
        "domain": ""
    }
    await xhsF.store_extract_info(extract_info_item)

async def update_xhs_note_image(pic_urls):
    return await XhsStoreFactory().create_store().store_pic({"pic_urls": pic_urls})