# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 21:34
# @Desc    :

import re
from typing import List

import config

from .weibo_store_image import *
from .weibo_store_impl import *


class WeibostoreFactory:
    STORES = {
        "csv": WeiboCsvStoreImplement,
        "db": WeiboDbStoreImplement,
        "json": WeiboJsonStoreImplement,
    }

    @staticmethod
    def create_store() -> AbstractStore:
        store_class = WeibostoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError(
                "[WeibotoreFactory.create_store] Invalid save option only supported csv or db or json ...")
        return store_class()

async def update_weibo_hotlist(hotlist_item: Dict, rank: int):
    if hotlist_item.get("desc_extr"):
        if type(hotlist_item.get("desc_extr")) == int:
            hot_score = hotlist_item.get("desc_extr")
        elif hotlist_item.get("desc_extr") == "":
            hot_score = 0
        else:
            hot_score = int(hotlist_item.get("desc_extr").split(" ")[1])
    else:
        hot_score = 0
    save_hotlist_item = {
        "hot_title": hotlist_item.get("desc"),
        "hot_time": utils.get_current_timestamp(),
        "hot_rank": rank,
        "hot_score": hot_score,
        "hot_source": "wb",
    }
    # utils.logger.info(f"[store.weibo.update_weibo_hotlist] xhs hotlist: {save_hotlist_item}")
    wbF = WeibostoreFactory.create_store()
    content_id = await wbF.store_hotlist(save_hotlist_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 1,
        "content_source": "wb",
        "domain": ""
    }
    await wbF.store_extract_info(extract_info_item)

async def update_weibo_note(note_item: Dict, hot_id=None, clean_text=None, pic_id=None):
    mblog: Dict = note_item.get("mblog")
    user_info: Dict = mblog.get("user")
    note_id = mblog.get("id")
    if user_info.get("gender", "") == 'f':
        content_user_gender = 0
    elif user_info.get("gender", "") == 'm':
        content_user_gender = 1
    else:
        content_user_gender = -1
    save_content_item = {
        # "hot_id": hot_id,
        "content_source": "wb",
        "content_id": note_id,
        "content_crawl_time": utils.get_current_timestamp(),
        # "content_desc": clean_text,
        "content_time": int(utils.rfc2822_to_timestamp(mblog.get("created_at"))) * 1000,
        # "create_date_time": str(utils.rfc2822_to_china_datetime(mblog.get("created_at"))),
        "content_user_id": str(user_info.get("id")),
        "content_user_gender": content_user_gender,
        "content_user_nickname": user_info.get("screen_name", ""),
        "content_user_home": user_info.get("profile_url", ""),
        "content_liked_cnt": str(mblog.get("attitudes_count", 0)),
        "content_comment_cnt": str(mblog.get("comments_count", 0)),
        "content_shared_cnt": str(mblog.get("reposts_count", 0)),
        "content_ip": mblog.get("region_name", "").replace("发布于 ", ""),
        "content_url": f"https://m.weibo.cn/detail/{note_id}",
        # "content_pics": pic_id,
    }
    if hot_id:
        save_content_item["hot_id"] = hot_id
    if clean_text:
        save_content_item["content_desc"] = clean_text
    if pic_id:
        save_content_item["content_pics"] = pic_id
    # utils.logger.info(
    #     f"[store.weibo.update_weibo_note] weibo note id:{note_id}, title:{save_content_item.get('content_desc')[:24]} ...")
    wbF = WeibostoreFactory.create_store()
    content_id = await wbF.store_content(save_content_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 2,
        "content_source": "wb",
        "domain": ""
    }
    await wbF.store_extract_info(extract_info_item)

async def batch_update_weibo_note_comments(note_id: str, comments: List[Dict]):
    if not comments:
        return
    for comment_item in comments:
        await update_weibo_note_comment(note_id, comment_item)

async def update_weibo_note_comment(note_id: str, comment_item: Dict):
    comment_id = str(comment_item.get("id"))
    user_info: Dict = comment_item.get("user")
    content_text = comment_item.get("text")
    clean_text = re.sub(r"<.*?>", "", content_text)
    if user_info.get("gender", "") == 'f':
        comment_user_gender = 0
    elif user_info.get("gender", "") == 'm':
        comment_user_gender = 1
    else:
        comment_user_gender = -1
    save_comment_item = {
        "content_id": note_id,
        "comment_id": comment_id,
        "par_comment_id": 0, # 微博没有二级评论，只有作者回复会有
        "comment_crawl_time": utils.get_current_timestamp(),
        "comment_time": int(utils.rfc2822_to_timestamp(comment_item.get("created_at"))) * 1000,
        # "create_date_time": str(utils.rfc2822_to_china_datetime(comment_item.get("created_at"))),
        "comment_desc": clean_text,
        "sub_comment_cnt": str(comment_item.get("total_number", 0)),
        "comment_liked_cnt": str(comment_item.get("like_count", 0)),
        "comment_ip": comment_item.get("source", "").replace("来自", ""),
        "comment_user_id": str(user_info.get("id")),
        "comment_user_nickname": user_info.get("screen_name", ""),
        "comment_user_gender": comment_user_gender,
        "comment_user_home": user_info.get("profile_url", ""),
        "comment_source": "wb"        
    }
    # utils.logger.info(
    #     f"[store.weibo.update_weibo_note_comment] Weibo note comment: {comment_id}, content: {save_comment_item.get('comment_desc', '')[:24]} ...")
    wbF = WeibostoreFactory.create_store()
    content_id = await wbF.store_comment(save_comment_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 3,
        "content_source": "wb",
        "domain": ""
    }
    await wbF.store_extract_info(extract_info_item)

# async def update_weibo_note_image(picid: str, pic_content, extension_file_name):
async def update_weibo_note_image(pic_urls):
    # pic_id = await WeiboStoreImage().store_pic({"pic_id": picid, "pic_content": pic_content, "extension_file_name": extension_file_name})
    return await WeibostoreFactory().create_store().store_pic({"pic_urls": pic_urls})