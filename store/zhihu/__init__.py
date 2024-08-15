# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 21:34
# @Desc    :

import re
from typing import List

import config

# from .zhihu_store_image import *
from .zhihu_store_impl import *


class ZhihustoreFactory:
    STORES = {
        "csv": ZhihuCsvStoreImplement,
        "db": ZhihuDbStoreImplement,
        "json": ZhihuJsonStoreImplement,
    }

    @staticmethod
    def create_store() -> AbstractStore:
        store_class = ZhihustoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError(
                "[ZhihuStoreFactory.create_store] Invalid save option only supported csv or db or json ...")
        return store_class()

async def update_zhihu_hotlist(hotlist_item: Dict, rank: int):
    detail_text = hotlist_item.get("target").get('metrics_area').get('text').split(" ")
    hot_score = int(detail_text[0]) * 10000
    card_id = hotlist_item.get('card_id').split('_')
    zhF = ZhihustoreFactory.create_store()
    if hotlist_item.get("target").get('image_area').get('url') != "":
        pic_ids = await zhF.store_pic({"pic_urls": hotlist_item.get("target").get('image_area').get('url')})
    else:
        pic_ids = None
    save_hotlist_item = {
        "hot_title": hotlist_item.get("target").get('title_area').get('text'),
        "hot_time": utils.get_current_timestamp(),
        "hot_rank": rank,
        "hot_score": hot_score,
        "hot_source": "zh",
        "hot_question_id": card_id[1],
        "hot_desc": hotlist_item.get("target").get('excerpt_area').get('text'),
        "hot_question_answer_cnt": hotlist_item.get('feed_specific').get('answer_count'),
        "hot_question_url": hotlist_item.get("target").get('link').get('url'),
        "hot_pic_url": pic_ids
    }
    # utils.logger.info(f"[store.zhihu.update_zhihu_hotlist] zhihu hotlist: {save_hotlist_item}")
    content_id = await zhF.store_hotlist(save_hotlist_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 1,
        "content_source": "zh",
        "domain": ""
    }
    await zhF.store_extract_info(extract_info_item)

async def update_zhihu_answer(answer_item: Dict, hot_id: None):
    pic_urls = []
    for thumbnail in answer_item.get("target").get("thumbnail_info").get("thumbnails"):
        pic_urls.append(thumbnail.get("url"))
    url_token = answer_item.get("target").get("author").get("url_token")
    clean_text = re.sub(r"<.*?>", "", answer_item.get("target").get("content"))
    zhF = ZhihustoreFactory.create_store()
    if pic_urls:
        pic_ids = await zhF.store_pic({"pic_urls": pic_urls})
    else:
        pic_ids = None
    question_id = answer_item["target"]["question"]["id"]
    answer_id = answer_item["target"]["id"]
    save_content_item = {
        "hot_id": hot_id,
        "content_source": "zh",
        "content_id": answer_id,
        "question_id": question_id,
        "content_crawl_time": utils.get_current_timestamp(),
        "content_desc": clean_text,
        "content_time": answer_item.get("target").get("created_time") * 1000,
        "content_user_id": answer_item.get("target").get("author").get("id"),
        "content_user_gender": answer_item.get("target").get("author").get("gender"),
        "content_user_nickname": answer_item.get("target").get("author").get("name"),
        "content_user_home": f"https://www.zhihu.com/people/{url_token}",
        "content_liked_cnt": answer_item.get("target").get("voteup_count"),
        "content_comment_cnt": answer_item.get("target").get("comment_count", 0),
        "content_url": f"https://www.zhihu.com/question/{question_id}/answer/{answer_id}",
        "content_pics": pic_ids,
    }
    config.ZH_ANSWER_ID_LIST.append(save_content_item.get('content_id'))
    # utils.logger.info(
        # f"[store.zhihu.update_zhihu_answer] zhihu answer id:{save_content_item.get('content_id')}, excerpt: {save_content_item.get('content_desc')[:24]} ...")
    content_id = await zhF.store_content(save_content_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 2,
        "content_source": "zh",
        "domain": ""
    }
    await zhF.store_extract_info(extract_info_item)

async def update_zhihu_search_note(note_item: Dict):
    pic_urls = []
    for thumbnail in note_item.get("thumbnail_info").get("thumbnails"):
        pic_urls.append(thumbnail.get("url"))
    zhF = ZhihustoreFactory.create_store()
    if pic_urls:
        pic_ids = await zhF.store_pic({"pic_urls": pic_urls})
    else:
        pic_ids = None
    question_id = note_item["question"]["id"]
    answer_id = note_item["id"]
    content_user_nickname = note_item.get("author").get("name")
    save_content_item = {
        "content_source": "zh",
        "content_id": answer_id,
        "question_id": question_id,
        "content_crawl_time": utils.get_current_timestamp(),
        "content_title": re.sub(r"<.*?>", "", note_item.get("question").get("name")),
        "content_desc": re.sub(r"<.*?>", "", note_item.get("content")),
        "content_time": note_item.get("created_time") * 1000,
        "content_user_id": note_item.get("author").get("id"),
        "content_user_gender": note_item.get("author").get("gender"),
        "content_user_nickname": content_user_nickname,
        "content_user_home": f"https://www.zhihu.com/people/{content_user_nickname}",
        "content_liked_cnt": note_item.get("voteup_count"),
        "content_comment_cnt": note_item.get("comment_count", 0),
        "content_url": f"https://www.zhihu.com/question/{question_id}/answer/{answer_id}",
        "content_pics": pic_ids,
    }
    config.ZH_ANSWER_ID_LIST.append(save_content_item.get('content_id'))
    # utils.logger.info(
        # f"[store.zhihu.update_zhihu_answer] zhihu answer id:{save_content_item.get('content_id')}, excerpt: {save_content_item.get('content_desc')[:24]} ...")
    content_id = await zhF.store_content(save_content_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 2,
        "content_source": "zh",
        "domain": ""
    }
    await zhF.store_extract_info(extract_info_item)

async def batch_update_zhihu_note_comments(note_id: str, comments: List[Dict], note_type: str):
    if not comments:
        return
    for comment_item in comments:
        await update_zhihu_note_comment(note_id, comment_item, note_type)

async def update_zhihu_note_comment(note_id: str, comment_item: Dict, note_type: str):
    if note_type == "questions":
        content_id = f'q{note_id}'
    else:
        content_id = f'a{note_id}'
    if len(comment_item['comment_tag']) > 0:
        user_ip = comment_item["comment_tag"][0]['text']
    else:
        user_ip = ""
    url_token = comment_item.get("author").get("url_token")
    comment_desc = comment_item['content']
    clean_text = re.sub(r"<.*?>", "", comment_desc)
    save_comment_item = {
        "content_id": content_id,
        "comment_id": comment_item['id'],
        "par_comment_id": comment_item['reply_comment_id'],
        "comment_crawl_time": utils.get_current_timestamp(),
        "comment_time": comment_item['created_time'] * 1000,
        "comment_desc": clean_text,
        "sub_comment_cnt": comment_item['child_comment_count'],
        "comment_liked_cnt": comment_item['like_count'],
        "comment_ip": user_ip,
        "comment_user_id": comment_item['author']['id'],
        "comment_user_nickname": comment_item.get("author").get("name"),
        "comment_user_gender": comment_item['author']['gender'],
        "comment_user_home": f"https://www.zhihu.com/people/{url_token}",
        "comment_source": "zh"  
    }
    # utils.logger.info(
        # f"[store.zhihu.update_zhihu_note_comment] zhihu note comment: {save_comment_item['comment_id']}, content: {save_comment_item.get('content', '')[:24]} ...")
    zhF = ZhihustoreFactory.create_store()
    content_id = await zhF.store_comment(save_comment_item)
    extract_info_item = {
        "content_id": content_id,
        "content_type": 3,
        "content_source": "zh",
        "domain": ""
    }
    await zhF.store_extract_info(extract_info_item)