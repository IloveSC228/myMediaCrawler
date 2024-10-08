# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 21:35
# @Desc    : 微博存储实现类
import asyncio
import csv
import json
import os
import pathlib
from typing import Dict

import aiofiles

import config
from base.base_crawler import AbstractStore
from tools import utils, words
from var import crawler_type_var, media_crawler_mongodb_var
from db import AsyncMongoDB

def calculate_number_of_files(file_store_path: str) -> int:
    """计算数据保存文件的前部分排序数字，支持每次运行代码不写到同一个文件中
    Args:
        file_store_path;
    Returns:
        file nums
    """
    if not os.path.exists(file_store_path):
        return 1
    try:
        return max([int(file_name.split("_")[0])for file_name in os.listdir(file_store_path)])+1
    except ValueError:
        return 1


class ZhihuCsvStoreImplement(AbstractStore):
    csv_store_path: str = "data/zhihu"
    file_count:int=calculate_number_of_files(csv_store_path)

    def make_save_file_name(self, store_type: str) -> str:
        """
        make save file name by store type
        Args:
            store_type: contents or comments

        Returns: eg: data/bilibili/search_comments_20240114.csv ...

        """

        return f"{self.csv_store_path}/zh_{self.file_count}_{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}.csv"

    async def save_data_to_csv(self, save_item: Dict, store_type: str):
        """
        Below is a simple way to save it in CSV format.
        Args:
            save_item:  save content dict info
            store_type: Save type contains content and comments（contents | comments）

        Returns: no returns

        """
        pathlib.Path(self.csv_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name = self.make_save_file_name(store_type=store_type)
        async with aiofiles.open(save_file_name, mode='a+', encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            if await f.tell() == 0:
                await writer.writerow(save_item.keys())
            await writer.writerow(save_item.values())

    async def store_content(self, content_item: Dict):
        """
        Zhihu content CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=content_item, store_type="contents")

    async def store_comment(self, comment_item: Dict):
        """
        Zhihu comment CSV storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=comment_item, store_type="comments")

    async def store_hotlist(self, hotlist_item: Dict):
        """
        Zhihu hotlist CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=hotlist_item, store_type="title")

    async def store_question(self, question_item: Dict):
        """
        Zhihu question CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=question_item, store_type="question")

    async def store_answer(self, answer_item: Dict):
        """
        Zhihu answer CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=answer_item, store_type="answer")

class ZhihuDbStoreImplement(AbstractStore):
    async def store_hotlist(self, hotlist_item: Dict):
        """
        Zhihu content mysqlDB storage implementation
        Args:
            hotlist_item: hotlist item dict

        Returns:

        """
        from .zhihu_store_sql import (add_new_hotlist,
                                        query_hotlist_by_hot_title,
                                        update_hotlist_by_hot_title)
        hot_title = hotlist_item.get("hot_title")
        hotlist_detail: Dict = await query_hotlist_by_hot_title(hot_title)
        if not hotlist_detail:
            return await add_new_hotlist(hotlist_item)
        else:
            await update_hotlist_by_hot_title(hot_title, hotlist_item=hotlist_item)
            return hotlist_detail["id"]

    async def store_content(self, content_item: Dict):
        """
        Zhihu content mysqlDB storage implementation
        Args:
            content_item: content item dict

        Returns:

        """
        from .zhihu_store_sql import (add_new_content,
                                    query_content_by_content_id,
                                    update_content_by_content_id)
        note_id = content_item.get("note_id")
        note_detail: Dict = await query_content_by_content_id(content_id=note_id)
        if not note_detail:
            return await add_new_content(content_item)
        else:
            await update_content_by_content_id(note_id, content_item=content_item)
            return note_detail["id"]

    async def store_comment(self, comment_item: Dict):
        """
        Zhihu content mysqlDB storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        from .zhihu_store_sql import (add_new_comment,
                                    query_comment_by_comment_id,
                                    update_comment_by_comment_id)
        comment_id = comment_item.get("comment_id")
        comment_detail: Dict = await query_comment_by_comment_id(comment_id=comment_id)
        if not comment_detail:
            return await add_new_comment(comment_item)
        else:
            await update_comment_by_comment_id(comment_id, comment_item=comment_item)
            return comment_detail["id"]

    async def store_extract_info(self, extract_info_item: Dict):
        """
        Zhihu extract_info mysqlDB storage implementation
        Args:
            extract_info_item: extract_info item dict

        Returns:

        """
        from .zhihu_store_sql import (add_new_extract_info,
                                         query_extract_info,
                                         update_extract_info)
        content_id = extract_info_item.get("content_id")
        content_type = extract_info_item.get("content_type")
        content_source = extract_info_item.get("content_source")
        extract_info_detail: Dict = await query_extract_info(content_id, content_type, content_source)
        if not extract_info_detail:
            await add_new_extract_info(extract_info_item)
        else:
            await update_extract_info(content_id, content_type, content_source, extract_info_item)
    
    async def store_pic(self, pic_item):
        async_db_conn: AsyncMongoDB = media_crawler_mongodb_var.get()
        return await async_db_conn.insert("pic", pic_item)

    async def store_video(self, video_item):
        pass

class ZhihuJsonStoreImplement(AbstractStore):
    json_store_path: str = "data/Zhihu/json"
    words_store_path: str = "data/Zhihu/words"
    lock = asyncio.Lock()
    file_count:int=calculate_number_of_files(json_store_path)
    WordCloud = words.AsyncWordCloudGenerator()


    def make_save_file_name(self, store_type: str) -> (str,str):
        """
        make save file name by store type
        Args:
            store_type: Save type contains content and comments（contents | comments）

        Returns:

        """

        return (
            f"{self.json_store_path}/{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}.json",
            f"{self.words_store_path}/{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}"
        )

    async def save_data_to_json(self, save_item: Dict, store_type: str):
        """
        Below is a simple way to save it in json format.
        Args:
            save_item: save content dict info
            store_type: Save type contains content and comments（contents | comments）

        Returns:

        """
        pathlib.Path(self.json_store_path).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.words_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name,words_file_name_prefix = self.make_save_file_name(store_type=store_type)
        save_data = []

        async with self.lock:
            if os.path.exists(save_file_name):
                async with aiofiles.open(save_file_name, 'r', encoding='utf-8') as file:
                    save_data = json.loads(await file.read())

            save_data.append(save_item)
            async with aiofiles.open(save_file_name, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(save_data, ensure_ascii=False))

            if config.ENABLE_GET_COMMENTS and config.ENABLE_GET_WORDCLOUD:
                try:
                    await self.WordCloud.generate_word_frequency_and_cloud(save_data, words_file_name_prefix)
                except:
                    pass

    async def store_content(self, content_item: Dict):
        """
        content JSON storage implementation
        Args:
            content_item:

        Returns:

        """
        await self.save_data_to_json(content_item, "contents")

    async def store_comment(self, comment_item: Dict):
        """
        comment JSON storage implementatio
        Args:
            comment_item:

        Returns:

        """
        await self.save_data_to_json(comment_item, "comments")
