import requests
import execjs
from typing import Dict
import asyncio
from urllib.parse import urlencode
from tools import utils

async def search_by_keyword(
    q: str,
    next: str = None
):
    with open('media_platform/zhihu/algorithm.js','r',encoding='utf-8') as f:
        func = f.read()
    if next == "":
        uri = "/api/v4/search_v3"
        params = {
            "gk_version": "gz-gaokao",
            "t": "general",
            "q": q, 
            "correction": 1,
            "offset": 0,
            "limit": 20,
            "filter_fields": "",
            "lc_idx": 0,
            "show_all_topics": 0,
            "search_source": "Normal"
        }
        url = (f"https://www.zhihu.com{uri}?{urlencode(params)}")
    else:
        url = next.replace('api.zhihu.com/search_v3?advert_count=0&', 'www.zhihu.com/api/v4/search_v3?')

    # url = "https://www.zhihu.com/api/v4/search_v3?gk_version=gz-gaokao&t=general&q=%E5%A5%A5%E8%BF%90%E4%BC%9A&correction=1&offset=0&limit=20&filter_fields=&lc_idx=0&show_all_topics=0&search_source=Normal"
    

    x_zse = execjs.compile(func).call('ed',url.replace('https://www.zhihu.com',""))
    # print(x_zse)
    ## 替换自己的cookie
    cookies = {
        "d_c0": "AWCSZWrk1BiPTpdbc4XhCLvp3Jwbt1Nt0zo=|1719383817",
        "YD00517437729195%3AWM_TID": "4%2Fr3iWQ5DIxAUBBEUQN%2FobdtqDGNMOQ7",
        "_zap": "e1cccd40-a3ae-48b5-8091-1cb9485d1386",
        "YD00517437729195%3AWM_NI": "AI%2Bew%2BX6Rd8jnKQhzlOZ3k5bxJENC6LKwcmUA%2FldtIZKYWtlG9KdodyY86xB4bAJSy8vPr9j%2FbIrIFVeFWj9fJX0mWSWc1BPDTjVUVWMtOzR0Com6uwfI4EbOWIynDqBQXQ%3D",
        "YD00517437729195%3AWM_NIKE": "9ca17ae2e6ffcda170e2e6eea3f45aa3afaeccb24787a88ea3d54a839b8f86c53aae8b9892ef658694a4dab32af0fea7c3b92a8397e586f6219cea9e85bb62ae959f89f9688d86fd8cb86abc968bafc23e9095b984f05f83f1a485f7439a9597b8f76885e78585ee70bb87f8d7fb4af8be8fabd273fcaaa093bc62edbe8793cc4e96bb8ab8e933a69698d1f05d9189add2c15c888ca6b7d85e9593bcb0f95af49da18eb57fb5ba87d5c86a8d89879aed50b39e9fb9d437e2a3",
        "q_c1": "47899854f5074cd8954a8f6403628964|1719285166000|1709446544000",
        "Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49": "1713252312",
        "_xsrf": "0nK98vKstbHvW3F7dnLEh6rALRCS0n2V",
        "__snaker__id": "oc0mSIVSE5geD84G",
        "__zse_ck": "001_fJI58bjRrd9RG8Pb0YRwywTALifaIJl6vTU3BFwKLavKhizy2=M+j2BUYv28BNQCuMUx/sy/BGgftLdrDR0L8Xb05lQqJ2rskyUc3p447gpHo08Td2rFdG2Y4SA6EHRP",
        "tst": "h",
        "BEC": "8ce9e721fafad59a55ed220f1ad7f253",
        "SESSIONID": "Yrmz44FjMaAocYplynCfbulXwHcc1R7I8cqDmpKorHj",
        "JOID": "U1kQC0L2WWqRdvmVXP6xsQ6oPGhLry8K6krN1zuzOC_zFKjlNIx1wP5-_ZxQ6ouNYmN4IBUGS0w9WqyoeNTGIj0=",
        "osd": "UVgdAUj0WGebfPuUUfS7sw-lNmJJriIA4EjM2jG5Oi7-HqLnNYF_yvx_8JZa6IqAaGl6IRgMQU48V6aietXLKDc=",
        "z_c0": "2|1:0|10:1722164619|4:z_c0|80:MS4xODBLeU1BQUFBQUFtQUFBQVlBSlZUWXR6azJmLU9tcFdhYVlranlzeDhBN2JpZm9mT3Iwa2x3PT0=|f1663085d6dec22e36ba7876e8bd4046ec37dca804bcf1be80bde3ab937a03b7"
    }

    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "priority": "u=1, i",
        "referer": "https://www.zhihu.com",
        "sec-ch-ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Google Chrome\";v=\"126\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "x-api-version": "3.0.91",
        "x-app-za": "OS=Web",
        "x-requested-with": "fetch",
        "x-zse-93": "101_3_3.0",
        "x-zse-96": f"2.0_{x_zse}",
    }
    
    response = requests.request(method='GET', url=url, headers=headers, cookies=cookies)

    data: Dict = response.json()
    paging = data.get("paging", "")
    data = data.get('data')
    if data:
        return data, paging
    else:
        logger = utils.get_logger("zh")
        logger.error("[ZhiHuCrawler.search] cannot crawl zh search data")
        # utils.logger.info("[ZhiHuCrawler.search] cannot crawl zh search data")

if __name__ == '__main__':
    keyword = "奥运会"
    # 运行异步请求
    asyncio.run(search_by_keyword(keyword))