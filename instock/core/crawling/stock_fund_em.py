#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2023/5/16 15:30
Desc: 东方财富网-数据中心-资金流向
https://data.eastmoney.com/zjlx/detail.html
"""
import json
import random
import time
import math
import logging
import pandas as pd
from instock.core.multi_source_fetcher import multi_fetcher, DataSource

__author__ = 'myh '
__date__ = '2025/12/31 '

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def stock_individual_fund_flow_rank(indicator: str = "5日") -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-排名
    https://data.eastmoney.com/zjlx/detail.html
    :param indicator: choice of {"今日", "3日", "5日", "10日"}
    :type indicator: str
    :return: 指定 indicator 资金流向排行
    :rtype: pandas.DataFrame
    """
    indicator_map = {
        "今日": [
            "f62",
            "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",
        ],
        "3日": [
            "f267",
            "f12,f14,f2,f127,f267,f268,f269,f270,f271,f272,f273,f274,f275,f276,f257,f258,f124",
        ],
        "5日": [
            "f164",
            "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124",
        ],
        "10日": [
            "f174",
            "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124",
        ],
    }
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    page_current = 1
    params = {
        "fid": indicator_map[indicator][0],
        "po": "1",
        "pz": page_size,
        "pn": page_current,
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fs": "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
        "fields": indicator_map[indicator][1],
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json.get("data") or not data_json["data"].get("diff"):
            logger.warning("资金流向数据为空")
            return pd.DataFrame()
        
        data = data_json["data"]["diff"]
        data_count = data_json["data"]["total"]
        page_count = math.ceil(data_count/page_size)
        
        while page_count > 1:
            time.sleep(random.uniform(0.5, 1))
            page_current += 1
            params["pn"] = page_current
            r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
            data_json = r.json()
            if data_json.get("data") and data_json["data"].get("diff"):
                data.extend(data_json["data"]["diff"])
            page_count -= 1

        temp_df = pd.DataFrame(data)
        temp_df = temp_df[~temp_df["f2"].isin(["-"])]
        
        if indicator == "今日":
            temp_df.columns = [
                "最新价", "今日涨跌幅", "代码", "名称", "今日主力净流入-净额",
                "今日超大单净流入-净额", "今日超大单净流入-净占比",
                "今日大单净流入-净额", "今日大单净流入-净占比",
                "今日中单净流入-净额", "今日中单净流入-净占比",
                "今日小单净流入-净额", "今日小单净流入-净占比",
                "_", "今日主力净流入-净占比", "_", "_", "_",
            ]
            temp_df = temp_df[
                ["代码", "名称", "最新价", "今日涨跌幅", "今日主力净流入-净额",
                 "今日主力净流入-净占比", "今日超大单净流入-净额", "今日超大单净流入-净占比",
                 "今日大单净流入-净额", "今日大单净流入-净占比",
                 "今日中单净流入-净额", "今日中单净流入-净占比",
                 "今日小单净流入-净额", "今日小单净流入-净占比"]
            ]
        elif indicator == "3日":
            temp_df.columns = [
                "最新价", "代码", "名称", "_", "3日涨跌幅", "_", "_", "_",
                "3日主力净流入-净额", "3日主力净流入-净占比",
                "3日超大单净流入-净额", "3日超大单净流入-净占比",
                "3日大单净流入-净额", "3日大单净流入-净占比",
                "3日中单净流入-净额", "3日中单净流入-净占比",
                "3日小单净流入-净额", "3日小单净流入-净占比",
            ]
            temp_df = temp_df[
                ["代码", "名称", "最新价", "3日涨跌幅", "3日主力净流入-净额",
                 "3日主力净流入-净占比", "3日超大单净流入-净额", "3日超大单净流入-净占比",
                 "3日大单净流入-净额", "3日大单净流入-净占比",
                 "3日中单净流入-净额", "3日中单净流入-净占比",
                 "3日小单净流入-净额", "3日小单净流入-净占比"]
            ]
        elif indicator == "5日":
            temp_df.columns = [
                "最新价", "代码", "名称", "5日涨跌幅", "_",
                "5日主力净流入-净额", "5日主力净流入-净占比",
                "5日超大单净流入-净额", "5日超大单净流入-净占比",
                "5日大单净流入-净额", "5日大单净流入-净占比",
                "5日中单净流入-净额", "5日中单净流入-净占比",
                "5日小单净流入-净额", "5日小单净流入-净占比",
                "_", "_", "_",
            ]
            temp_df = temp_df[
                ["代码", "名称", "最新价", "5日涨跌幅", "5日主力净流入-净额",
                 "5日主力净流入-净占比", "5日超大单净流入-净额", "5日超大单净流入-净占比",
                 "5日大单净流入-净额", "5日大单净流入-净占比",
                 "5日中单净流入-净额", "5日中单净流入-净占比",
                 "5日小单净流入-净额", "5日小单净流入-净占比"]
            ]
        elif indicator == "10日":
            temp_df.columns = [
                "最新价", "代码", "名称", "_", "10日涨跌幅",
                "10日主力净流入-净额", "10日主力净流入-净占比",
                "10日超大单净流入-净额", "10日超大单净流入-净占比",
                "10日大单净流入-净额", "10日大单净流入-净占比",
                "10日中单净流入-净额", "10日中单净流入-净占比",
                "10日小单净流入-净额", "10日小单净流入-净占比",
                "_", "_", "_",
            ]
            temp_df = temp_df[
                ["代码", "名称", "最新价", "10日涨跌幅", "10日主力净流入-净额",
                 "10日主力净流入-净占比", "10日超大单净流入-净额", "10日超大单净流入-净占比",
                 "10日大单净流入-净额", "10日大单净流入-净占比",
                 "10日中单净流入-净额", "10日中单净流入-净占比",
                 "10日小单净流入-净额", "10日小单净流入-净占比"]
            ]
        return temp_df
        
    except Exception as e:
        logger.error(f"获取资金流向数据失败: {e}")
        return pd.DataFrame()


def stock_sector_fund_flow_rank(
    indicator: str = "10日", sector_type: str = "行业资金流"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-资金流向-板块资金流-排名
    https://data.eastmoney.com/bkzj/hy.html
    :param indicator: choice of {"今日", "5日", "10日"}
    :type indicator: str
    :param sector_type: choice of {"行业资金流", "概念资金流", "地域资金流"}
    :type sector_type: str
    :return: 指定参数的资金流排名数据
    :rtype: pandas.DataFrame
    """
    sector_type_map = {"行业资金流": "2", "概念资金流": "3", "地域资金流": "1"}
    indicator_map = {
        "今日": ["f62", "1", "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124"],
        "5日": ["f164", "5", "f12,f14,f2,f109,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f257,f258,f124"],
        "10日": ["f174", "10", "f12,f14,f2,f160,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f260,f261,f124"],
    }
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    page_size = 50
    page_current = 1
    params = {
        "pn": page_current,
        "pz": page_size,
        "po": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fid0": indicator_map[indicator][0],
        "fs": f"m:90 t:{sector_type_map[sector_type]}",
        "stat": indicator_map[indicator][1],
        "fields": indicator_map[indicator][2],
        "rt": "52975239",
        "_": int(time.time() * 1000),
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        text_data = r.text
        data_json = json.loads(text_data[text_data.find("{") : -2])
        
        if not data_json.get("data") or not data_json["data"].get("diff"):
            logger.warning("板块资金流数据为空")
            return pd.DataFrame()
        
        data = data_json["data"]["diff"]
        data_count = data_json["data"]["total"]
        page_count = math.ceil(data_count/page_size)
        
        while page_count > 1:
            time.sleep(random.uniform(0.5, 1))
            page_current += 1
            params["pn"] = page_current
            r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
            text_data = r.text
            json_data = json.loads(text_data[text_data.find("{"): -2])
            if json_data.get("data") and json_data["data"].get("diff"):
                data.extend(json_data["data"]["diff"])
            page_count -= 1

        temp_df = pd.DataFrame(data)
        temp_df = temp_df[~temp_df["f2"].isin(["-"])]
        
        if indicator == "今日":
            temp_df.columns = [
                "-", "今日涨跌幅", "_", "名称", "今日主力净流入-净额",
                "今日超大单净流入-净额", "今日超大单净流入-净占比",
                "今日大单净流入-净额", "今日大单净流入-净占比",
                "今日中单净流入-净额", "今日中单净流入-净占比",
                "今日小单净流入-净额", "今日小单净流入-净占比",
                "-", "今日主力净流入-净占比", "今日主力净流入最大股",
                "今日主力净流入最大股代码", "是否净流入",
            ]
            temp_df = temp_df[
                ["名称", "今日涨跌幅", "今日主力净流入-净额", "今日主力净流入-净占比",
                 "今日超大单净流入-净额", "今日超大单净流入-净占比",
                 "今日大单净流入-净额", "今日大单净流入-净占比",
                 "今日中单净流入-净额", "今日中单净流入-净占比",
                 "今日小单净流入-净额", "今日小单净流入-净占比", "今日主力净流入最大股"]
            ]
        elif indicator == "5日":
            temp_df.columns = [
                "-", "_", "名称", "5日涨跌幅", "_",
                "5日主力净流入-净额", "5日主力净流入-净占比",
                "5日超大单净流入-净额", "5日超大单净流入-净占比",
                "5日大单净流入-净额", "5日大单净流入-净占比",
                "5日中单净流入-净额", "5日中单净流入-净占比",
                "5日小单净流入-净额", "5日小单净流入-净占比",
                "5日主力净流入最大股", "_", "_",
            ]
            temp_df = temp_df[
                ["名称", "5日涨跌幅", "5日主力净流入-净额", "5日主力净流入-净占比",
                 "5日超大单净流入-净额", "5日超大单净流入-净占比",
                 "5日大单净流入-净额", "5日大单净流入-净占比",
                 "5日中单净流入-净额", "5日中单净流入-净占比",
                 "5日小单净流入-净额", "5日小单净流入-净占比", "5日主力净流入最大股"]
            ]
        elif indicator == "10日":
            temp_df.columns = [
                "-", "_", "名称", "_", "10日涨跌幅",
                "10日主力净流入-净额", "10日主力净流入-净占比",
                "10日超大单净流入-净额", "10日超大单净流入-净占比",
                "10日大单净流入-净额", "10日大单净流入-净占比",
                "10日中单净流入-净额", "10日中单净流入-净占比",
                "10日小单净流入-净额", "10日小单净流入-净占比",
                "10日主力净流入最大股", "_", "_",
            ]
            temp_df = temp_df[
                ["名称", "10日涨跌幅", "10日主力净流入-净额", "10日主力净流入-净占比",
                 "10日超大单净流入-净额", "10日超大单净流入-净占比",
                 "10日大单净流入-净额", "10日大单净流入-净占比",
                 "10日中单净流入-净额", "10日中单净流入-净占比",
                 "10日小单净流入-净额", "10日小单净流入-净占比", "10日主力净流入最大股"]
            ]
        return temp_df
        
    except Exception as e:
        logger.error(f"获取板块资金流数据失败: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    stock_individual_fund_flow_rank_df = stock_individual_fund_flow_rank(indicator="今日")
    print(f"获取到 {len(stock_individual_fund_flow_rank_df)} 条数据")
    print(stock_individual_fund_flow_rank_df.head())
