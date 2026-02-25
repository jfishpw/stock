# -*- coding:utf-8 -*-
# !/usr/bin/env python

import math
import random
import time
import logging
import pandas as pd
import instock.core.tablestructure as tbs
from instock.core.multi_source_fetcher import multi_fetcher, DataSource

__author__ = 'myh '
__date__ = '2025/12/31 '

logger = logging.getLogger(__name__)

def stock_selection() -> pd.DataFrame:
    """
    东方财富网-个股-选股器
    https://data.eastmoney.com/xuangu/
    :return: 选股器
    :rtype: pandas.DataFrame
    """
    cols = tbs.TABLE_CN_STOCK_SELECTION['columns']
    page_size = 50
    page_current = 1
    sty = ""
    for k in cols:
        sty = f"{sty},{cols[k]['map']}"
    url = "https://data.eastmoney.com/dataapi/xuangu/list"
    params = {
        "sty": sty[1:],
        "filter": "(MARKET+in+(\"上交所主板\",\"深交所主板\",\"深交所创业板\"))(NEW_PRICE>0)",
        "p": page_current,
        "ps": page_size,
        "source": "SELECT_SECURITIES",
        "client": "WEB"
    }

    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json.get("result") or not data_json["result"].get("data"):
            logger.warning("选股数据为空")
            return pd.DataFrame()
        
        data = data_json["result"]["data"]
        data_count = data_json["result"]["count"]
        page_count = math.ceil(data_count/page_size)
        
        while page_count > 1:
            time.sleep(random.uniform(0.5, 1))
            page_current += 1
            params["p"] = page_current
            r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
            data_json = r.json()
            if data_json.get("result") and data_json["result"].get("data"):
                data.extend(data_json["result"]["data"])
            page_count -= 1

        temp_df = pd.DataFrame(data)

        if 'CONCEPT' in temp_df.columns:
            mask = ~temp_df['CONCEPT'].isna()
            temp_df.loc[mask, 'CONCEPT'] = temp_df.loc[mask, 'CONCEPT'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        if 'STYLE' in temp_df.columns:
            mask = ~temp_df['STYLE'].isna()
            temp_df.loc[mask, 'STYLE'] = temp_df.loc[mask, 'STYLE'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        for k in cols:
            if cols[k]["map"] in temp_df.columns:
                t = tbs.get_field_type_name(cols[k]["type"])
                if t == 'numeric':
                    temp_df[cols[k]["map"]] = pd.to_numeric(temp_df[cols[k]["map"]], errors="coerce")
                elif t == 'datetime':
                    temp_df[cols[k]["map"]] = pd.to_datetime(temp_df[cols[k]["map"]], errors="coerce").dt.date

        return temp_df
        
    except Exception as e:
        logger.error(f"获取选股数据失败: {e}")
        return pd.DataFrame()


def stock_selection_params():
    """
    东方财富网-个股-选股器-选股指标
    https://data.eastmoney.com/xuangu/
    :return: 选股器-选股指标
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/wstock/selection/api/data/get"
    params = {
        "type": "RPTA_PCNEW_WHOLE",
        "sty": "ALL",
        "p": 1,
        "ps": 1000,
        "source": "SELECT_SECURITIES",
        "client": "WEB"
    }

    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        zxzb = data_json["result"]["data"]
        print(zxzb)
    except Exception as e:
        logger.error(f"获取选股指标失败: {e}")


if __name__ == "__main__":
    stock_selection_df = stock_selection()
    print(f"获取到 {len(stock_selection_df)} 条数据")
    print(stock_selection_df.head())
