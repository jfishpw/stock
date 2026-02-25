#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2023/4/19 15:31
Desc: 东方财富网-数据中心-年报季报-分红送配
https://data.eastmoney.com/yjfp/
"""
import warnings
import pandas as pd
from instock.core.multi_source_fetcher import multi_fetcher, DataSource

__author__ = 'myh '
__date__ = '2025/12/31 '

warnings.simplefilter(action="ignore", category=FutureWarning)

def stock_fhps_em(date: str = "20231231") -> pd.DataFrame:
    """
    东方财富网-数据中心-年报季报-分红送配
    https://data.eastmoney.com/yjfp/
    :param date: 分红送配报告期
    :type date: str
    :return: 分红送配
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "PLAN_NOTICE_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_SHAREBONUS_DET",
        "columns": "ALL",
        "quoteColumns": "",
        "js": '{"data":(x),"pages":(tp)}',
        "source": "WEB",
        "client": "WEB",
        "filter": f"""(REPORT_DATE='{"-".join([date[:4], date[4:6], date[6:]])}')""",
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json.get("result") or not data_json["result"].get("data"):
            return pd.DataFrame()
            
        data = data_json["result"]["data"]
        temp_df = pd.DataFrame(data)
        
        temp_df.rename(columns={
            "SECURITY_CODE": "代码",
            "SECURITY_NAME_ABBR": "名称",
            "PLAN_NOTICE_DATE": "方案公告日",
            "DIVIDEND_YEAR": "分红年度",
            "REPORT_DATE": "报告期",
            "BONUS_TYPE_CODE": "分红类型",
            "BONUS_AMOUNT_RMB": "送股比例",
            "CAPITAL_RESERVE_TO_SHARE_RMB": "转增比例",
            "CASH_AMOUNT_RMB": "派息比例",
            "EX_RIGHT_DATE": "除权除息日",
            "DIVIDEND_RECORD_DATE": "股权登记日",
        }, inplace=True)
        
        cols = ["代码", "名称", "方案公告日", "分红年度", "报告期", "分红类型", 
                "送股比例", "转增比例", "派息比例", "除权除息日", "股权登记日"]
        available_cols = [c for c in cols if c in temp_df.columns]
        return temp_df[available_cols]
        
    except Exception as e:
        return pd.DataFrame()
