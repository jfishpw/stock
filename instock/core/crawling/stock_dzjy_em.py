#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/5/16 15:31
Desc: 东方财富网-数据中心-大宗交易-市场统计
http://data.eastmoney.com/dzjy/dzjy_sctj.aspx
"""
import random
import time
import logging
import pandas as pd
from instock.core.multi_source_fetcher import multi_fetcher, DataSource

__author__ = 'myh '
__date__ = '2025/12/31 '

logger = logging.getLogger(__name__)

def stock_dzjy_sctj() -> pd.DataFrame:
    """
    东方财富网-数据中心-大宗交易-市场统计
    http://data.eastmoney.com/dzjy/dzjy_sctj.aspx
    :return: 市场统计表
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        'sortColumns': 'TRADE_DATE',
        'sortTypes': '-1',
        'pageSize': '500',
        'pageNumber': '1',
        'reportName': 'PRT_BLOCKTRADE_MARKET_STA',
        'columns': 'TRADE_DATE,SZ_INDEX,SZ_CHANGE_RATE,BLOCKTRADE_DEAL_AMT,PREMIUM_DEAL_AMT,PREMIUM_RATIO,DISCOUNT_DEAL_AMT,DISCOUNT_RATIO',
        'source': 'WEB',
        'client': 'WEB',
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json.get("result"):
            logger.warning("大宗交易市场统计数据为空")
            return pd.DataFrame()
            
        total_page = int(data_json['result']["pages"])
        big_df = pd.DataFrame()
        for page in range(1, total_page+1):
            time.sleep(random.uniform(0.5, 1))
            params.update({'pageNumber': page})
            r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
            data_json = r.json()
            if data_json.get("result") and data_json["result"].get("data"):
                temp_df = pd.DataFrame(data_json["result"]["data"])
                big_df = pd.concat([big_df, temp_df], ignore_index=True)
        
        if big_df.empty:
            return pd.DataFrame()
            
        big_df.columns = ['交易日期', '上证指数', '上证指数涨跌幅', '大宗交易成交额', 
                          '溢价成交额', '溢价比例', '折价成交额', '折价比例']
        return big_df
        
    except Exception as e:
        logger.error(f"获取大宗交易市场统计失败: {e}")
        return pd.DataFrame()
