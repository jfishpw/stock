# -*- coding:utf-8 -*-
# !/usr/bin/env python
"""
Date: 2022/6/20 15:30
Desc: 东方财富网-个股-操盘必读
https://emweb.securities.eastmoney.com/PC_HSF10/OperationsRequired/Index?type=web&code=SH688041#
"""
import logging
import pandas as pd
import instock.core.tablestructure as tbs
from instock.core.multi_source_fetcher import multi_fetcher, DataSource

__author__ = 'myh '
__date__ = '2025/12/31 '

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def stock_cpbd_em(symbol: str = "688041") -> pd.DataFrame:
    """
    东方财富网-个股-操盘必读
    https://emweb.securities.eastmoney.com/PC_HSF10/OperationsRequired/Index?type=web&code=SH688041#
    :param symbol: 带市场标识的股票代码
    :type symbol: str
    :return: 操盘必读
    :rtype: pandas.DataFrame
    """
    url = "https://emweb.eastmoney.com/PC_HSF10/OperationsRequired/PageAjax"
    params = {
        "code": f"SH{symbol}" if symbol.startswith("6") else f"SZ{symbol}"
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json:
            return pd.DataFrame()
            
        zxzb = data_json.get("zxzb", [])
        if len(zxzb) < 1:
            return pd.DataFrame()

        data_dict = zxzb[0]
        zxzbOther = data_json.get("zxzbOther", [])
        if len(zxzbOther) > 0:
            zxzbOther = zxzbOther[0]
            data_dict = {**data_dict, **zxzbOther}

        temp_df = pd.DataFrame([data_dict])
        
        cols = tbs.TABLE_CN_STOCK_CPBD['columns']
        rename_dict = {}
        for col_name, col_info in cols.items():
            if col_info.get('map') in temp_df.columns:
                rename_dict[col_info['map']] = col_name
        
        temp_df = temp_df.rename(columns=rename_dict)
        available_cols = [c for c in cols.keys() if c in temp_df.columns]
        return temp_df[available_cols]
        
    except Exception as e:
        logger.error(f"获取操盘必读失败: {e}")
        return pd.DataFrame()
