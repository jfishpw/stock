#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2023/1/4 12:18
Desc: 东方财富-ETF 行情
https://quote.eastmoney.com/sh513500.html
"""
import random
import time
from functools import lru_cache
import math
import logging
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

__author__ = 'myh '
__date__ = '2025/12/31 '

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

_etf_session = None
_etf_available = True
_last_check_time = 0

def _get_etf_session():
    global _etf_session
    if _etf_session is None:
        _etf_session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
        _etf_session.mount("http://", adapter)
        _etf_session.mount("https://", adapter)
        _etf_session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        })
    return _etf_session

def _check_etf_available():
    global _etf_available, _last_check_time
    import time as t
    current_time = t.time()
    if current_time - _last_check_time > 300:
        _etf_available = True
        _last_check_time = current_time
    return _etf_available

def _mark_etf_unavailable():
    global _etf_available, _last_check_time
    _etf_available = False
    _last_check_time = time.time()
    logger.warning("ETF数据源暂时不可用，将在5分钟后重试")

def _make_etf_request(url, params, timeout=15):
    if not _check_etf_available():
        return None
    
    session = _get_etf_session()
    session.headers['User-Agent'] = random.choice(USER_AGENTS)
    
    try:
        if url.startswith('http://'):
            url = 'https://' + url[7:]
        
        response = session.get(url, params=params, timeout=timeout, verify=True)
        
        if response.status_code == 403:
            logger.warning("ETF请求被拒绝(403)")
            _mark_etf_unavailable()
            return None
        
        response.raise_for_status()
        return response
        
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"ETF连接错误: {e}")
        _mark_etf_unavailable()
        return None
        
    except requests.exceptions.Timeout as e:
        logger.warning(f"ETF请求超时: {e}")
        return None
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"ETF请求错误: {e}")
        return None

def fund_etf_spot_em() -> pd.DataFrame:
    """
    东方财富-ETF 实时行情
    https://quote.eastmoney.com/center/gridlist.html#fund_etf
    :return: ETF 实时行情
    :rtype: pandas.DataFrame
    """
    if not _check_etf_available():
        logger.info("ETF数据源暂时不可用，跳过获取")
        return pd.DataFrame()
    
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
        "fid": "f12",
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
    }
    
    r = _make_etf_request(url, params)
    if r is None:
        return pd.DataFrame()
    
    try:
        data_json = r.json()

        if not data_json.get("data") or not data_json["data"].get("diff"):
            logger.warning("ETF实时行情数据为空")
            return pd.DataFrame()

        data = data_json["data"]["diff"]
        data_count = data_json["data"]["total"]
        page_count = math.ceil(data_count/page_size)
        
        while page_count > 1:
            time.sleep(random.uniform(0.5, 1))
            page_current = page_current + 1
            params["pn"] = page_current
            r = _make_etf_request(url, params)
            if r is None:
                break
            data_json = r.json()
            if data_json.get("data") and data_json["data"].get("diff"):
                data.extend(data_json["data"]["diff"])
            page_count = page_count - 1

        temp_df = pd.DataFrame(data)
        temp_df.rename(
            columns={
                "f12": "代码",
                "f14": "名称",
                "f2": "最新价",
                "f3": "涨跌幅",
                "f4": "涨跌额",
                "f5": "成交量",
                "f6": "成交额",
                "f17": "开盘价",
                "f15": "最高价",
                "f16": "最低价",
                "f18": "昨收",
                "f8": "换手率",
                "f21": "流通市值",
                "f20": "总市值",
            },
            inplace=True,
        )
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "开盘价",
                "最高价",
                "最低价",
                "昨收",
                "换手率",
                "流通市值",
                "总市值",
            ]
        ]
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        temp_df["开盘价"] = pd.to_numeric(temp_df["开盘价"], errors="coerce")
        temp_df["最高价"] = pd.to_numeric(temp_df["最高价"], errors="coerce")
        temp_df["最低价"] = pd.to_numeric(temp_df["最低价"], errors="coerce")
        temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
        temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
        temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
        return temp_df
        
    except Exception as e:
        logger.error(f"解析ETF实时行情失败: {e}")
        _mark_etf_unavailable()
        return pd.DataFrame()


@lru_cache()
def _fund_etf_code_id_map_em() -> dict:
    """
    东方财富-ETF 代码和市场标识映射
    https://quote.eastmoney.com/center/gridlist.html#fund_etf
    :return: ETF 代码和市场标识映射
    :rtype: pandas.DataFrame
    """
    if not _check_etf_available():
        return {}
    
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
        "fields": "f12,f13",
    }
    
    r = _make_etf_request(url, params)
    if r is None:
        return {}
    
    try:
        data_json = r.json()
        if data_json.get("data") and data_json["data"].get("diff"):
            temp_df = pd.DataFrame(data_json["data"]["diff"])
            temp_dict = dict(zip(temp_df["f12"], temp_df["f13"]))
            return temp_dict
        return {}
    except Exception as e:
        logger.error(f"获取ETF代码映射失败: {e}")
        return {}


def fund_etf_hist_em(
    symbol: str = "159707",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富-ETF 行情
    https://quote.eastmoney.com/sz159707.html
    :param symbol: ETF 代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    if not _check_etf_available():
        return pd.DataFrame()
    
    code_id_dict = _fund_etf_code_id_map_em()
    if symbol not in code_id_dict:
        logger.warning(f"ETF代码 {symbol} 不存在")
        return pd.DataFrame()
        
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
    }
    
    r = _make_etf_request(url, params)
    if r is None:
        return pd.DataFrame()
    
    try:
        data_json = r.json()
        if not (data_json.get("data") and data_json["data"].get("klines")):
            return pd.DataFrame()
        temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
        temp_df.columns = [
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
        temp_df.index = pd.to_datetime(temp_df["日期"])
        temp_df.reset_index(inplace=True, drop=True)

        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
        return temp_df
        
    except Exception as e:
        logger.error(f"解析ETF历史行情失败: {e}")
        return pd.DataFrame()


def fund_etf_hist_min_em(
    symbol: str = "159707",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
    period: str = "5",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富-ETF 行情
    https://quote.eastmoney.com/sz159707.html
    :param symbol: ETF 代码
    :type symbol: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param period: choice of {'1', '5', '15', '30', '60'}
    :type period: str
    :param adjust: choice of {'', 'qfq', 'hfq'}
    :type adjust: str
    :return: 每日分时行情
    :rtype: pandas.DataFrame
    """
    if not _check_etf_available():
        return pd.DataFrame()
    
    code_id_dict = _fund_etf_code_id_map_em()
    if symbol not in code_id_dict:
        logger.warning(f"ETF代码 {symbol} 不存在")
        return pd.DataFrame()
        
    adjust_map = {
        "": "0",
        "qfq": "1",
        "hfq": "2",
    }
    
    try:
        if period == "1":
            url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
            params = {
                "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
                "ndays": "5",
                "iscr": "0",
                "secid": f"{code_id_dict[symbol]}.{symbol}",
            }
            r = _make_etf_request(url, params)
            if r is None:
                return pd.DataFrame()
                
            data_json = r.json()
            
            if not data_json.get("data") or not data_json["data"].get("trends"):
                return pd.DataFrame()
                
            temp_df = pd.DataFrame(
                [item.split(",") for item in data_json["data"]["trends"]]
            )
            temp_df.columns = [
                "时间",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "最新价",
            ]
            temp_df.index = pd.to_datetime(temp_df["时间"])
            temp_df = temp_df[start_date:end_date]
            temp_df.reset_index(drop=True, inplace=True)
            temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
            temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
            temp_df["最高"] = pd.to_numeric(temp_df["最高"])
            temp_df["最低"] = pd.to_numeric(temp_df["最低"])
            temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
            temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
            temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
            temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
            return temp_df
        else:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": period,
                "fqt": adjust_map[adjust],
                "secid": f"{code_id_dict[symbol]}.{symbol}",
                "beg": "0",
                "end": "20500000",
            }
            r = _make_etf_request(url, params)
            if r is None:
                return pd.DataFrame()
                
            data_json = r.json()
            
            if not data_json.get("data") or not data_json["data"].get("klines"):
                return pd.DataFrame()
                
            temp_df = pd.DataFrame(
                [item.split(",") for item in data_json["data"]["klines"]]
            )
            temp_df.columns = [
                "时间",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "振幅",
                "涨跌幅",
                "涨跌额",
                "换手率",
            ]
            temp_df.index = pd.to_datetime(temp_df["时间"])
            temp_df = temp_df[start_date:end_date]
            temp_df.reset_index(drop=True, inplace=True)
            temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
            temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
            temp_df["最高"] = pd.to_numeric(temp_df["最高"])
            temp_df["最低"] = pd.to_numeric(temp_df["最低"])
            temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
            temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
            temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
            temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
            temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
            temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
            temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
            temp_df = temp_df[
                [
                    "时间",
                    "开盘",
                    "收盘",
                    "最高",
                    "最低",
                    "涨跌幅",
                    "涨跌额",
                    "成交量",
                    "成交额",
                    "振幅",
                    "换手率",
                ]
            ]
            return temp_df
            
    except Exception as e:
        logger.error(f"解析ETF分时行情失败: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    fund_etf_spot_em_df = fund_etf_spot_em()
    print(fund_etf_spot_em_df)
