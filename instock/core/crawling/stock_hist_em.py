#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/6/19 15:26
Desc: 东方财富网/新浪财经-行情首页-沪深京 A 股
支持多数据源，自动故障转移
"""
import random
import time
import logging
import pandas as pd
import math
from functools import lru_cache
from instock.core.multi_source_fetcher import multi_fetcher, sina_data_fetcher, DataSource

__author__ = 'myh '
__date__ = '2025/12/31 '

logger = logging.getLogger(__name__)

def stock_zh_a_spot_em() -> pd.DataFrame:
    """
    获取沪深京 A 股-实时行情
    优先使用新浪财经数据源，失败时自动切换到东方财富网
    :return: 实时行情
    :rtype: pandas.DataFrame
    """
    try:
        logger.info("正在使用新浪财经数据源获取A股实时行情...")
        result = _stock_zh_a_spot_sina()
        logger.info(f"新浪财经数据源获取成功，共 {len(result)} 条数据")
        return result
    except Exception as e:
        logger.warning(f"新浪数据源失败: {e}，尝试东方财富网...")
        try:
            return _stock_zh_a_spot_eastmoney()
        except Exception as e2:
            logger.error(f"东方财富网数据源也失败: {e2}")
            return pd.DataFrame()

def _stock_zh_a_spot_sina() -> pd.DataFrame:
    """使用新浪财经获取实时行情"""
    all_data = []
    page = 1
    page_size = 100
    
    while True:
        data = sina_data_fetcher.get_stock_list(page=page, page_size=page_size)
        if not data:
            break
        all_data.extend(data)
        logger.info(f"新浪财经: 第{page}页获取{len(data)}条，累计{len(all_data)}条")
        if len(data) < 100:
            break
        page += 1
        time.sleep(random.uniform(0.3, 0.6))
    
    if not all_data:
        return pd.DataFrame()
    
    temp_df = pd.DataFrame(all_data)
    
    sina_to_table_mapping = {
        'code': 'code',
        'name': 'name',
        'trade': 'new_price',
        'pricechange': 'ups_downs',
        'changepercent': 'change_rate',
        'settlement': 'pre_close_price',
        'open': 'open_price',
        'high': 'high_price',
        'low': 'low_price',
        'volume': 'volume',
        'amount': 'deal_amount',
        'per': 'pe',
        'pb': 'pbnewmrq',
        'mktcap': 'total_market_cap',
        'nmc': 'free_cap',
        'turnoverratio': 'turnoverrate'
    }
    
    temp_df = temp_df.rename(columns=sina_to_table_mapping)
    
    numeric_cols = ['new_price', 'ups_downs', 'change_rate', 'pre_close_price', 
                    'open_price', 'high_price', 'low_price', 'volume', 'deal_amount',
                    'pe', 'pbnewmrq', 'total_market_cap', 'free_cap', 'turnoverrate']
    for col in numeric_cols:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')
    
    default_cols = ['code', 'name', 'new_price', 'change_rate', 'ups_downs', 'volume', 
                    'deal_amount', 'open_price', 'high_price', 'low_price', 'pre_close_price', 
                    'turnoverrate', 'pe', 'pbnewmrq', 'total_market_cap', 'free_cap']
    
    available_cols = [col for col in default_cols if col in temp_df.columns]
    temp_df = temp_df[available_cols]
    
    return temp_df

def _stock_zh_a_spot_eastmoney() -> pd.DataFrame:
    """使用东方财富网获取实时行情"""
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
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
    }
    r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
    data_json = r.json()
    
    if not data_json.get("data") or not data_json["data"].get("diff"):
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
    temp_df.columns = [
        "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅", "换手率", "市盈率动",
        "量比", "5分钟涨跌", "代码", "名称", "最高", "最低", "今开", "昨收", "总市值",
        "流通市值", "涨速", "市净率", "60日涨跌幅", "年初至今涨跌幅", "上市时间",
        "加权净资产收益率", "总股本", "已流通股份", "营业收入", "营业收入同比增长",
        "归属净利润", "归属净利润同比增长", "每股未分配利润", "毛利率", "资产负债率",
        "每股公积金", "所处行业", "每股收益", "每股净资产", "市盈率静", "市盈率TTM", "报告期"
    ]
    
    cols = ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅",
            "换手率", "量比", "今开", "最高", "最低", "昨收", "涨速", "5分钟涨跌",
            "60日涨跌幅", "年初至今涨跌幅", "市盈率动", "市盈率TTM", "市盈率静",
            "市净率", "每股收益", "每股净资产", "每股公积金", "每股未分配利润",
            "加权净资产收益率", "毛利率", "资产负债率", "营业收入", "营业收入同比增长",
            "归属净利润", "归属净利润同比增长", "报告期", "总股本", "已流通股份",
            "总市值", "流通市值", "所处行业", "上市时间"]
    
    available_cols = [c for c in cols if c in temp_df.columns]
    temp_df = temp_df[available_cols]
    
    numeric_cols = ["最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅", "换手率",
                    "量比", "今开", "最高", "最低", "昨收", "涨速", "5分钟涨跌",
                    "60日涨跌幅", "年初至今涨跌幅", "市盈率动", "市盈率TTM", "市盈率静",
                    "市净率", "每股收益", "每股净资产", "每股公积金", "每股未分配利润",
                    "加权净资产收益率", "毛利率", "资产负债率", "营业收入", "营业收入同比增长",
                    "归属净利润", "归属净利润同比增长", "总股本", "已流通股份", "总市值", "流通市值"]
    
    for col in numeric_cols:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
    
    return temp_df


@lru_cache(maxsize=1)
def code_id_map_em() -> dict:
    """
    获取股票和市场代码映射
    :return: 股票和市场代码
    :rtype: dict
    """
    try:
        return _code_id_map_sina()
    except Exception as e:
        logger.warning(f"新浪代码映射失败: {e}")
        return {}

def _code_id_map_sina() -> dict:
    """使用新浪财经获取代码映射"""
    code_id_dict = {}
    all_data = []
    page = 1
    page_size = 100
    
    while True:
        try:
            data = sina_data_fetcher.get_stock_list(page=page, page_size=page_size)
            if not data:
                break
            all_data.extend(data)
            if len(data) < 100:
                break
            page += 1
            # 增加睡眠时间，避免触发新浪API的频率限制
            import time
            import random
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as e:
            logger.warning(f"获取股票列表失败: {e}")
            break
    
    for item in all_data:
        code = item.get('code', '')
        symbol = item.get('symbol', '')
        if code and symbol:
            if symbol.startswith('bj'):
                code_id_dict[code] = 0
            elif symbol.startswith('sh'):
                code_id_dict[code] = 1
            elif symbol.startswith('sz'):
                code_id_dict[code] = 0
    
    return code_id_dict


def stock_zh_a_hist(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    """
    获取股票历史K线数据
    :param symbol: 股票代码
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
    try:
        return _stock_zh_a_hist_sina(symbol, period, start_date, end_date)
    except Exception as e:
        logger.warning(f"新浪历史数据获取失败: {e}")
        return pd.DataFrame()

def _stock_zh_a_hist_sina(symbol: str, period: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用新浪财经获取历史K线"""
    if symbol.startswith('6'):
        full_symbol = f'sh{symbol}'
    else:
        full_symbol = f'sz{symbol}'
    
    scale_map = {'daily': 240, 'weekly': 1200, 'monthly': 5200}
    scale = scale_map.get(period, 240)
    
    datalen = 1000
    
    data = sina_data_fetcher.get_kline_data(full_symbol, scale=scale, datalen=datalen)
    
    if not data:
        return pd.DataFrame()
    
    temp_df = pd.DataFrame(data)
    temp_df.columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
    
    temp_df['日期'] = pd.to_datetime(temp_df['日期'])
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    temp_df = temp_df[(temp_df['日期'] >= start_dt) & (temp_df['日期'] <= end_dt)]
    
    temp_df['开盘'] = pd.to_numeric(temp_df['开盘'], errors='coerce')
    temp_df['收盘'] = pd.to_numeric(temp_df['收盘'], errors='coerce')
    temp_df['最高'] = pd.to_numeric(temp_df['最高'], errors='coerce')
    temp_df['最低'] = pd.to_numeric(temp_df['最低'], errors='coerce')
    temp_df['成交量'] = pd.to_numeric(temp_df['成交量'], errors='coerce')
    
    temp_df['成交额'] = 0.0
    temp_df['振幅'] = 0.0
    temp_df['涨跌幅'] = 0.0
    temp_df['涨跌额'] = 0.0
    temp_df['换手率'] = 0.0
    
    if len(temp_df) > 1:
        prev_close = temp_df['收盘'].shift(1)
        temp_df['振幅'] = ((temp_df['最高'] - temp_df['最低']) / prev_close * 100).fillna(0)
        temp_df['涨跌额'] = (temp_df['收盘'] - prev_close).fillna(0)
        temp_df['涨跌幅'] = (temp_df['涨跌额'] / prev_close * 100).fillna(0)
    
    temp_df = temp_df.reset_index(drop=True)
    temp_df.index = pd.to_datetime(temp_df['日期'])
    
    return temp_df[['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]


def stock_zh_a_hist_min_em(
    symbol: str = "000001",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
    period: str = "5",
    adjust: str = "",
) -> pd.DataFrame:
    """
    获取股票分时行情
    :param symbol: 股票代码
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
    try:
        if symbol.startswith('6'):
            full_symbol = f'sh{symbol}'
        else:
            full_symbol = f'sz{symbol}'
        
        scale_map = {'1': 1, '5': 5, '15': 15, '30': 30, '60': 60}
        scale = scale_map.get(period, 5)
        
        data = sina_data_fetcher.get_kline_data(full_symbol, scale=scale, datalen=500)
        
        if not data:
            return pd.DataFrame()
        
        temp_df = pd.DataFrame(data)
        if period == '1':
            temp_df.columns = ['时间', '开盘', '最高', '最低', '收盘', '成交量']
            temp_df['成交额'] = 0
            temp_df['最新价'] = temp_df['收盘']
        else:
            temp_df.columns = ['时间', '开盘', '最高', '最低', '收盘', '成交量']
            temp_df['成交额'] = 0
            temp_df['振幅'] = 0.0
            temp_df['涨跌幅'] = 0.0
            temp_df['涨跌额'] = 0.0
            temp_df['换手率'] = 0.0
        
        for col in ['开盘', '最高', '最低', '收盘', '成交量']:
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')
        
        temp_df['时间'] = pd.to_datetime(temp_df['时间'])
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        temp_df = temp_df[(temp_df['时间'] >= start_dt) & (temp_df['时间'] <= end_dt)]
        
        temp_df = temp_df.reset_index(drop=True)
        return temp_df
        
    except Exception as e:
        logger.warning(f"分时数据获取失败: {e}")
        return pd.DataFrame()


def stock_zh_a_hist_pre_min_em(
    symbol: str = "000001",
    start_time: str = "09:00:00",
    end_time: str = "15:50:00",
) -> pd.DataFrame:
    """
    获取股票分时行情包含盘前数据
    :param symbol: 股票代码
    :type symbol: str
    :param start_time: 开始时间
    :type start_time: str
    :param end_time: 结束时间
    :type end_time: str
    :return: 每日分时行情包含盘前数据
    :rtype: pandas.DataFrame
    """
    return stock_zh_a_hist_min_em(symbol=symbol, period="1")


if __name__ == "__main__":
    print("=== 测试实时行情 ===")
    stock_zh_a_spot_em_df = stock_zh_a_spot_em()
    print(f"获取到 {len(stock_zh_a_spot_em_df)} 条数据")
    print(stock_zh_a_spot_em_df.head())

    print("\n=== 测试历史K线 ===")
    stock_zh_a_hist_df = stock_zh_a_hist(
        symbol="000001",
        period="daily",
        start_date="20250101",
        end_date="20250220",
        adjust="",
    )
    print(f"获取到 {len(stock_zh_a_hist_df)} 条数据")
    print(stock_zh_a_hist_df.head())
