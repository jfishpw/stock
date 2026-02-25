# -*- coding:utf-8 -*-
# !/usr/bin/env python
"""
Date: 2022/3/15 17:32
Desc: 东方财富网-数据中心-龙虎榜单
https://data.eastmoney.com/stock/tradedetail.html
"""
import random
import time
import logging
import pandas as pd
from tqdm import tqdm
from instock.core.multi_source_fetcher import multi_fetcher, DataSource

__author__ = 'myh '
__date__ = '2025/12/31 '

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def stock_lhb_detail_em(
    start_date: str = "20230403", end_date: str = "20230417"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-龙虎榜单-龙虎榜详情
    https://data.eastmoney.com/stock/tradedetail.html
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :return: 龙虎榜详情
    :rtype: pandas.DataFrame
    """
    start_date = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
    end_date = "-".join([end_date[:4], end_date[4:6], end_date[6:]])
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "SECURITY_CODE,TRADE_DATE",
        "sortTypes": "1,-1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_DAILYBILLBOARD_DETAILSNEW",
        "columns": "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,EXPLANATION,D1_CLOSE_ADJCHRATE,D2_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,SECURITY_TYPE_CODE",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(TRADE_DATE<='{end_date}')(TRADE_DATE>='{start_date}')",
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json.get("result"):
            logger.warning("龙虎榜数据为空")
            return pd.DataFrame()
            
        total_page_num = data_json["result"]["pages"]
        big_df = pd.DataFrame()
        for page in range(1, total_page_num + 1):
            time.sleep(random.uniform(0.5, 1))
            params.update({"pageNumber": page})
            r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
            data_json = r.json()
            if data_json.get("result") and data_json["result"].get("data"):
                temp_df = pd.DataFrame(data_json["result"]["data"])
                big_df = pd.concat([big_df, temp_df], ignore_index=True)

        if big_df.empty:
            return pd.DataFrame()

        big_df.reset_index(inplace=True)
        big_df["index"] = big_df.index + 1
        big_df.rename(
            columns={
                "index": "-",
                "SECURITY_CODE": "代码",
                "SECUCODE": "-",
                "SECURITY_NAME_ABBR": "名称",
                "TRADE_DATE": "上榜日",
                "EXPLAIN": "解读",
                "CLOSE_PRICE": "收盘价",
                "CHANGE_RATE": "涨跌幅",
                "BILLBOARD_NET_AMT": "龙虎榜净买额",
                "BILLBOARD_BUY_AMT": "龙虎榜买入额",
                "BILLBOARD_SELL_AMT": "龙虎榜卖出额",
                "BILLBOARD_DEAL_AMT": "龙虎榜成交额",
                "ACCUM_AMOUNT": "市场总成交额",
                "DEAL_NET_RATIO": "净买额占总成交比",
                "DEAL_AMOUNT_RATIO": "成交额占总成交比",
                "TURNOVERRATE": "换手率",
                "FREE_MARKET_CAP": "流通市值",
                "EXPLANATION": "上榜原因",
                "D1_CLOSE_ADJCHRATE": "上榜后1日",
                "D2_CLOSE_ADJCHRATE": "上榜后2日",
                "D5_CLOSE_ADJCHRATE": "上榜后5日",
                "D10_CLOSE_ADJCHRATE": "上榜后10日",
            },
            inplace=True,
        )

        big_df = big_df[
            [
                "代码",
                "名称",
                "上榜日",
                "解读",
                "收盘价",
                "涨跌幅",
                "龙虎榜净买额",
                "龙虎榜买入额",
                "龙虎榜卖出额",
                "龙虎榜成交额",
                "市场总成交额",
                "净买额占总成交比",
                "成交额占总成交比",
                "换手率",
                "流通市值",
                "上榜原因",
                "上榜后1日",
                "上榜后2日",
                "上榜后5日",
                "上榜后10日",
            ]
        ]
        big_df["上榜日"] = pd.to_datetime(big_df["上榜日"]).dt.date

        big_df["收盘价"] = pd.to_numeric(big_df["收盘价"], errors="coerce")
        big_df["涨跌幅"] = pd.to_numeric(big_df["涨跌幅"], errors="coerce")
        big_df["龙虎榜净买额"] = pd.to_numeric(big_df["龙虎榜净买额"], errors="coerce")
        big_df["龙虎榜买入额"] = pd.to_numeric(big_df["龙虎榜买入额"], errors="coerce")
        big_df["龙虎榜卖出额"] = pd.to_numeric(big_df["龙虎榜卖出额"], errors="coerce")
        big_df["龙虎榜成交额"] = pd.to_numeric(big_df["龙虎榜成交额"], errors="coerce")
        big_df["市场总成交额"] = pd.to_numeric(big_df["市场总成交额"], errors="coerce")
        big_df["净买额占总成交比"] = pd.to_numeric(big_df["净买额占总成交比"], errors="coerce")
        big_df["成交额占总成交比"] = pd.to_numeric(big_df["成交额占总成交比"], errors="coerce")
        big_df["换手率"] = pd.to_numeric(big_df["换手率"], errors="coerce")
        big_df["流通市值"] = pd.to_numeric(big_df["流通市值"], errors="coerce")
        big_df["上榜后1日"] = pd.to_numeric(big_df["上榜后1日"], errors="coerce")
        big_df["上榜后2日"] = pd.to_numeric(big_df["上榜后2日"], errors="coerce")
        big_df["上榜后5日"] = pd.to_numeric(big_df["上榜后5日"], errors="coerce")
        big_df["上榜后10日"] = pd.to_numeric(big_df["上榜后10日"], errors="coerce")
        return big_df
        
    except Exception as e:
        logger.error(f"获取龙虎榜详情失败: {e}")
        return pd.DataFrame()


def stock_lhb_stock_statistic_em(symbol: str = "近一月") -> pd.DataFrame:
    """
    东方财富网-数据中心-龙虎榜单-个股上榜统计
    https://data.eastmoney.com/stock/tradedetail.html
    :param symbol: choice of {"近一月", "近三月", "近六月", "近一年"}
    :type symbol: str
    :return: 个股上榜统计
    :rtype: pandas.DataFrame
    """
    symbol_map = {
        "近一月": "01",
        "近三月": "02",
        "近六月": "03",
        "近一年": "04",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "BILLBOARD_TIMES,LATEST_TDATE,SECURITY_CODE",
        "sortTypes": "-1,-1,1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_BILLBOARD_TRADEALL",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(STATISTICS_CYCLE="{symbol_map[symbol]}")',
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json.get("result") or not data_json["result"].get("data"):
            logger.warning("个股上榜统计数据为空")
            return pd.DataFrame()
            
        temp_df = pd.DataFrame(data_json["result"]["data"])
        temp_df.reset_index(inplace=True)
        temp_df["index"] = temp_df.index + 1
        temp_df.columns = [
            "序号",
            "-",
            "代码",
            "最近上榜日",
            "名称",
            "近1个月涨跌幅",
            "近3个月涨跌幅",
            "近6个月涨跌幅",
            "近1年涨跌幅",
            "涨跌幅",
            "收盘价",
            "-",
            "龙虎榜总成交额",
            "龙虎榜净买额",
            "-",
            "-",
            "机构买入净额",
            "上榜次数",
            "龙虎榜买入额",
            "龙虎榜卖出额",
            "机构买入总额",
            "机构卖出总额",
            "买方机构次数",
            "卖方机构次数",
            "-",
        ]
        temp_df = temp_df[
            [
                "序号",
                "代码",
                "名称",
                "最近上榜日",
                "收盘价",
                "涨跌幅",
                "上榜次数",
                "龙虎榜净买额",
                "龙虎榜买入额",
                "龙虎榜卖出额",
                "龙虎榜总成交额",
                "买方机构次数",
                "卖方机构次数",
                "机构买入净额",
                "机构买入总额",
                "机构卖出总额",
                "近1个月涨跌幅",
                "近3个月涨跌幅",
                "近6个月涨跌幅",
                "近1年涨跌幅",
            ]
        ]
        temp_df["最近上榜日"] = pd.to_datetime(temp_df["最近上榜日"]).dt.date
        return temp_df
        
    except Exception as e:
        logger.error(f"获取个股上榜统计失败: {e}")
        return pd.DataFrame()


def stock_lhb_jgmmtj_em(
    start_date: str = "20220906", end_date: str = "20220906"
) -> pd.DataFrame:
    """
    东方财富网-数据中心-龙虎榜单-机构买卖每日统计
    https://data.eastmoney.com/stock/jgmmtj.html
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :return: 机构买卖每日统计
    :rtype: pandas.DataFrame
    """
    start_date = "-".join([start_date[:4], start_date[4:6], start_date[6:]])
    end_date = "-".join([end_date[:4], end_date[4:6], end_date[6:]])
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "NET_BUY_AMT,TRADE_DATE,SECURITY_CODE",
        "sortTypes": "-1,-1,1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_ORGANIZATION_TRADE_DETAILS",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(TRADE_DATE>='{start_date}')(TRADE_DATE<='{end_date}')",
    }
    
    try:
        r = multi_fetcher.make_request(url, params=params, source=DataSource.EASTMONEY)
        data_json = r.json()
        
        if not data_json.get("result") or not data_json["result"].get("data"):
            logger.warning("机构买卖每日统计数据为空")
            return pd.DataFrame()
            
        temp_df = pd.DataFrame(data_json["result"]["data"])
        temp_df.reset_index(inplace=True)
        temp_df["index"] = temp_df.index + 1
        temp_df.columns = [
            "序号",
            "-",
            "名称",
            "代码",
            "上榜日期",
            "收盘价",
            "涨跌幅",
            "买方机构数",
            "卖方机构数",
            "机构买入总额",
            "机构卖出总额",
            "机构买入净额",
            "市场总成交额",
            "机构净买额占总成交额比",
            "换手率",
            "流通市值",
            "上榜原因",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
        ]
        temp_df = temp_df[
            [
                "序号",
                "代码",
                "名称",
                "收盘价",
                "涨跌幅",
                "买方机构数",
                "卖方机构数",
                "机构买入总额",
                "机构卖出总额",
                "机构买入净额",
                "市场总成交额",
                "机构净买额占总成交额比",
                "换手率",
                "流通市值",
                "上榜原因",
                "上榜日期",
            ]
        ]
        temp_df["上榜日期"] = pd.to_datetime(temp_df["上榜日期"]).dt.date
        temp_df["收盘价"] = pd.to_numeric(temp_df["收盘价"], errors="coerce")
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
        temp_df["买方机构数"] = pd.to_numeric(temp_df["买方机构数"], errors="coerce")
        temp_df["卖方机构数"] = pd.to_numeric(temp_df["卖方机构数"], errors="coerce")
        temp_df["机构买入总额"] = pd.to_numeric(temp_df["机构买入总额"], errors="coerce")
        temp_df["机构卖出总额"] = pd.to_numeric(temp_df["机构卖出总额"], errors="coerce")
        temp_df["机构买入净额"] = pd.to_numeric(temp_df["机构买入净额"], errors="coerce")
        temp_df["市场总成交额"] = pd.to_numeric(temp_df["市场总成交额"], errors="coerce")
        temp_df["机构净买额占总成交额比"] = pd.to_numeric(temp_df["机构净买额占总成交额比"], errors="coerce")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
        temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")

        return temp_df
        
    except Exception as e:
        logger.error(f"获取机构买卖每日统计失败: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    stock_lhb_detail_em_df = stock_lhb_detail_em(
        start_date="20250201", end_date="20250220"
    )
    print(f"获取到 {len(stock_lhb_detail_em_df)} 条龙虎榜数据")
    print(stock_lhb_detail_em_df.head())

    stock_lhb_stock_statistic_em_df = stock_lhb_stock_statistic_em(symbol="近一月")
    print(f"获取到 {len(stock_lhb_stock_statistic_em_df)} 条个股上榜统计数据")
    print(stock_lhb_stock_statistic_em_df.head())
