#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试脚本：测试策略执行流程
"""

import os
import sys

current_dir = os.path.abspath(__file__)
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)
sys.path.insert(0, root_dir)

import datetime
import logging
import concurrent.futures

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import instock.core.stockfetch as stf
import instock.core.singleton_stock as ss
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.core.strategy.enter import check_volume

print("=== 步骤A: 测试单只股票策略计算 ===")
today = datetime.date.today()

try:
    # 获取单只股票的历史数据
    test_code = '600000'
    stock_key = (str(today), test_code)

    # 使用 fetch_stock_hist 获取历史数据
    hist_data = stf.fetch_stock_hist(stock_key, None, False)

    if hist_data is not None:
        print(f"股票 {test_code} 历史数据: {len(hist_data)}条")
        print(f"列名: {list(hist_data.columns)}")

        # 测试策略函数
        print(f"\n测试策略函数 check_volume...")
        result = check_volume(stock_key, hist_data, date=today)
        print(f"策略结果: {result}")
    else:
        print(f"股票 {test_code} 历史数据获取失败")

except Exception as e:
    import traceback
    print(f"步骤A异常: {e}")
    traceback.print_exc()

print("\n=== 步骤B: 测试多只股票策略计算 ===")

try:
    # 获取多只股票数据
    stock_data = stf.fetch_stocks(today)
    if stock_data is None:
        print("获取股票数据失败")
    else:
        # 取前10只股票测试
        test_stocks = stock_data.head(10)
        stocks_list = [tuple(x) for x in test_stocks[['date', 'code']].values]
        print(f"测试股票数量: {len(stocks_list)}")

        # 获取历史数据
        from instock.core.singleton_stock import stock_hist_data
        stocks_data = stock_hist_data(date=today).get_data()

        if stocks_data is None:
            print("获取历史数据失败")
        else:
            print(f"历史数据股票数量: {len(stocks_data)}")

            # 测试策略计算
            results = []
            for stock in stocks_list[:5]:
                if stock in stocks_data:
                    try:
                        result = check_volume(stock, stocks_data[stock], date=today)
                        if result:
                            results.append(stock)
                            print(f"  {stock[1]}: 符合策略")
                        else:
                            print(f"  {stock[1]}: 不符合策略")
                    except Exception as e:
                        print(f"  {stock[1]}: 异常 - {e}")

            print(f"\n符合策略的股票数量: {len(results)}")

except Exception as e:
    import traceback
    print(f"步骤B异常: {e}")
    traceback.print_exc()

print("\n=== 步骤C: 检查策略表是否存在 ===")

try:
    conn = mdb.get_connection()
    cursor = conn.cursor()

    strategy_tables = [
        'cn_stock_strategy_enter',
        'cn_stock_strategy_keep_increasing',
        'cn_stock_strategy_parking_apron',
    ]

    for table in strategy_tables:
        cursor.execute(f"SHOW TABLES LIKE '{table}'")
        if cursor.fetchone():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count}条")
        else:
            print(f"{table}: 表不存在")

    conn.close()
except Exception as e:
    print(f"步骤C异常: {e}")

print("\n=== 调试完成 ===")
