#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试脚本：详细检查策略数据
"""

import os
import sys

current_dir = os.path.abspath(__file__)
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)
sys.path.insert(0, root_dir)

import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import instock.core.stockfetch as stf
import instock.core.singleton_stock as ss

print("=== 测试策略数据详情 ===")
today = datetime.date.today()

# 获取所有股票数据
stock_data = stf.fetch_stocks(today)
if stock_data is None:
    print("获取股票数据失败")
    exit(1)

print(f"股票总数: {len(stock_data)}")

# 获取前20只股票的历史数据
from instock.core.singleton_stock import stock_hist_data

# 只取一小部分测试
test_stocks = stock_data.head(20)
stocks_list = [tuple(x) for x in test_stocks[['date', 'code']].values]
print(f"测试股票数量: {len(stocks_list)}")

# 获取历史数据
stocks_data = stock_hist_data(date=today).get_data()

if stocks_data is None:
    print("获取历史数据失败")
    exit(1)

print(f"历史数据股票数量: {len(stocks_data)}")

# 检查每只股票的最后一天数据
from instock.core.strategy.enter import check_volume

print("\n=== 详细检查每只股票 ===")
print(f"{'代码':<10} {'收盘价':<10} {'开盘价':<10} {'涨跌幅':<12} {'成交量':<15} {'符合策略':<10}")
print("-" * 80)

match_count = 0
for stock in stocks_list:
    if stock in stocks_data:
        data = stocks_data[stock]
        if data is not None and len(data) > 0:
            last_row = data.iloc[-1]
            close = last_row['close']
            open_price = last_row['open']
            p_change = last_row.get('p_change', 0)
            volume = last_row['volume']

            try:
                result = check_volume(stock, data, date=today)
                if result:
                    match_count += 1
                    status = "✓ 符合"
                else:
                    status = "✗ 不符合"
            except Exception as e:
                status = f"错误: {e}"

            print(f"{stock[1]:<10} {close:<10.2f} {open_price:<10.2f} {p_change:<12.2f} {volume:<15.0f} {status:<10}")

print(f"\n符合策略的股票数量: {match_count}")

# 检查涨跌幅分布
print("\n=== 涨跌幅分布 ===")
p_changes = []
for stock in stocks_list:
    if stock in stocks_data:
        data = stocks_data[stock]
        if data is not None and len(data) > 0:
            p_change = data.iloc[-1].get('p_change', 0)
            p_changes.append(p_change)

if p_changes:
    import numpy as np
    print(f"最大涨幅: {max(p_changes):.2f}%")
    print(f"最小涨幅: {min(p_changes):.2f}%")
    print(f"平均涨幅: {np.mean(p_changes):.2f}%")
    print(f"涨幅>=2%的股票数量: {len([x for x in p_changes if x >= 2])}")

print("\n=== 调试完成 ===")
