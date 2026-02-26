#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试脚本：检查数据格式问题
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

print("=== 检查数据格式 ===")
today = datetime.date.today()

# 获取当天股票数据
stock_data = stf.fetch_stocks(today)
if stock_data is None:
    print("获取股票数据失败")
    exit(1)

print(f"股票数据总条数: {len(stock_data)}")
print(f"列名: {list(stock_data.columns)}")
print(f"\n前3条数据:")
print(stock_data.head(3))

# 检查股票代码格式
stocks_list = [tuple(x) for x in stock_data[['date', 'code']].values]
print(f"\n股票列表格式示例:")
for s in stocks_list[:5]:
    print(f"  {s} (type: {type(s)}, code type: {type(s[1])})")

# 获取历史数据
from instock.core.singleton_stock import stock_hist_data

stocks_data = stock_hist_data(date=today).get_data()

if stocks_data is None:
    print("\n获取历史数据失败")
    exit(1)

print(f"\n历史数据股票数量: {len(stocks_data)}")
print(f"历史数据key格式示例:")
for i, (key, value) in enumerate(list(stocks_data.items())[:5]):
    print(f"  key: {key} (type: {type(key)})")

# 检查匹配问题
print("\n=== 检查匹配问题 ===")
test_stock = stocks_list[0]
print(f"查找: {test_stock}")
print(f"是否在历史数据中: {test_stock in stocks_data}")

# 尝试不同格式的key
test_code = test_stock[1]
print(f"\n尝试用代码字符串查找: {test_code}")
if test_code in stocks_data:
    print("  找到!")
else:
    print("  未找到")

# 检查第一个历史数据的结构
print(f"\n第一个历史数据的结构:")
first_key = list(stocks_data.keys())[0]
first_data = stocks_data[first_key]
print(f"  key: {first_key}")
print(f"  数据类型: {type(first_data)}")
if first_data is not None and len(first_data) > 0:
    print(f"  数据条数: {len(first_data)}")
    print(f"  列名: {list(first_data.columns)}")
    print(f"  最后一条: {first_data.iloc[-1].to_dict()}")
else:
    print(f"  数据为空或None")

print("\n=== 调试完成 ===")
