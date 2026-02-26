#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试脚本：检查策略数据生成流程
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
import instock.core.tablestructure as tbs
import instock.lib.database as mdb

print("=== 步骤1: 获取当天股票数据 ===")
today = datetime.date.today()
print(f"获取日期: {today}")

try:
    stock_data = stf.fetch_stocks(today)
    if stock_data is not None:
        print(f"成功获取股票数据: {len(stock_data)}条")
        print(f"列名: {list(stock_data.columns)[:5]}...")
        print(f"样例数据:\n{stock_data.head(2)}")
    else:
        print("获取股票数据失败: 返回None")
except Exception as e:
    print(f"获取股票数据异常: {e}")

print("\n=== 步骤2: 获取股票代码列表 ===")
try:
    stock_list = stf.fetch_stocks(today)
    if stock_list is not None:
        codes = list(stock_list['code'].values)
        print(f"股票代码数量: {len(codes)}")
        print(f"前10个代码: {codes[:10]}")
    else:
        print("无法获取股票代码列表")
except Exception as e:
    print(f"获取股票代码列表异常: {e}")

print("\n=== 步骤3: 测试单只股票历史数据 ===")
test_code = '600000'
try:
    import instock.core.stockfetch as stf
    result = stf.fetch_stock_hist((str(today), test_code), None, False)
    if result is not None:
        print(f"股票 {test_code} 历史数据: {len(result)}条")
        print(f"列名: {list(result.columns)}")
    else:
        print(f"股票 {test_code} 历史数据获取失败")
except Exception as e:
    print(f"获取股票 {test_code} 历史数据异常: {e}")

print("\n=== 步骤4: 检查数据库连接 ===")
try:
    conn = mdb.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM cn_stock_spot')
    count = cursor.fetchone()[0]
    print(f"数据库连接正常: cn_stock_spot表有{count}条数据")
    conn.close()
except Exception as e:
    print(f"数据库连接异常: {e}")

print("\n=== 调试完成 ===")
