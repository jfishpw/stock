#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDD测试：验证股票指标数据是否正确生成

测试用例：
1. cn_stock_indicators 表在指定日期有数据
2. cn_stock_indicators_buy 表在指定日期有数据  
3. cn_stock_indicators_sell 表在指定日期有数据
"""

import os
import sys
import datetime

# 添加项目路径
current_dir = os.path.abspath(__file__)
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)
sys.path.insert(0, root_dir)

import instock.lib.database as mdb


def test_indicators_data_exists(date_str):
    """测试1：cn_stock_indicators表在指定日期有数据"""
    print(f"\n=== 测试1: cn_stock_indicators 表数据 ===")
    
    conn = mdb.get_connection()
    cursor = conn.cursor()
    
    try:
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'cn_stock_indicators'")
        if not cursor.fetchone():
            print(f"  ❌ FAIL: 表 cn_stock_indicators 不存在")
            return False
        
        # 检查数据
        cursor.execute(f"SELECT COUNT(*) FROM cn_stock_indicators WHERE date = '{date_str}'")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"  ✓ PASS: cn_stock_indicators 有 {count} 条数据 (日期: {date_str})")
            return True
        else:
            print(f"  ❌ FAIL: cn_stock_indicators 在 {date_str} 没有数据")
            return False
    except Exception as e:
        print(f"  ❌ FAIL: 错误 - {e}")
        return False
    finally:
        conn.close()


def test_indicators_buy_data_exists(date_str):
    """测试2：cn_stock_indicators_buy表在指定日期有数据"""
    print(f"\n=== 测试2: cn_stock_indicators_buy 表数据 ===")
    
    conn = mdb.get_connection()
    cursor = conn.cursor()
    
    try:
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'cn_stock_indicators_buy'")
        if not cursor.fetchone():
            print(f"  ❌ FAIL: 表 cn_stock_indicators_buy 不存在")
            return False
        
        # 检查数据
        cursor.execute(f"SELECT COUNT(*) FROM cn_stock_indicators_buy WHERE date = '{date_str}'")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"  ✓ PASS: cn_stock_indicators_buy 有 {count} 条数据 (日期: {date_str})")
            return True
        else:
            print(f"  ❌ FAIL: cn_stock_indicators_buy 在 {date_str} 没有数据")
            return False
    except Exception as e:
        print(f"  ❌ FAIL: 错误 - {e}")
        return False
    finally:
        conn.close()


def test_indicators_sell_data_exists(date_str):
    """测试3：cn_stock_indicators_sell表在指定日期有数据"""
    print(f"\n=== 测试3: cn_stock_indicators_sell 表数据 ===")
    
    conn = mdb.get_connection()
    cursor = conn.cursor()
    
    try:
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'cn_stock_indicators_sell'")
        if not cursor.fetchone():
            print(f"  ❌ FAIL: 表 cn_stock_indicators_sell 不存在")
            return False
        
        # 检查数据
        cursor.execute(f"SELECT COUNT(*) FROM cn_stock_indicators_sell WHERE date = '{date_str}'")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"  ✓ PASS: cn_stock_indicators_sell 有 {count} 条数据 (日期: {date_str})")
            return True
        else:
            print(f"  ❌ FAIL: cn_stock_indicators_sell 在 {date_str} 没有数据")
            return False
    except Exception as e:
        print(f"  ❌ FAIL: 错误 - {e}")
        return False
    finally:
        conn.close()


def main():
    print("=" * 60)
    print("TDD测试：验证股票指标数据")
    print("=" * 60)
    
    # 测试日期（可以修改为其他日期）
    test_date = "2026-02-26"
    
    # 运行测试
    results = []
    results.append(test_indicators_data_exists(test_date))
    results.append(test_indicators_buy_data_exists(test_date))
    results.append(test_indicators_sell_data_exists(test_date))
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("测试汇总")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"通过: {passed}/{total}")
    
    if all(results):
        print("✓ 所有测试通过！")
    else:
        print("❌ 部分测试失败，需要修复")
        print("\n可能的原因：")
        print("1. indicators_data_daily_job 没有被 execute_daily_job 调用")
        print("2. 数据获取失败")
        print("3. 数据库表创建失败")
    
    return all(results)


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
