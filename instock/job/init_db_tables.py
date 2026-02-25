#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库表初始化脚本
确保所有必要的表都已创建
"""

import os
import sys

# 添加项目路径
current_dir = os.path.abspath(__file__)
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
root_dir = os.path.dirname(grandparent_dir)
sys.path.insert(0, root_dir)

import instock.lib.database as mdb

def create_strategy_tables():
    """创建策略相关的表"""
    conn = mdb.get_connection()
    cursor = conn.cursor()
    
    # 策略表列表
    strategy_tables = [
        'cn_stock_strategy_enter',
        'cn_stock_strategy_keep_increasing',
        'cn_stock_strategy_parking_apron',
        'cn_stock_strategy_backtrace_ma250',
        'cn_stock_strategy_breakthrough_platform',
        'cn_stock_strategy_low_backtrace_increase',
        'cn_stock_strategy_turtle_trade',
        'cn_stock_strategy_high_tight_flag',
        'cn_stock_strategy_climax_limitdown',
        'cn_stock_strategy_low_atr'
    ]
    
    for table_name in strategy_tables:
        try:
            # 检查表是否存在
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                # 创建表
                create_sql = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    date DATE,
                    code VARCHAR(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                    name VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                    PRIMARY KEY (date, code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                '''
                cursor.execute(create_sql)
                print(f'创建表 {table_name} 成功')
            else:
                print(f'表 {table_name} 已存在')
        except Exception as e:
            print(f'创建表 {table_name} 失败: {e}')
    
    # 创建关注表
    try:
        cursor.execute("SHOW TABLES LIKE 'cn_stock_attention'")
        if not cursor.fetchone():
            create_sql = '''
            CREATE TABLE IF NOT EXISTS cn_stock_attention (
                datetime DATETIME,
                code VARCHAR(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                PRIMARY KEY (code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            '''
            cursor.execute(create_sql)
            print('创建表 cn_stock_attention 成功')
        else:
            print('表 cn_stock_attention 已存在')
    except Exception as e:
        print(f'创建表 cn_stock_attention 失败: {e}')
    
    conn.commit()
    conn.close()
    print('所有表创建完成！')

def main():
    """主函数"""
    print('开始初始化数据库表...')
    create_strategy_tables()
    print('数据库表初始化完成！')

if __name__ == '__main__':
    main()