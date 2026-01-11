#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证MySQL和PostgreSQL初始化结果
"""

import mysql.connector
import psycopg2

MYSQL_CONFIG = {
    'host': 'mysql-db',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'database': 'sqlExpert'
}

POSTGRES_CONFIG = {
    'host': 'postgres-db',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres123',
    'database': 'sqlExpert'
}

print("=" * 70)
print("数据库初始化验证报告")
print("=" * 70)

# MySQL验证
print("\n【MySQL数据库】")
try:
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    
    mysql_cursor.execute("SHOW TABLES")
    mysql_tables = mysql_cursor.fetchall()
    print(f"✓ 总表数: {len(mysql_tables)} 张")
    
    # 统计各层表数
    ods = [t[0] for t in mysql_tables if t[0].startswith('ods_')]
    dwd = [t[0] for t in mysql_tables if t[0].startswith('dwd_')]
    dws = [t[0] for t in mysql_tables if t[0].startswith('dws_')]
    ads = [t[0] for t in mysql_tables if t[0].startswith('ads_')]
    
    print(f"  - ODS层: {len(ods)} 张表")
    print(f"  - DWD层: {len(dwd)} 张表")
    print(f"  - DWS层: {len(dws)} 张表")
    print(f"  - ADS层: {len(ads)} 张表")
    
    # 主要表数据统计
    print("\n主要表数据:")
    tables_to_check = [
        ('ods_material_master', '物料表'),
        ('ods_customer_master', '客户表'),
        ('ods_product_master', '产品表'),
        ('ods_order_master', '订单表')
    ]
    
    for table, name in tables_to_check:
        try:
            mysql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = mysql_cursor.fetchone()[0]
            print(f"  ✓ {name} ({table}): {count} 行")
        except:
            print(f"  ✗ {name} ({table}): 表不存在或查询失败")
    
    mysql_cursor.close()
    mysql_conn.close()
except Exception as e:
    print(f"❌ MySQL验证错误: {e}")

# PostgreSQL验证
print("\n【PostgreSQL数据库】")
try:
    postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
    postgres_cursor = postgres_conn.cursor()
    
    postgres_cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' ORDER BY table_name
    """)
    postgres_tables = postgres_cursor.fetchall()
    print(f"✓ 总表数: {len(postgres_tables)} 张")
    
    # 统计各层表数
    ods = [t[0] for t in postgres_tables if t[0].startswith('ods_')]
    dwd = [t[0] for t in postgres_tables if t[0].startswith('dwd_')]
    dws = [t[0] for t in postgres_tables if t[0].startswith('dws_')]
    ads = [t[0] for t in postgres_tables if t[0].startswith('ads_')]
    
    print(f"  - ODS层: {len(ods)} 张表")
    print(f"  - DWD层: {len(dwd)} 张表")
    print(f"  - DWS层: {len(dws)} 张表")
    print(f"  - ADS层: {len(ads)} 张表")
    
    # 主要表数据统计
    print("\n主要表数据:")
    for table, name in tables_to_check:
        try:
            postgres_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = postgres_cursor.fetchone()[0]
            print(f"  ✓ {name} ({table}): {count} 行")
        except:
            print(f"  ✗ {name} ({table}): 表不存在或查询失败")
    
    postgres_cursor.close()
    postgres_conn.close()
except Exception as e:
    print(f"❌ PostgreSQL验证错误: {e}")

print("\n" + "=" * 70)
print("验证完成！")
print("=" * 70)
