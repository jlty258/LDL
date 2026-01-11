#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
展示MySQL和PostgreSQL中的表内容
"""

import mysql.connector
import psycopg2

# MySQL配置
MYSQL_CONFIG = {
    'host': 'mysql-db',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'database': 'sqlExpert'
}

# PostgreSQL配置
POSTGRES_CONFIG = {
    'host': 'postgres-db',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres123',
    'database': 'sqlExpert'
}

def show_mysql_tables():
    """展示MySQL表内容"""
    print("=" * 60)
    print("MySQL 数据库表内容")
    print("=" * 60)
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # 获取所有表
        cursor.execute("SHOW TABLES")
        all_tables = [t[0] for t in cursor.fetchall()]
        
        print(f"\n总表数: {len(all_tables)}")
        
        # 按层分类
        ods_tables = [t for t in all_tables if t.startswith('ods_')]
        dwd_tables = [t for t in all_tables if t.startswith('dwd_')]
        dws_tables = [t for t in all_tables if t.startswith('dws_')]
        ads_tables = [t for t in all_tables if t.startswith('ads_')]
        
        print(f"  - ODS层: {len(ods_tables)} 张表")
        print(f"  - DWD层: {len(dwd_tables)} 张表")
        print(f"  - DWS层: {len(dws_tables)} 张表")
        print(f"  - ADS层: {len(ads_tables)} 张表")
        
        # 展示主要表的数据
        print("\n" + "-" * 60)
        print("主要表数据统计:")
        print("-" * 60)
        
        # 物料表（最大表）
        cursor.execute("SELECT COUNT(*) FROM ods_material_master")
        count = cursor.fetchone()[0]
        print(f"\n1. 物料表 (ods_material_master): {count} 行")
        cursor.execute("SELECT material_id, material_code, material_name, material_category, standard_price FROM ods_material_master LIMIT 5")
        print("   示例数据:")
        for row in cursor.fetchall():
            print(f"     {row}")
        
        # 产品表
        cursor.execute("SELECT COUNT(*) FROM ods_product_master")
        count = cursor.fetchone()[0]
        print(f"\n2. 产品表 (ods_product_master): {count} 行")
        cursor.execute("SELECT product_id, product_code, product_name, product_category, standard_price FROM ods_product_master LIMIT 5")
        print("   示例数据:")
        for row in cursor.fetchall():
            print(f"     {row}")
        
        # 客户表
        cursor.execute("SELECT COUNT(*) FROM ods_customer_master")
        count = cursor.fetchone()[0]
        print(f"\n3. 客户表 (ods_customer_master): {count} 行")
        if count > 0:
            cursor.execute("SELECT customer_id, customer_code, customer_name, customer_type, region FROM ods_customer_master LIMIT 5")
            print("   示例数据:")
            for row in cursor.fetchall():
                print(f"     {row}")
        else:
            print("   (暂无数据)")
        
        # 订单表（如果存在）
        try:
            cursor.execute("SELECT COUNT(*) FROM ods_order_master")
            count = cursor.fetchone()[0]
            print(f"\n4. 订单表 (ods_order_master): {count} 行")
            if count > 0:
                cursor.execute("SELECT order_id, order_no, customer_id, order_date, total_amount FROM ods_order_master LIMIT 5")
                print("   示例数据:")
                for row in cursor.fetchall():
                    print(f"     {row}")
            else:
                print("   (暂无数据)")
        except:
            print("\n4. 订单表 (ods_order_master): (表不存在)")
        
        # 其他ODS层表统计
        print("\n" + "-" * 60)
        print("其他ODS层表数据统计:")
        print("-" * 60)
        for table in sorted(ods_tables):
            if table not in ['ods_material_master', 'ods_product_master', 'ods_customer_master', 'ods_order_master']:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        print(f"  {table}: {count} 行")
                except:
                    pass
        
        # DWD层表统计
        print("\n" + "-" * 60)
        print("DWD层表数据统计:")
        print("-" * 60)
        for table in sorted(dwd_tables):
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} 行")
            except:
                print(f"  {table}: (无法查询)")
        
        # DWS层表统计
        print("\n" + "-" * 60)
        print("DWS层表数据统计:")
        print("-" * 60)
        for table in sorted(dws_tables):
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} 行")
            except:
                print(f"  {table}: (无法查询)")
        
        # ADS层表统计
        print("\n" + "-" * 60)
        print("ADS层表数据统计:")
        print("-" * 60)
        for table in sorted(ads_tables):
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} 行")
            except:
                print(f"  {table}: (无法查询)")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ MySQL错误: {e}")

def show_postgres_tables():
    """展示PostgreSQL表内容"""
    print("\n" + "=" * 60)
    print("PostgreSQL 数据库表内容")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # 获取所有表
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        all_tables = [t[0] for t in cursor.fetchall()]
        
        print(f"\n总表数: {len(all_tables)}")
        
        if len(all_tables) == 0:
            print("  (暂无表)")
            return
        
        # 按层分类
        ods_tables = [t for t in all_tables if t.startswith('ods_')]
        dwd_tables = [t for t in all_tables if t.startswith('dwd_')]
        dws_tables = [t for t in all_tables if t.startswith('dws_')]
        ads_tables = [t for t in all_tables if t.startswith('ads_')]
        
        print(f"  - ODS层: {len(ods_tables)} 张表")
        print(f"  - DWD层: {len(dwd_tables)} 张表")
        print(f"  - DWS层: {len(dws_tables)} 张表")
        print(f"  - ADS层: {len(ads_tables)} 张表")
        
        # 展示前10张表的数据统计
        print("\n" + "-" * 60)
        print("表数据统计 (前10张):")
        print("-" * 60)
        for table in all_tables[:10]:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} 行")
            except Exception as e:
                print(f"  {table}: (无法查询 - {str(e)[:50]})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ PostgreSQL错误: {e}")

if __name__ == "__main__":
    show_mysql_tables()
    show_postgres_tables()
    print("\n" + "=" * 60)
