#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化数据库表结构
"""
import mysql.connector
import os

DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'charset': 'utf8mb4'
}

def execute_sql_file(conn, sql_file_path):
    """执行SQL文件"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割SQL语句
        statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
        
        cursor = conn.cursor()
        for statement in statements:
            if statement:
                try:
                    cursor.execute(statement)
                    print(f"  ✓ 执行: {statement[:50]}...")
                except Exception as e:
                    print(f"  ⚠ 警告: {e}")
        
        conn.commit()
        cursor.close()
        print(f"✓ {os.path.basename(sql_file_path)} 执行完成")
        return True
    except Exception as e:
        print(f"❌ 错误: {e}")
        return False

def main():
    print("=" * 60)
    print("初始化制造业数仓数据库")
    print("=" * 60)
    
    try:
        # 连接数据库
        print("\n1. 连接数据库...")
        conn = mysql.connector.connect(**DB_CONFIG)
        print("✓ 数据库连接成功")
        
        # 创建数据库
        print("\n2. 创建数据库...")
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS sqlExpert CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("USE sqlExpert")
        cursor.close()
        print("✓ 数据库创建成功")
        
        # 执行SQL文件
        base_dir = os.path.dirname(os.path.dirname(__file__))
        sql_files = [
            '02_ods_tables.sql',
            '03_dwd_tables.sql',
            '04_dws_tables.sql',
            '05_ads_tables.sql'
        ]
        
        print("\n3. 创建表结构...")
        for sql_file in sql_files:
            sql_path = os.path.join(base_dir, 'sql', sql_file)
            if os.path.exists(sql_path):
                print(f"\n执行 {sql_file}...")
                execute_sql_file(conn, sql_path)
            else:
                print(f"⚠ 文件不存在: {sql_path}")
        
        # 检查表数量
        print("\n4. 验证表结构...")
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"✓ 共创建 {len(tables)} 张表")
        
        # 统计各层表数量
        ods_count = sum(1 for t in tables if t[0].startswith('ods_'))
        dwd_count = sum(1 for t in tables if t[0].startswith('dwd_'))
        dws_count = sum(1 for t in tables if t[0].startswith('dws_'))
        ads_count = sum(1 for t in tables if t[0].startswith('ads_'))
        
        print(f"  - ODS层: {ods_count} 张表")
        print(f"  - DWD层: {dwd_count} 张表")
        print(f"  - DWS层: {dws_count} 张表")
        print(f"  - ADS层: {ads_count} 张表")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("数据库初始化完成！")
        print("=" * 60)
        
    except mysql.connector.Error as e:
        print(f"❌ 数据库错误: {e}")
    except Exception as e:
        print(f"❌ 错误: {e}")

if __name__ == "__main__":
    main()
