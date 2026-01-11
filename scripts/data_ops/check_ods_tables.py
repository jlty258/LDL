#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查所有ODS表的数据行数
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

def check_mysql():
    """检查MySQL的ODS表"""
    print("=" * 70)
    print("MySQL ODS表数据统计")
    print("=" * 70)
    
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SHOW TABLES LIKE 'ods_%'")
    tables = [t[0] for t in cursor.fetchall()]
    tables.sort()
    
    print(f"\n共 {len(tables)} 张ODS表:\n")
    
    total_rows = 0
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        total_rows += count
        status = "✓" if count > 0 else "✗"
        max_indicator = " (最大表)" if count >= 10000 else ""
        print(f"  {status} {table:35} {count:>6} 行{max_indicator}")
    
    print(f"\n总计: {total_rows} 行数据")
    cursor.close()
    conn.close()

def check_postgresql():
    """检查PostgreSQL的ODS表"""
    print("\n" + "=" * 70)
    print("PostgreSQL ODS表数据统计")
    print("=" * 70)
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'ods_%'
        ORDER BY table_name
    """)
    tables = [t[0] for t in cursor.fetchall()]
    
    print(f"\n共 {len(tables)} 张ODS表:\n")
    
    total_rows = 0
    for table in tables:
        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cursor.fetchone()[0]
        total_rows += count
        status = "✓" if count > 0 else "✗"
        max_indicator = " (最大表)" if count >= 10000 else ""
        print(f"  {status} {table:35} {count:>6} 行{max_indicator}")
    
    print(f"\n总计: {total_rows} 行数据")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_mysql()
    check_postgresql()
    print("\n" + "=" * 70)
    print("检查完成！")
    print("=" * 70)
