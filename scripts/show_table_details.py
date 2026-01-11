#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细展示MySQL表内容
"""

import mysql.connector

MYSQL_CONFIG = {
    'host': 'mysql-db',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'database': 'sqlExpert'
}

conn = mysql.connector.connect(**MYSQL_CONFIG)
cursor = conn.cursor()

# 获取所有ODS层表
cursor.execute("SHOW TABLES LIKE 'ods_%'")
ods_tables = [t[0] for t in cursor.fetchall()]

print("=" * 70)
print("MySQL ODS层表详细数据统计")
print("=" * 70)

for table in sorted(ods_tables):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        if count > 0:
            # 获取表结构
            cursor.execute(f"DESCRIBE {table}")
            columns = [row[0] for row in cursor.fetchall()]
            # 获取示例数据
            cursor.execute(f"SELECT * FROM {table} LIMIT 3")
            samples = cursor.fetchall()
            print(f"\n{table}: {count} 行")
            print(f"  列: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
            if samples:
                print("  示例数据:")
                for sample in samples:
                    print(f"    {sample[:5]}{'...' if len(sample) > 5 else ''}")
        else:
            print(f"{table}: 0 行 (空表)")
    except Exception as e:
        print(f"{table}: 错误 - {str(e)[:50]}")

cursor.close()
conn.close()
