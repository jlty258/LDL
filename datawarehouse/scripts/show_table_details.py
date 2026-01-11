#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Display detailed MySQL table contents
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

# Get all ODS layer tables
cursor.execute("SHOW TABLES LIKE 'ods_%'")
ods_tables = [t[0] for t in cursor.fetchall()]

print("=" * 70)
print("MySQL ODS Layer Table Detailed Statistics")
print("=" * 70)

for table in sorted(ods_tables):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        if count > 0:
            # Get table structure
            cursor.execute(f"DESCRIBE {table}")
            columns = [row[0] for row in cursor.fetchall()]
            # Get sample data
            cursor.execute(f"SELECT * FROM {table} LIMIT 3")
            samples = cursor.fetchall()
            print(f"\n{table}: {count} rows")
            print(f"  Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
            if samples:
                print("  Sample data:")
                for sample in samples:
                    print(f"    {sample[:5]}{'...' if len(sample) > 5 else ''}")
        else:
            print(f"{table}: 0 rows (empty table)")
    except Exception as e:
        print(f"{table}: Error - {str(e)[:50]}")

cursor.close()
conn.close()
