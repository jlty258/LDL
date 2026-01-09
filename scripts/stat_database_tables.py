#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统计 MySQL 和 PostgreSQL 数据库中的库表信息
"""
import mysql.connector
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

# MySQL 配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'charset': 'utf8mb4'
}

# PostgreSQL 配置
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres123'
}

def get_mysql_databases():
    """获取 MySQL 所有数据库"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall() if db[0] not in ['information_schema', 'performance_schema', 'mysql', 'sys']]
        cursor.close()
        conn.close()
        return databases
    except Exception as e:
        print(f"❌ MySQL 连接错误: {e}")
        return []

def get_mysql_tables(database):
    """获取 MySQL 指定数据库的所有表"""
    try:
        config = MYSQL_CONFIG.copy()
        config['database'] = database
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        # 获取每个表的详细信息
        table_info = {}
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                row_count = cursor.fetchone()[0]
                
                # 统计字段数量
                cursor.execute(f"DESCRIBE `{table}`")
                columns = cursor.fetchall()
                column_count = len(columns)
                
                table_info[table] = {
                    'row_count': row_count,
                    'column_count': column_count
                }
            except Exception as e:
                # 如果表无法访问，记录错误但继续
                table_info[table] = {
                    'row_count': -1,
                    'column_count': -1,
                    'error': str(e)
                }
        
        cursor.close()
        conn.close()
        return table_info
    except Exception as e:
        print(f"  ⚠ 获取表信息错误 ({database}): {e}")
        return {}

def get_postgres_databases():
    """获取 PostgreSQL 所有数据库"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT datname 
            FROM pg_database 
            WHERE datistemplate = false 
            AND datname NOT IN ('postgres', 'template0', 'template1')
            ORDER BY datname
        """)
        databases = [db[0] for db in cursor.fetchall()]
        cursor.close()
        conn.close()
        return databases
    except Exception as e:
        print(f"❌ PostgreSQL 连接错误: {e}")
        return []

def get_postgres_tables(database):
    """获取 PostgreSQL 指定数据库的所有表"""
    try:
        config = POSTGRES_CONFIG.copy()
        config['database'] = database
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        # 获取所有表（排除系统表）
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        tables = [table[0] for table in cursor.fetchall()]
        
        # 获取每个表的详细信息
        table_info = {}
        for table in tables:
            try:
                # 获取行数
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                row_count = cursor.fetchone()[0]
                
                # 获取字段数量
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                """, (table,))
                column_count = cursor.fetchone()[0]
                
                table_info[table] = {
                    'row_count': row_count,
                    'column_count': column_count
                }
            except Exception as e:
                # 如果表无法访问，记录错误但继续
                table_info[table] = {
                    'row_count': -1,
                    'column_count': -1,
                    'error': str(e)
                }
        
        cursor.close()
        conn.close()
        return table_info
    except Exception as e:
        print(f"  ⚠ 获取表信息错误 ({database}): {e}")
        return {}

def print_statistics():
    """打印统计信息"""
    print("=" * 80)
    print("数据库库表统计报告")
    print("=" * 80)
    
    # 初始化统计变量
    mysql_total_tables = 0
    mysql_total_rows = 0
    postgres_total_tables = 0
    postgres_total_rows = 0
    
    # MySQL 统计
    print("\n" + "=" * 80)
    print("📊 MySQL 数据库统计")
    print("=" * 80)
    
    mysql_databases = get_mysql_databases()
    if not mysql_databases:
        print("⚠ MySQL 数据库连接失败或无数据库")
    else:
        
        for db in mysql_databases:
            print(f"\n📁 数据库: {db}")
            print("-" * 80)
            tables = get_mysql_tables(db)
            
            if not tables:
                print("  (无表)")
            else:
                print(f"  表数量: {len(tables)}")
                print(f"\n  {'表名':<40} {'行数':<15} {'字段数':<10}")
                print(f"  {'-'*40} {'-'*15} {'-'*10}")
                
                for table_name, info in sorted(tables.items()):
                    if info['row_count'] >= 0:
                        print(f"  {table_name:<40} {info['row_count']:<15} {info['column_count']:<10}")
                        mysql_total_rows += info['row_count']
                    else:
                        print(f"  {table_name:<40} {'错误':<15} {'错误':<10}")
                
                mysql_total_tables += len(tables)
        
        print("\n" + "-" * 80)
        print(f"MySQL 总计: {len(mysql_databases)} 个数据库, {mysql_total_tables} 张表, {mysql_total_rows:,} 行数据")
    
    # PostgreSQL 统计
    print("\n" + "=" * 80)
    print("📊 PostgreSQL 数据库统计")
    print("=" * 80)
    
    postgres_databases = get_postgres_databases()
    if not postgres_databases:
        print("⚠ PostgreSQL 数据库连接失败或无数据库")
    else:
        
        for db in postgres_databases:
            print(f"\n📁 数据库: {db}")
            print("-" * 80)
            tables = get_postgres_tables(db)
            
            if not tables:
                print("  (无表)")
            else:
                print(f"  表数量: {len(tables)}")
                print(f"\n  {'表名':<40} {'行数':<15} {'字段数':<10}")
                print(f"  {'-'*40} {'-'*15} {'-'*10}")
                
                for table_name, info in sorted(tables.items()):
                    if info['row_count'] >= 0:
                        print(f"  {table_name:<40} {info['row_count']:<15} {info['column_count']:<10}")
                        postgres_total_rows += info['row_count']
                    else:
                        print(f"  {table_name:<40} {'错误':<15} {'错误':<10}")
                
                postgres_total_tables += len(tables)
        
        print("\n" + "-" * 80)
        print(f"PostgreSQL 总计: {len(postgres_databases)} 个数据库, {postgres_total_tables} 张表, {postgres_total_rows:,} 行数据")
    
    # 汇总
    print("\n" + "=" * 80)
    print("📈 汇总统计")
    print("=" * 80)
    mysql_db_count = len(mysql_databases) if mysql_databases else 0
    postgres_db_count = len(postgres_databases) if postgres_databases else 0
    
    print(f"MySQL:      {mysql_db_count} 个数据库, {mysql_total_tables} 张表")
    print(f"PostgreSQL: {postgres_db_count} 个数据库, {postgres_total_tables} 张表")
    print(f"总计:       {mysql_db_count + postgres_db_count} 个数据库, {mysql_total_tables + postgres_total_tables} 张表")
    print("=" * 80)

if __name__ == "__main__":
    try:
        print_statistics()
    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
