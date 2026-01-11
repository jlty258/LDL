#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化数据库表结构 - 支持 MySQL 和 PostgreSQL
"""
import os
import sys

# 尝试导入数据库驱动
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    print("⚠ MySQL驱动未安装，将跳过MySQL初始化")

try:
    import psycopg2
    from psycopg2 import Error as PostgreSQLError
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    print("⚠ PostgreSQL驱动未安装，将跳过PostgreSQL初始化")

# 数据库配置
DB_CONFIG = {
    'mysql': {
        'host': 'localhost',
        'port': 3306,
        'user': 'sqluser',
        'password': 'sqlpass123',
        'charset': 'utf8mb4'
    },
    'postgresql': {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres123'
    }
}

def execute_sql_file_mysql(conn, sql_file_path):
    """执行SQL文件 - MySQL"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割SQL语句
        statements = []
        current_statement = ""
        for line in sql_content.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            current_statement += line + " "
            if line.endswith(';'):
                statements.append(current_statement.strip())
                current_statement = ""
        
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        cursor = conn.cursor()
        executed = 0
        for statement in statements:
            if statement and statement != ';':
                try:
                    cursor.execute(statement)
                    executed += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"  ⚠ 警告: {str(e)[:100]}")
        
        conn.commit()
        cursor.close()
        return True, executed
    except Exception as e:
        print(f"  ❌ 错误: {e}")
        return False, 0

def execute_sql_file_postgresql(conn, sql_file_path):
    """执行SQL文件 - PostgreSQL"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # PostgreSQL需要移除USE语句
        sql_content = sql_content.replace('USE sqlExpert;', '')
        sql_content = sql_content.replace('USE sqlExpert', '')
        
        # 分割SQL语句
        statements = []
        current_statement = ""
        for line in sql_content.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            # MySQL的注释语法需要转换
            if 'COMMENT' in line.upper():
                # PostgreSQL使用COMMENT ON语法，这里简化处理
                line = line.split('COMMENT')[0].strip()
                if line.endswith(','):
                    line = line[:-1]
            current_statement += line + " "
            if line.endswith(';'):
                stmt = current_statement.strip()
                if stmt and stmt != ';':
                    statements.append(stmt)
                current_statement = ""
        
        if current_statement.strip():
            stmt = current_statement.strip()
            if stmt and stmt != ';':
                statements.append(stmt)
        
        cursor = conn.cursor()
        executed = 0
        for statement in statements:
            if statement:
                try:
                    cursor.execute(statement)
                    executed += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"  ⚠ 警告: {str(e)[:100]}")
        conn.commit()
        cursor.close()
        return True, executed
    except Exception as e:
        print(f"  ❌ 错误: {e}")
        return False, 0

def init_mysql():
    """初始化MySQL"""
    if not MYSQL_AVAILABLE:
        return False
    
    print("\n" + "=" * 60)
    print("初始化 MySQL 数据库")
    print("=" * 60)
    
    try:
        # 连接数据库
        print("\n1. 连接数据库...")
        config = DB_CONFIG['mysql'].copy()
        conn = mysql.connector.connect(**config)
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
            '01_init_database.sql',
            '02_ods_tables.sql',
            '03_dwd_tables.sql',
            '04_dws_tables.sql',
            '05_ads_tables.sql'
        ]
        
        print("\n3. 创建表结构...")
        total_executed = 0
        for sql_file in sql_files:
            sql_path = os.path.join(base_dir, 'sql', sql_file)
            if os.path.exists(sql_path):
                print(f"\n执行 {sql_file}...")
                success, count = execute_sql_file_mysql(conn, sql_path)
                if success:
                    print(f"  ✓ 执行完成 ({count} 条语句)")
                    total_executed += count
                else:
                    print(f"  ⚠ 部分语句执行失败")
            else:
                print(f"⚠ 文件不存在: {sql_path}")
        
        # 检查表数量
        print("\n4. 验证表结构...")
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        print(f"✓ 共创建 {len(tables)} 张表")
        
        # 统计各层表数量
        ods_count = sum(1 for t in table_names if t.startswith('ods_'))
        dwd_count = sum(1 for t in table_names if t.startswith('dwd_'))
        dws_count = sum(1 for t in table_names if t.startswith('dws_'))
        ads_count = sum(1 for t in table_names if t.startswith('ads_'))
        
        print(f"  - ODS层: {ods_count} 张表")
        print(f"  - DWD层: {dwd_count} 张表")
        print(f"  - DWS层: {dws_count} 张表")
        print(f"  - ADS层: {ads_count} 张表")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("MySQL 数据库初始化完成！")
        print("=" * 60)
        return True
        
    except MySQLError as e:
        print(f"❌ MySQL数据库错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 错误: {e}")
        return False

def init_postgresql():
    """初始化PostgreSQL"""
    if not POSTGRESQL_AVAILABLE:
        return False
    
    print("\n" + "=" * 60)
    print("初始化 PostgreSQL 数据库")
    print("=" * 60)
    
    try:
        # 连接数据库
        print("\n1. 连接数据库...")
        config = DB_CONFIG['postgresql'].copy()
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        print("✓ 数据库连接成功")
        
        # 创建数据库
        print("\n2. 创建数据库...")
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'sqlExpert'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute("CREATE DATABASE sqlExpert")
            print("✓ 数据库创建成功")
        else:
            print("✓ 数据库已存在")
        cursor.close()
        conn.close()
        
        # 重新连接到新数据库
        config['database'] = 'sqlExpert'
        conn = psycopg2.connect(**config)
        
        # 执行SQL文件
        base_dir = os.path.dirname(os.path.dirname(__file__))
        sql_files = [
            '02_ods_tables.sql',
            '03_dwd_tables.sql',
            '04_dws_tables.sql',
            '05_ads_tables.sql'
        ]
        
        print("\n3. 创建表结构...")
        print("⚠ 注意: PostgreSQL需要手动转换MySQL语法，部分功能可能不完整")
        total_executed = 0
        for sql_file in sql_files:
            sql_path = os.path.join(base_dir, 'sql', sql_file)
            if os.path.exists(sql_path):
                print(f"\n执行 {sql_file}...")
                success, count = execute_sql_file_postgresql(conn, sql_path)
                if success:
                    print(f"  ✓ 执行完成 ({count} 条语句)")
                    total_executed += count
                else:
                    print(f"  ⚠ 部分语句执行失败")
            else:
                print(f"⚠ 文件不存在: {sql_path}")
        
        # 检查表数量
        print("\n4. 验证表结构...")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        print(f"✓ 共创建 {len(tables)} 张表")
        
        # 统计各层表数量
        ods_count = sum(1 for t in table_names if t.startswith('ods_'))
        dwd_count = sum(1 for t in table_names if t.startswith('dwd_'))
        dws_count = sum(1 for t in table_names if t.startswith('dws_'))
        ads_count = sum(1 for t in table_names if t.startswith('ads_'))
        
        print(f"  - ODS层: {ods_count} 张表")
        print(f"  - DWD层: {dwd_count} 张表")
        print(f"  - DWS层: {dws_count} 张表")
        print(f"  - ADS层: {ads_count} 张表")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("PostgreSQL 数据库初始化完成！")
        print("=" * 60)
        return True
        
    except PostgreSQLError as e:
        print(f"❌ PostgreSQL数据库错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 错误: {e}")
        return False

def main():
    print("=" * 60)
    print("初始化制造业数仓数据库 - MySQL & PostgreSQL")
    print("=" * 60)
    
    mysql_ok = init_mysql()
    postgresql_ok = init_postgresql()
    
    print("\n" + "=" * 60)
    print("初始化总结")
    print("=" * 60)
    print(f"MySQL: {'✓ 成功' if mysql_ok else '❌ 失败'}")
    print(f"PostgreSQL: {'✓ 成功' if postgresql_ok else '❌ 失败'}")
    print("=" * 60)

if __name__ == "__main__":
    main()
