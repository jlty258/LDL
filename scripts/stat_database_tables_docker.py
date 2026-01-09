#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用 Docker 容器内命令统计 MySQL 和 PostgreSQL 数据库中的库表信息
"""
import subprocess
import sys
import re

def run_docker_cmd(container, cmd):
    """在 Docker 容器内执行命令"""
    full_cmd = f"docker exec {container} {cmd}"
    try:
        result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        return result.stdout.strip(), result.stderr.strip(), result.returncode == 0
    except Exception as e:
        return "", str(e), False

def get_mysql_stats():
    """获取 MySQL 统计信息"""
    print("\n" + "=" * 80)
    print("📊 MySQL 数据库统计")
    print("=" * 80)
    
    # 获取数据库列表
    stdout, stderr, success = run_docker_cmd("mysql-db", 
        "mysql -usqluser -psqlpass123 -e \"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys') ORDER BY SCHEMA_NAME;\"")
    
    if not success:
        print(f"⚠ MySQL 连接失败: {stderr}")
        return 0, 0
    
    # 解析数据库列表
    databases = []
    for line in stdout.split('\n'):
        if line.strip() and not line.startswith('SCHEMA_NAME') and not line.startswith('-'):
            db = line.strip()
            if db:
                databases.append(db)
    
    if not databases:
        print("⚠ 未找到数据库")
        return 0, 0
    
    total_tables = 0
    total_rows = 0
    
    for db in databases:
        print(f"\n📁 数据库: {db}")
        print("-" * 80)
        
        # 获取表列表
        stdout, stderr, success = run_docker_cmd("mysql-db",
            f"mysql -usqluser -psqlpass123 {db} -e \"SHOW TABLES;\"")
        
        if not success:
            print(f"  ⚠ 获取表列表失败: {stderr}")
            continue
        
        # 解析表列表
        tables = []
        for line in stdout.split('\n'):
            if line.strip() and not line.startswith('Tables_in_') and not line.startswith('-'):
                table = line.strip()
                if table:
                    tables.append(table)
        
        if not tables:
            print("  (无表)")
            continue
        
        print(f"  表数量: {len(tables)}")
        print(f"\n  {'表名':<45} {'行数':<15} {'字段数':<10}")
        print(f"  {'-'*45} {'-'*15} {'-'*10}")
        
        for table in sorted(tables):
            # 获取行数
            stdout, _, _ = run_docker_cmd("mysql-db",
                f"mysql -usqluser -psqlpass123 {db} -e \"SELECT COUNT(*) as cnt FROM `{table}`;\"")
            
            row_count = 0
            for line in stdout.split('\n'):
                if line.strip() and not line.startswith('cnt') and not line.startswith('-'):
                    try:
                        row_count = int(line.strip())
                        break
                    except:
                        pass
            
            # 获取字段数
            stdout, _, _ = run_docker_cmd("mysql-db",
                f"mysql -usqluser -psqlpass123 {db} -e \"SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{table}';\"")
            
            col_count = 0
            for line in stdout.split('\n'):
                if line.strip() and not line.startswith('cnt') and not line.startswith('-'):
                    try:
                        col_count = int(line.strip())
                        break
                    except:
                        pass
            
            print(f"  {table:<45} {row_count:<15} {col_count:<10}")
            total_rows += row_count
        
        total_tables += len(tables)
    
    print("\n" + "-" * 80)
    print(f"MySQL 总计: {len(databases)} 个数据库, {total_tables} 张表, {total_rows:,} 行数据")
    
    return len(databases), total_tables

def get_postgres_stats():
    """获取 PostgreSQL 统计信息"""
    print("\n" + "=" * 80)
    print("📊 PostgreSQL 数据库统计")
    print("=" * 80)
    
    # 获取数据库列表
    stdout, stderr, success = run_docker_cmd("postgres-db",
        "psql -U postgres -c \"SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1') ORDER BY datname;\"")
    
    if not success:
        print(f"⚠ PostgreSQL 连接失败: {stderr}")
        return 0, 0
    
    # 解析数据库列表
    databases = []
    for line in stdout.split('\n'):
        line = line.strip()
        if line and not line.startswith('datname') and not line.startswith('-') and not line.startswith('('):
            parts = line.split('|')
            if len(parts) > 0:
                db = parts[0].strip()
                if db:
                    databases.append(db)
    
    if not databases:
        print("⚠ 未找到数据库")
        return 0, 0
    
    total_tables = 0
    total_rows = 0
    
    for db in databases:
        print(f"\n📁 数据库: {db}")
        print("-" * 80)
        
        # 获取表列表
        stdout, stderr, success = run_docker_cmd("postgres-db",
            f"psql -U postgres -d {db} -c \"SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;\"")
        
        if not success:
            print(f"  ⚠ 获取表列表失败: {stderr}")
            continue
        
        # 解析表列表
        tables = []
        for line in stdout.split('\n'):
            line = line.strip()
            if line and not line.startswith('tablename') and not line.startswith('-') and not line.startswith('('):
                parts = line.split('|')
                if len(parts) > 0:
                    table = parts[0].strip()
                    if table:
                        tables.append(table)
        
        if not tables:
            print("  (无表)")
            continue
        
        print(f"  表数量: {len(tables)}")
        print(f"\n  {'表名':<45} {'行数':<15} {'字段数':<10}")
        print(f"  {'-'*45} {'-'*15} {'-'*10}")
        
        for table in sorted(tables):
            # 获取行数
            stdout, _, _ = run_docker_cmd("postgres-db",
                f"psql -U postgres -d {db} -t -c \"SELECT COUNT(*) FROM \\\"{table}\\\";\"")
            
            row_count = 0
            try:
                row_count = int(stdout.strip())
            except:
                pass
            
            # 获取字段数
            stdout, _, _ = run_docker_cmd("postgres-db",
                f"psql -U postgres -d {db} -t -c \"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}';\"")
            
            col_count = 0
            try:
                col_count = int(stdout.strip())
            except:
                pass
            
            print(f"  {table:<45} {row_count:<15} {col_count:<10}")
            total_rows += row_count
        
        total_tables += len(tables)
    
    print("\n" + "-" * 80)
    print(f"PostgreSQL 总计: {len(databases)} 个数据库, {total_tables} 张表, {total_rows:,} 行数据")
    
    return len(databases), total_tables

def main():
    print("=" * 80)
    print("数据库库表统计报告")
    print("=" * 80)
    
    mysql_db_count, mysql_table_count = get_mysql_stats()
    postgres_db_count, postgres_table_count = get_postgres_stats()
    
    # 汇总
    print("\n" + "=" * 80)
    print("📈 汇总统计")
    print("=" * 80)
    print(f"MySQL:      {mysql_db_count} 个数据库, {mysql_table_count} 张表")
    print(f"PostgreSQL: {postgres_db_count} 个数据库, {postgres_table_count} 张表")
    print(f"总计:       {mysql_db_count + postgres_db_count} 个数据库, {mysql_table_count + postgres_table_count} 张表")
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
