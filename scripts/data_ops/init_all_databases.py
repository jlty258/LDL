#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整初始化MySQL和PostgreSQL数据库
包括表结构创建和测试数据生成
"""

import mysql.connector
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import sys
import random
from datetime import datetime, timedelta

# 数据库配置
MYSQL_CONFIG = {
    'host': 'mysql-db',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'charset': 'utf8mb4'
}

POSTGRES_CONFIG = {
    'host': 'postgres-db',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres123',
    'database': 'sqlExpert'
}

# SQL文件路径
SQL_DIR = '/workspace/datawarehouse/sql'
if not os.path.exists(SQL_DIR):
    SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'datawarehouse', 'sql')

def convert_mysql_to_postgres_sql(mysql_sql):
    """将MySQL SQL转换为PostgreSQL兼容的SQL"""
    sql = mysql_sql
    sql = sql.replace('USE sqlExpert;', '')
    # 移除COMMENT（PostgreSQL需要单独执行COMMENT命令）
    import re
    sql = re.sub(r"COMMENT\s+'[^']*'", '', sql)
    sql = sql.replace('DATETIME', 'TIMESTAMP')
    sql = sql.replace('ON UPDATE CURRENT_TIMESTAMP', '')
    sql = sql.replace('AUTO_INCREMENT', 'SERIAL')
    sql = sql.replace('ENGINE=InnoDB', '')
    sql = sql.replace('DEFAULT CHARSET=utf8mb4', '')
    sql = sql.replace('CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci', '')
    # 修复VARCHAR长度语法
    sql = re.sub(r'VARCHAR\((\d+)\)', r'VARCHAR(\1)', sql)
    return sql

def execute_sql_file_mysql(conn, sql_file_path):
    """在MySQL中执行SQL文件"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS sqlExpert CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("USE sqlExpert")
        
        # 按分号分割，但需要处理多行语句
        # 先移除注释行
        lines = []
        for line in sql_content.split('\n'):
            stripped = line.strip()
            if stripped and not stripped.startswith('--'):
                # 移除行内注释
                if '--' in stripped:
                    stripped = stripped.split('--')[0].strip()
                if stripped:
                    lines.append(stripped)
        
        # 重新组合成完整语句
        full_sql = ' '.join(lines)
        # 按分号分割
        statements = [s.strip() for s in full_sql.split(';') if s.strip()]
        
        for statement in statements:
            if statement and not statement.upper().startswith('USE '):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    error_msg = str(e)
                    if 'already exists' not in error_msg.lower() and 'duplicate' not in error_msg.lower():
                        # 只显示重要错误
                        if 'syntax' not in error_msg.lower():
                            print(f"  ⚠ MySQL警告: {error_msg[:80]}")
        
        conn.commit()
        cursor.close()
        print(f"✓ MySQL: {os.path.basename(sql_file_path)} 执行完成")
        return True
    except Exception as e:
        print(f"❌ MySQL错误: {e}")
        import traceback
        traceback.print_exc()
        return False

def execute_sql_file_postgres(conn, sql_file_path):
    """在PostgreSQL中执行SQL文件"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        cursor = conn.cursor()
        
        # 移除注释行
        lines = []
        for line in sql_content.split('\n'):
            stripped = line.strip()
            if stripped and not stripped.startswith('--'):
                # 移除行内注释
                if '--' in stripped:
                    stripped = stripped.split('--')[0].strip()
                if stripped:
                    lines.append(stripped)
        
        # 重新组合
        full_sql = ' '.join(lines)
        
        # 分离CREATE TABLE和CREATE INDEX语句
        create_table_statements = []
        create_index_statements = []
        
        # 按分号分割
        parts = [p.strip() for p in full_sql.split(';') if p.strip()]
        
        for part in parts:
            part_upper = part.upper()
            # 转换MySQL语法到PostgreSQL
            pg_part = convert_mysql_to_postgres_sql(part)
            
            if 'CREATE TABLE' in part_upper or 'CREATE TABLE IF NOT EXISTS' in part_upper:
                # 移除INDEX定义（PostgreSQL需要在CREATE TABLE后单独执行）
                import re
                # 移除表定义中的INDEX
                pg_part = re.sub(r',\s*INDEX\s+\w+\s*\([^)]+\)', '', pg_part, flags=re.IGNORECASE)
                create_table_statements.append(pg_part)
            elif 'CREATE INDEX' in part_upper or 'CREATE UNIQUE INDEX' in part_upper:
                pg_part = convert_mysql_to_postgres_sql(part)
                create_index_statements.append(pg_part)
        
        # 先执行CREATE TABLE
        for statement in create_table_statements:
            if statement:
                try:
                    cursor.execute(statement)
                except Exception as e:
                    error_msg = str(e)
                    if 'already exists' not in error_msg.lower():
                        print(f"  ⚠ PostgreSQL警告: {error_msg[:80]}")
        
        # 再执行CREATE INDEX
        for statement in create_index_statements:
            if statement:
                try:
                    cursor.execute(statement)
                except Exception as e:
                    error_msg = str(e)
                    if 'already exists' not in error_msg.lower():
                        print(f"  ⚠ PostgreSQL索引警告: {error_msg[:80]}")
        
        conn.commit()
        cursor.close()
        print(f"✓ PostgreSQL: {os.path.basename(sql_file_path)} 执行完成")
        return True
    except Exception as e:
        print(f"❌ PostgreSQL错误: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_tables():
    """创建所有表结构"""
    print("\n" + "=" * 60)
    print("步骤1: 创建表结构")
    print("=" * 60)
    
    sql_files = [
        '01_init_database.sql',
        '02_ods_tables.sql',
        '03_dwd_tables.sql',
        '04_dws_tables.sql',
        '05_ads_tables.sql'
    ]
    
    mysql_conn = None
    postgres_conn = None
    
    try:
        # 连接MySQL
        print("\n连接MySQL...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("✓ MySQL连接成功")
        
        # 连接PostgreSQL
        print("\n连接PostgreSQL...")
        pg_config_temp = POSTGRES_CONFIG.copy()
        pg_config_temp['database'] = 'postgres'
        postgres_conn = psycopg2.connect(**pg_config_temp)
        postgres_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        # 创建数据库
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'sqlExpert'")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE sqlExpert")
            print("✓ PostgreSQL数据库创建成功")
        cursor.close()
        postgres_conn.close()
        
        # 连接到sqlExpert数据库
        postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("✓ PostgreSQL连接成功")
        
        # 执行SQL文件
        for sql_file in sql_files:
            sql_path = os.path.join(SQL_DIR, sql_file)
            if os.path.exists(sql_path):
                print(f"\n执行 {sql_file}...")
                execute_sql_file_mysql(mysql_conn, sql_path)
                execute_sql_file_postgres(postgres_conn, sql_path)
            else:
                print(f"⚠ 文件不存在: {sql_path}")
        
        # 验证表数量
        print("\n验证表结构...")
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute("USE sqlExpert")
        mysql_cursor.execute("SHOW TABLES")
        mysql_tables = mysql_cursor.fetchall()
        print(f"✓ MySQL: 共创建 {len(mysql_tables)} 张表")
        
        postgres_cursor = postgres_conn.cursor()
        postgres_cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        postgres_tables = postgres_cursor.fetchall()
        print(f"✓ PostgreSQL: 共创建 {len(postgres_tables)} 张表")
        
        mysql_cursor.close()
        postgres_cursor.close()
        
    except Exception as e:
        print(f"❌ 错误: {e}")
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()

def generate_id(prefix, num):
    """生成ID"""
    return f"{prefix}{num:08d}"

def generate_code(prefix, num):
    """生成编码"""
    return f"{prefix}{num:06d}"

def random_date(start_date, end_date):
    """生成随机日期"""
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    return start_date + timedelta(days=random_days)

def random_datetime(start_date, end_date):
    """生成随机日期时间"""
    time_between = end_date - start_date
    seconds_between = time_between.total_seconds()
    random_seconds = random.randrange(int(seconds_between))
    return start_date + timedelta(seconds=random_seconds)

def generate_test_data_mysql(conn, max_rows=10000):
    """在MySQL中生成测试数据"""
    cursor = conn.cursor()
    cursor.execute("USE sqlExpert")
    
    try:
        regions = ['华东', '华南', '华北', '华中', '西南', '西北', '东北']
        cities = ['上海', '北京', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']
        
        print("生成基础数据...")
        
        # 生成客户数据（200条）
        customers = []
        for i in range(1, 201):
            customer_id = generate_id('C', i)
            customers.append(customer_id)
            cursor.execute("""
                INSERT IGNORE INTO ods_customer_master 
                (customer_id, customer_code, customer_name, customer_type, industry, region, city, 
                 contact_person, contact_phone, credit_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                customer_id, generate_code('CUST', i), f'客户公司{i}',
                random.choice(['企业', '经销商', '代理商']),
                random.choice(['汽车', '电子', '机械', '化工', '纺织']),
                random.choice(regions), random.choice(cities),
                f'联系人{i}', f'138{random.randint(10000000, 99999999)}',
                random.choice(['A', 'B', 'C', 'D'])
            ))
        
        # 生成产品数据（300条）
        products = []
        for i in range(1, 301):
            product_id = generate_id('P', i)
            products.append(product_id)
            cursor.execute("""
                INSERT IGNORE INTO ods_product_master 
                (product_id, product_code, product_name, product_category, product_type, brand,
                 standard_price, cost_price, weight, volume, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                product_id, generate_code('PROD', i), f'产品{i}',
                random.choice(['成品', '半成品', '零部件']),
                random.choice(['标准', '定制', '特殊']),
                random.choice(['品牌A', '品牌B', '品牌C', '品牌D']),
                random.uniform(100, 10000), random.uniform(50, 5000),
                random.uniform(0.1, 100), random.uniform(0.01, 10), '正常'
            ))
        
        # 生成物料数据（最大表，10k行）
        print(f"生成物料数据（{max_rows}行）...")
        materials = []
        material_categories = ['原材料', '辅料', '包装材料', '备件', '工具']
        suppliers = [generate_id('S', i) for i in range(1, 101)]
        
        for i in range(1, max_rows + 1):
            material_id = generate_id('M', i)
            materials.append(material_id)
            cursor.execute("""
                INSERT IGNORE INTO ods_material_master 
                (material_id, material_code, material_name, material_category, material_type,
                 standard_price, cost_price, supplier_id, lead_time, min_stock, max_stock, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                material_id, generate_code('MAT', i), f'物料{i}',
                random.choice(material_categories),
                random.choice(['标准', '定制']),
                random.uniform(1, 1000), random.uniform(0.5, 500),
                random.choice(suppliers) if suppliers else None,
                random.randint(1, 30),
                random.uniform(100, 1000), random.uniform(1000, 10000), '正常'
            ))
            if i % 1000 == 0:
                print(f"  已生成 {i} 条物料数据...")
                conn.commit()
        
        # 生成订单数据（5000条）
        print("生成订单数据...")
        orders = []
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 5001):
            order_id = generate_id('O', i)
            orders.append(order_id)
            order_date = random_datetime(start_date, end_date)
            cursor.execute("""
                INSERT IGNORE INTO ods_order_master 
                (order_id, order_no, customer_id, order_date, order_status, total_amount, currency, 
                 sales_rep_id, warehouse_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_id, f'ORD{order_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(customers), order_date,
                random.choice(['待确认', '已确认', '生产中', '已完成', '已取消']),
                random.uniform(1000, 100000), 'CNY',
                generate_id('E', random.randint(1, 500)),
                generate_id('W', random.randint(1, 10))
            ))
            if i % 1000 == 0:
                conn.commit()
        
        conn.commit()
        print(f"\n✓ MySQL测试数据生成完成！")
        print(f"  - 物料表(ods_material_master): {max_rows}行")
        print(f"  - 客户表: 200行")
        print(f"  - 产品表: 300行")
        print(f"  - 订单表: 5000行")
        
    except Exception as e:
        print(f"❌ MySQL测试数据生成错误: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()

def generate_test_data_postgres(conn, max_rows=10000):
    """在PostgreSQL中生成测试数据"""
    cursor = conn.cursor()
    
    try:
        regions = ['华东', '华南', '华北', '华中', '西南', '西北', '东北']
        cities = ['上海', '北京', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']
        
        print("生成基础数据...")
        
        # 生成客户数据
        customers = []
        for i in range(1, 201):
            customer_id = generate_id('C', i)
            customers.append(customer_id)
            cursor.execute("""
                INSERT INTO ods_customer_master 
                (customer_id, customer_code, customer_name, customer_type, industry, region, city, 
                 contact_person, contact_phone, credit_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING
            """, (
                customer_id, generate_code('CUST', i), f'客户公司{i}',
                random.choice(['企业', '经销商', '代理商']),
                random.choice(['汽车', '电子', '机械', '化工', '纺织']),
                random.choice(regions), random.choice(cities),
                f'联系人{i}', f'138{random.randint(10000000, 99999999)}',
                random.choice(['A', 'B', 'C', 'D'])
            ))
        
        # 生成产品数据
        products = []
        for i in range(1, 301):
            product_id = generate_id('P', i)
            products.append(product_id)
            cursor.execute("""
                INSERT INTO ods_product_master 
                (product_id, product_code, product_name, product_category, product_type, brand,
                 standard_price, cost_price, weight, volume, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
            """, (
                product_id, generate_code('PROD', i), f'产品{i}',
                random.choice(['成品', '半成品', '零部件']),
                random.choice(['标准', '定制', '特殊']),
                random.choice(['品牌A', '品牌B', '品牌C', '品牌D']),
                random.uniform(100, 10000), random.uniform(50, 5000),
                random.uniform(0.1, 100), random.uniform(0.01, 10), '正常'
            ))
        
        # 生成物料数据（最大表，10k行）
        print(f"生成物料数据（{max_rows}行）...")
        materials = []
        material_categories = ['原材料', '辅料', '包装材料', '备件', '工具']
        suppliers = [generate_id('S', i) for i in range(1, 101)]
        
        for i in range(1, max_rows + 1):
            material_id = generate_id('M', i)
            materials.append(material_id)
            cursor.execute("""
                INSERT INTO ods_material_master 
                (material_id, material_code, material_name, material_category, material_type,
                 standard_price, cost_price, supplier_id, lead_time, min_stock, max_stock, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (material_id) DO NOTHING
            """, (
                material_id, generate_code('MAT', i), f'物料{i}',
                random.choice(material_categories),
                random.choice(['标准', '定制']),
                random.uniform(1, 1000), random.uniform(0.5, 500),
                random.choice(suppliers) if suppliers else None,
                random.randint(1, 30),
                random.uniform(100, 1000), random.uniform(1000, 10000), '正常'
            ))
            if i % 1000 == 0:
                print(f"  已生成 {i} 条物料数据...")
                conn.commit()
        
        # 生成订单数据
        print("生成订单数据...")
        orders = []
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 5001):
            order_id = generate_id('O', i)
            orders.append(order_id)
            order_date = random_datetime(start_date, end_date)
            cursor.execute("""
                INSERT INTO ods_order_master 
                (order_id, order_no, customer_id, order_date, order_status, total_amount, currency, 
                 sales_rep_id, warehouse_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                order_id, f'ORD{order_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(customers), order_date,
                random.choice(['待确认', '已确认', '生产中', '已完成', '已取消']),
                random.uniform(1000, 100000), 'CNY',
                generate_id('E', random.randint(1, 500)),
                generate_id('W', random.randint(1, 10))
            ))
            if i % 1000 == 0:
                conn.commit()
        
        conn.commit()
        print(f"\n✓ PostgreSQL测试数据生成完成！")
        print(f"  - 物料表(ods_material_master): {max_rows}行")
        print(f"  - 客户表: 200行")
        print(f"  - 产品表: 300行")
        print(f"  - 订单表: 5000行")
        
    except Exception as e:
        print(f"❌ PostgreSQL测试数据生成错误: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()

def generate_test_data():
    """生成测试数据（最多10k行）"""
    print("\n" + "=" * 60)
    print("步骤2: 生成测试数据（最大表10k行）")
    print("=" * 60)
    
    mysql_conn = None
    postgres_conn = None
    
    try:
        # 连接MySQL
        print("\n连接MySQL...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("✓ MySQL连接成功")
        
        # 连接PostgreSQL
        print("\n连接PostgreSQL...")
        postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("✓ PostgreSQL连接成功")
        
        # 生成测试数据
        max_rows = 10000
        generate_test_data_mysql(mysql_conn, max_rows)
        generate_test_data_postgres(postgres_conn, max_rows)
        
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()

def main():
    """主函数"""
    print("=" * 60)
    print("MySQL和PostgreSQL数据库完整初始化")
    print("=" * 60)
    print("功能:")
    print("  1. 在MySQL和PostgreSQL中创建所有表")
    print("  2. 生成测试数据（最大表10k行）")
    print("=" * 60)
    
    # 步骤1: 创建表
    create_tables()
    
    # 步骤2: 生成测试数据
    generate_test_data()
    
    print("\n" + "=" * 60)
    print("初始化完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()
