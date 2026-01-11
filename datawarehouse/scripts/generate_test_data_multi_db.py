#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
制造业数仓测试数据生成脚本 - 支持 MySQL 和 PostgreSQL
最大表生成10000行数据
"""

import sys
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# 尝试导入数据库驱动
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    print("⚠ MySQL驱动未安装，将跳过MySQL数据生成")

try:
    import psycopg2
    from psycopg2 import Error as PostgreSQLError
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    print("⚠ PostgreSQL驱动未安装，将跳过PostgreSQL数据生成")

# 数据库配置
DB_CONFIG = {
    'mysql': {
        'host': 'mysql-db',
        'port': 3306,
        'user': 'sqluser',
        'password': 'sqlpass123',
        'database': 'sqlExpert',
        'charset': 'utf8mb4'
    },
    'postgresql': {
        'host': 'postgres-db',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres123',
        'database': 'sqlExpert'
    }
}


def generate_id(prefix: str, num: int) -> str:
    """生成ID"""
    return f"{prefix}{num:08d}"


def generate_code(prefix: str, num: int) -> str:
    """生成编码"""
    return f"{prefix}{num:06d}"


def random_date(start_date: datetime, end_date: datetime) -> datetime:
    """生成随机日期"""
    time_between = end_date - start_date
    days_between = time_between.days
    if days_between <= 0:
        return start_date
    random_days = random.randrange(days_between)
    return start_date + timedelta(days=random_days)


def random_datetime(start_date: datetime, end_date: datetime) -> datetime:
    """生成随机日期时间"""
    time_between = end_date - start_date
    seconds_between = int(time_between.total_seconds())
    if seconds_between <= 0:
        return start_date
    random_seconds = random.randrange(seconds_between)
    return start_date + timedelta(seconds=random_seconds)


class DataGenerator:
    """数据生成器基类"""
    
    def __init__(self, conn, db_type: str):
        self.conn = conn
        self.db_type = db_type
        self.cursor = conn.cursor()
        
        # 基础数据
        self.regions = ['华东', '华南', '华北', '华中', '西南', '西北', '东北']
        self.cities = ['上海', '北京', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']
        self.statuses = ['正常', '暂停', '关闭']
        self.order_statuses = ['待确认', '已确认', '生产中', '已完成', '已取消']
        self.production_statuses = ['计划中', '生产中', '已完成', '已暂停', '已取消']
        
        # 存储生成的数据ID，供后续关联使用
        self.factories: List[str] = []
        self.departments: List[str] = []
        self.workshops: List[str] = []
        self.production_lines: List[str] = []
        self.customers: List[str] = []
        self.suppliers: List[str] = []
        self.products: List[str] = []
        self.materials: List[str] = []
        self.employees: List[str] = []
        self.warehouses: List[str] = []
        self.orders: List[str] = []
        self.plans: List[str] = []
        self.work_orders: List[str] = []
        self.purchase_orders: List[str] = []
        self.inbound_orders: List[str] = []
        self.outbound_orders: List[str] = []
        self.equipments: List[str] = []
        self.cost_centers: List[str] = []
    
    def execute(self, sql: str, params: tuple = None):
        """执行SQL语句"""
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
        except Exception as e:
            # 忽略重复键错误
            if 'duplicate' in str(e).lower() or '1062' in str(e) or 'unique' in str(e).lower():
                pass  # 忽略重复数据错误
            else:
                raise
    
    def commit(self):
        """提交事务"""
        self.conn.commit()
    
    def clear_ods_tables(self):
        """清空所有ODS表"""
        print("\n清空现有ODS表数据...")
        ods_tables = [
            'ods_factory_master', 'ods_department_master', 'ods_workshop_master',
            'ods_production_line', 'ods_customer_master', 'ods_supplier_master',
            'ods_product_master', 'ods_material_master', 'ods_employee_master',
            'ods_warehouse_master', 'ods_order_master', 'ods_order_detail',
            'ods_production_plan', 'ods_production_order', 'ods_bom',
            'ods_inventory', 'ods_purchase_order', 'ods_purchase_detail',
            'ods_inbound_order', 'ods_inbound_detail', 'ods_outbound_order',
            'ods_outbound_detail', 'ods_equipment_master', 'ods_equipment_runtime',
            'ods_quality_inspection', 'ods_defect_record', 'ods_attendance',
            'ods_sales_payment', 'ods_cost_center', 'ods_cost_detail'
        ]
        
        for table in ods_tables:
            try:
                if self.db_type == 'mysql':
                    self.cursor.execute(f"DELETE FROM {table}")
                else:
                    self.cursor.execute(f'DELETE FROM "{table}"')
            except Exception as e:
                # 表不存在时忽略错误
                if 'doesn\'t exist' not in str(e) and 'does not exist' not in str(e):
                    print(f"  ⚠ 清空 {table} 时出错: {str(e)[:50]}")
        
        self.commit()
        print("✓ ODS表数据已清空")
    
    def generate_all(self):
        """生成所有测试数据"""
        print("\n" + "=" * 60)
        print(f"开始生成测试数据 ({self.db_type.upper()})")
        print("=" * 60)
        
        try:
            # 先清空现有数据
            self.clear_ods_tables()
            # 基础主数据
            self.generate_factories()
            self.generate_departments()
            self.generate_workshops()
            self.generate_production_lines()
            self.generate_customers()
            self.generate_suppliers()
            self.generate_products()
            self.generate_materials()  # 最大表，10000行
            self.generate_employees()
            self.generate_warehouses()
            
            # 业务数据
            self.generate_orders()
            self.generate_order_details()
            self.generate_production_plans()
            self.generate_production_orders()
            self.generate_bom()
            self.generate_inventory()
            self.generate_purchase_orders()
            self.generate_purchase_details()
            self.generate_inbound_orders()
            self.generate_inbound_details()
            self.generate_outbound_orders()
            self.generate_outbound_details()
            self.generate_equipments()
            self.generate_equipment_runtime()
            self.generate_quality_inspections()
            self.generate_defect_records()
            self.generate_attendance()
            self.generate_sales_payments()
            self.generate_cost_centers()
            self.generate_cost_details()
            
            self.commit()
            print("\n" + "=" * 60)
            print("✓ 所有测试数据生成完成！")
            print("=" * 60)
            self.print_statistics()
            
        except Exception as e:
            print(f"\n❌ 错误: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
            raise
    
    def generate_factories(self):
        """生成工厂数据"""
        print("\n1. 生成工厂数据...")
        for i in range(1, 6):
            factory_id = generate_id('F', i)
            self.factories.append(factory_id)
            self.execute("""
                INSERT INTO ods_factory_master 
                (factory_id, factory_code, factory_name, region, city, address, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                factory_id,
                generate_code('FAC', i),
                f'制造工厂{i}',
                random.choice(self.regions),
                random.choice(self.cities),
                f'{random.choice(self.cities)}市工业园区{i}号',
                '正常'
            ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.factories)} 条工厂数据")
    
    def generate_departments(self):
        """生成部门数据"""
        print("\n2. 生成部门数据...")
        dept_names = ['生产部', '质量部', '采购部', '销售部', '仓储部', '财务部', '人事部']
        for factory_id in self.factories:
            for dept_name in dept_names:
                dept_id = generate_id('DEPT', len(self.departments) + 1)
                self.departments.append(dept_id)
                self.execute("""
                    INSERT INTO ods_department_master 
                    (department_id, department_code, department_name, factory_id, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    dept_id,
                    generate_code('DEPT', len(self.departments)),
                    dept_name,
                    factory_id,
                    '正常'
                ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.departments)} 条部门数据")
    
    def generate_workshops(self):
        """生成车间数据"""
        print("\n3. 生成车间数据...")
        for factory_id in self.factories:
            for j in range(1, 4):
                workshop_id = generate_id('WS', len(self.workshops) + 1)
                self.workshops.append(workshop_id)
                self.execute("""
                    INSERT INTO ods_workshop_master 
                    (workshop_id, workshop_code, workshop_name, factory_id, workshop_type, capacity, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    workshop_id,
                    generate_code('WS', len(self.workshops)),
                    f'车间{len(self.workshops)}',
                    factory_id,
                    random.choice(['装配', '加工', '包装']),
                    round(random.uniform(1000, 10000), 2),
                    '正常'
                ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.workshops)} 条车间数据")
    
    def generate_production_lines(self):
        """生成生产线数据"""
        print("\n4. 生成生产线数据...")
        for workshop_id in self.workshops:
            for j in range(1, 3):
                line_id = generate_id('PL', len(self.production_lines) + 1)
                self.production_lines.append(line_id)
                self.execute("""
                    INSERT INTO ods_production_line 
                    (line_id, line_code, line_name, workshop_id, line_type, capacity, efficiency, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    line_id,
                    generate_code('PL', len(self.production_lines)),
                    f'生产线{len(self.production_lines)}',
                    workshop_id,
                    random.choice(['自动', '半自动', '手动']),
                    round(random.uniform(500, 5000), 2),
                    round(random.uniform(80, 98), 2),
                    '正常'
                ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.production_lines)} 条生产线数据")
    
    def generate_customers(self):
        """生成客户数据"""
        print("\n5. 生成客户数据...")
        for i in range(1, 201):
            customer_id = generate_id('C', i)
            self.customers.append(customer_id)
            self.execute("""
                INSERT INTO ods_customer_master 
                (customer_id, customer_code, customer_name, customer_type, industry, region, city, 
                 contact_person, contact_phone, credit_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                customer_id,
                generate_code('CUST', i),
                f'客户公司{i}',
                random.choice(['企业', '经销商', '代理商']),
                random.choice(['汽车', '电子', '机械', '化工', '纺织']),
                random.choice(self.regions),
                random.choice(self.cities),
                f'联系人{i}',
                f'138{random.randint(10000000, 99999999)}',
                random.choice(['A', 'B', 'C', 'D'])
            ))
            if i % 50 == 0:
                self.commit()
                print(f"  已生成 {i} 条客户数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.customers)} 条客户数据")
    
    def generate_suppliers(self):
        """生成供应商数据"""
        print("\n6. 生成供应商数据...")
        for i in range(1, 101):
            supplier_id = generate_id('S', i)
            self.suppliers.append(supplier_id)
            self.execute("""
                INSERT INTO ods_supplier_master 
                (supplier_id, supplier_code, supplier_name, supplier_type, region, city,
                 contact_person, contact_phone, credit_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                supplier_id,
                generate_code('SUP', i),
                f'供应商{i}',
                random.choice(['原材料', '零部件', '设备', '服务']),
                random.choice(self.regions),
                random.choice(self.cities),
                f'联系人{i}',
                f'139{random.randint(10000000, 99999999)}',
                random.choice(['A', 'B', 'C', 'D'])
            ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.suppliers)} 条供应商数据")
    
    def generate_products(self):
        """生成产品数据"""
        print("\n7. 生成产品数据...")
        categories = ['成品', '半成品', '零部件']
        brands = ['品牌A', '品牌B', '品牌C', '品牌D']
        for i in range(1, 301):
            product_id = generate_id('P', i)
            self.products.append(product_id)
            self.execute("""
                INSERT INTO ods_product_master 
                (product_id, product_code, product_name, product_category, product_type, brand,
                 standard_price, cost_price, weight, volume, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                product_id,
                generate_code('PROD', i),
                f'产品{i}',
                random.choice(categories),
                random.choice(['标准', '定制', '特殊']),
                random.choice(brands),
                round(random.uniform(100, 10000), 2),
                round(random.uniform(50, 5000), 2),
                round(random.uniform(0.1, 100), 3),
                round(random.uniform(0.01, 10), 3),
                '正常'
            ))
            if i % 100 == 0:
                self.commit()
                print(f"  已生成 {i} 条产品数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.products)} 条产品数据")
    
    def generate_materials(self):
        """生成物料数据（最大表，10000行）"""
        print("\n8. 生成物料数据（最大表，10000行）...")
        material_categories = ['原材料', '辅料', '包装材料', '备件', '工具']
        for i in range(1, 10001):
            material_id = generate_id('M', i)
            self.materials.append(material_id)
            supplier_id = random.choice(self.suppliers) if self.suppliers else None
            self.execute("""
                INSERT INTO ods_material_master 
                (material_id, material_code, material_name, material_category, material_type,
                 standard_price, cost_price, supplier_id, lead_time, min_stock, max_stock, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                material_id,
                generate_code('MAT', i),
                f'物料{i}',
                random.choice(material_categories),
                random.choice(['标准', '定制']),
                round(random.uniform(1, 1000), 2),
                round(random.uniform(0.5, 500), 2),
                supplier_id,
                random.randint(1, 30),
                round(random.uniform(100, 1000), 3),
                round(random.uniform(1000, 10000), 3),
                '正常'
            ))
            if i % 1000 == 0:
                self.commit()
                print(f"  已生成 {i} 条物料数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.materials)} 条物料数据（最大表）")
    
    def generate_employees(self):
        """生成员工数据"""
        print("\n9. 生成员工数据...")
        positions = ['工人', '技术员', '质检员', '班组长', '车间主任', '经理']
        for i in range(1, 501):
            employee_id = generate_id('E', i)
            self.employees.append(employee_id)
            dept_id = random.choice(self.departments) if self.departments else None
            workshop_id = random.choice(self.workshops) if self.workshops else None
            self.execute("""
                INSERT INTO ods_employee_master 
                (employee_id, employee_code, employee_name, department_id, position, workshop_id, hire_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                employee_id,
                generate_code('EMP', i),
                f'员工{i}',
                dept_id,
                random.choice(positions),
                workshop_id,
                random_date(datetime(2020, 1, 1), datetime(2024, 1, 1)),
                '正常'
            ))
            if i % 100 == 0:
                self.commit()
                print(f"  已生成 {i} 条员工数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.employees)} 条员工数据")
    
    def generate_warehouses(self):
        """生成仓库数据"""
        print("\n10. 生成仓库数据...")
        for factory_id in self.factories:
            for j in range(1, 3):
                warehouse_id = generate_id('W', len(self.warehouses) + 1)
                self.warehouses.append(warehouse_id)
                self.execute("""
                    INSERT INTO ods_warehouse_master 
                    (warehouse_id, warehouse_code, warehouse_name, warehouse_type, factory_id, capacity, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    warehouse_id,
                    generate_code('WH', len(self.warehouses)),
                    f'仓库{len(self.warehouses)}',
                    random.choice(['原料库', '成品库', '半成品库']),
                    factory_id,
                    round(random.uniform(10000, 100000), 2),
                    '正常'
                ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.warehouses)} 条仓库数据")
    
    def generate_orders(self):
        """生成订单数据"""
        print("\n11. 生成订单数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 5001):
            order_id = generate_id('O', i)
            self.orders.append(order_id)
            order_date = random_datetime(start_date, end_date)
            self.execute("""
                INSERT INTO ods_order_master 
                (order_id, order_no, customer_id, order_date, order_status, total_amount, currency, 
                 sales_rep_id, warehouse_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_id,
                f'ORD{order_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(self.customers),
                order_date,
                random.choice(self.order_statuses),
                round(random.uniform(1000, 100000), 2),
                'CNY',
                random.choice(self.employees) if self.employees else None,
                random.choice(self.warehouses)
            ))
            if i % 1000 == 0:
                self.commit()
                print(f"  已生成 {i} 条订单数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.orders)} 条订单数据")
    
    def generate_order_details(self):
        """生成订单明细数据（限制在10000行以内）"""
        print("\n12. 生成订单明细数据...")
        detail_count = 0
        max_details = 10000  # 限制最大10000行
        for order_id in self.orders:
            if detail_count >= max_details:
                break
            # 限制每个订单最多2条明细，确保不超过10000行
            detail_count_per_order = min(random.randint(1, 2), max_details - detail_count)
            for j in range(detail_count_per_order):
                detail_count += 1
                product = random.choice(self.products)
                quantity = round(random.uniform(1, 100), 3)
                unit_price = round(random.uniform(100, 1000), 2)
                amount = round(quantity * unit_price, 2)
                self.execute("""
                    INSERT INTO ods_order_detail 
                    (detail_id, order_id, product_id, product_code, product_name, quantity, unit_price, amount, unit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('OD', detail_count),
                    order_id,
                    product,
                    f'PROD{random.randint(1, 300):06d}',
                    f'产品{random.randint(1, 300)}',
                    quantity,
                    unit_price,
                    amount,
                    '件'
                ))
            if detail_count % 1000 == 0:
                self.commit()
                print(f"  已生成 {detail_count} 条订单明细数据...")
        self.commit()
        print(f"  ✓ 已生成约 {detail_count} 条订单明细数据")
    
    def generate_production_plans(self):
        """生成生产计划数据"""
        print("\n13. 生成生产计划数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 2001):
            plan_id = generate_id('PLAN', i)
            self.plans.append(plan_id)
            plan_date = random_date(start_date, end_date)
            self.execute("""
                INSERT INTO ods_production_plan 
                (plan_id, plan_no, product_id, plan_date, plan_quantity, actual_quantity, 
                 workshop_id, production_line_id, plan_status, start_time, end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                plan_id,
                f'PLAN{plan_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(self.products),
                plan_date,
                round(random.uniform(100, 10000), 3),
                round(random.uniform(80, 10000), 3),
                random.choice(self.workshops),
                random.choice(self.production_lines),
                random.choice(self.production_statuses),
                random_datetime(plan_date, plan_date + timedelta(days=1)),
                random_datetime(plan_date, plan_date + timedelta(days=7))
            ))
            if i % 500 == 0:
                self.commit()
                print(f"  已生成 {i} 条生产计划数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.plans)} 条生产计划数据")
    
    def generate_production_orders(self):
        """生成生产工单数据"""
        print("\n14. 生成生产工单数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i, plan_id in enumerate(self.plans):
            work_order_id = generate_id('WO', i + 1)
            self.work_orders.append(work_order_id)
            plan_date = random_date(start_date, end_date)
            self.execute("""
                INSERT INTO ods_production_order 
                (work_order_id, work_order_no, plan_id, product_id, order_quantity, completed_quantity,
                 workshop_id, production_line_id, order_status, start_time, end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                work_order_id,
                f'WO{plan_date.strftime("%Y%m%d")}{i+1:06d}',
                plan_id,
                random.choice(self.products),
                round(random.uniform(100, 10000), 3),
                round(random.uniform(0, 10000), 3),
                random.choice(self.workshops),
                random.choice(self.production_lines),
                random.choice(self.production_statuses),
                random_datetime(plan_date, plan_date + timedelta(days=1)),
                random_datetime(plan_date, plan_date + timedelta(days=7))
            ))
            if (i + 1) % 500 == 0:
                self.commit()
                print(f"  已生成 {i + 1} 条生产工单数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.work_orders)} 条生产工单数据")
    
    def generate_bom(self):
        """生成BOM数据"""
        print("\n15. 生成BOM数据...")
        bom_count = 0
        for product in self.products[:100]:  # 为前100个产品生成BOM
            bom_count_per_product = random.randint(3, 10)
            for j in range(bom_count_per_product):
                bom_count += 1
                material = random.choice(self.materials[:1000])  # 从前1000个物料中选择
                self.execute("""
                    INSERT INTO ods_bom 
                    (bom_id, bom_no, product_id, material_id, material_code, material_name, 
                     quantity, unit, loss_rate, version, effective_date, expire_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('BOM', bom_count),
                    f'BOM{product}V1',
                    product,
                    material,
                    f'MAT{random.randint(1, 1000):06d}',
                    f'物料{random.randint(1, 1000)}',
                    round(random.uniform(0.1, 10), 6),
                    'kg',
                    round(random.uniform(0, 5), 2),
                    'V1',
                    datetime(2023, 1, 1),
                    datetime(2025, 12, 31)
                ))
            if bom_count % 200 == 0:
                self.commit()
                print(f"  已生成 {bom_count} 条BOM数据...")
        self.commit()
        print(f"  ✓ 已生成约 {bom_count} 条BOM数据")
    
    def generate_inventory(self):
        """生成库存数据（限制在10000行以内）"""
        print("\n16. 生成库存数据...")
        inventory_count = 0
        max_inventory = 10000  # 限制最大10000行
        # 计算每个物料可以对应几个仓库，确保不超过10000行
        num_warehouses = len(self.warehouses)
        max_materials = max_inventory // num_warehouses if num_warehouses > 0 else max_inventory
        materials_to_process = self.materials[:max_materials]
        
        for i, material in enumerate(materials_to_process):
            if inventory_count >= max_inventory:
                break
            for warehouse in self.warehouses:
                if inventory_count >= max_inventory:
                    break
                inventory_count += 1
                quantity = round(random.uniform(100, 10000), 3)
                available_quantity = round(random.uniform(50, quantity), 3)
                reserved_quantity = round(random.uniform(0, quantity - available_quantity), 3)
                unit_cost = round(random.uniform(1, 100), 2)
                total_cost = round(quantity * unit_cost, 2)
                self.execute("""
                    INSERT INTO ods_inventory 
                    (inventory_id, material_id, warehouse_id, location_code, quantity, available_quantity,
                     reserved_quantity, unit_cost, total_cost, batch_no)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('INV', inventory_count),
                    material,
                    warehouse,
                    f'LOC{random.randint(1, 100):03d}',
                    quantity,
                    available_quantity,
                    reserved_quantity,
                    unit_cost,
                    total_cost,
                    f'BATCH{random.randint(1, 1000):06d}'
                ))
            if (i + 1) % 500 == 0:
                self.commit()
                print(f"  已生成约 {inventory_count} 条库存数据...")
        self.commit()
        print(f"  ✓ 已生成约 {inventory_count} 条库存数据（限制在{max_inventory}行以内）")
    
    def generate_purchase_orders(self):
        """生成采购订单数据"""
        print("\n17. 生成采购订单数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 2001):
            purchase_id = generate_id('PO', i)
            self.purchase_orders.append(purchase_id)
            order_date = random_datetime(start_date, end_date)
            self.execute("""
                INSERT INTO ods_purchase_order 
                (purchase_id, purchase_no, supplier_id, order_date, delivery_date, order_status,
                 total_amount, currency, buyer_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                purchase_id,
                f'PO{order_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(self.suppliers),
                order_date,
                order_date + timedelta(days=random.randint(7, 30)),
                random.choice(self.order_statuses),
                round(random.uniform(1000, 50000), 2),
                'CNY',
                random.choice(self.employees) if self.employees else None
            ))
            if i % 500 == 0:
                self.commit()
                print(f"  已生成 {i} 条采购订单数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.purchase_orders)} 条采购订单数据")
    
    def generate_purchase_details(self):
        """生成采购明细数据"""
        print("\n18. 生成采购明细数据...")
        detail_count = 0
        for purchase_id in self.purchase_orders:
            detail_count_per_order = random.randint(1, 5)
            for j in range(detail_count_per_order):
                detail_count += 1
                material = random.choice(self.materials[:1000])
                quantity = round(random.uniform(100, 10000), 3)
                unit_price = round(random.uniform(1, 100), 2)
                amount = round(quantity * unit_price, 2)
                self.execute("""
                    INSERT INTO ods_purchase_detail 
                    (detail_id, purchase_id, material_id, quantity, unit_price, amount, delivery_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('PD', detail_count),
                    purchase_id,
                    material,
                    quantity,
                    unit_price,
                    amount,
                    random_date(datetime(2023, 1, 1), datetime(2024, 12, 31))
                ))
            if detail_count % 1000 == 0:
                self.commit()
                print(f"  已生成 {detail_count} 条采购明细数据...")
        self.commit()
        print(f"  ✓ 已生成约 {detail_count} 条采购明细数据")
    
    def generate_inbound_orders(self):
        """生成入库单数据"""
        print("\n19. 生成入库单数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 1501):
            inbound_id = generate_id('IN', i)
            self.inbound_orders.append(inbound_id)
            inbound_date = random_datetime(start_date, end_date)
            self.execute("""
                INSERT INTO ods_inbound_order 
                (inbound_id, inbound_no, inbound_type, warehouse_id, supplier_id, inbound_date,
                 inbound_status, total_amount, operator_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                inbound_id,
                f'IN{inbound_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(['采购入库', '生产入库', '退货入库']),
                random.choice(self.warehouses),
                random.choice(self.suppliers),
                inbound_date,
                random.choice(['待入库', '已入库', '已取消']),
                round(random.uniform(1000, 50000), 2),
                random.choice(self.employees) if self.employees else None
            ))
            if i % 500 == 0:
                self.commit()
                print(f"  已生成 {i} 条入库单数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.inbound_orders)} 条入库单数据")
    
    def generate_inbound_details(self):
        """生成入库明细数据"""
        print("\n20. 生成入库明细数据...")
        detail_count = 0
        for inbound_id in self.inbound_orders:
            detail_count_per_order = random.randint(1, 5)
            for j in range(detail_count_per_order):
                detail_count += 1
                material = random.choice(self.materials[:1000])
                quantity = round(random.uniform(100, 10000), 3)
                unit_price = round(random.uniform(1, 100), 2)
                amount = round(quantity * unit_price, 2)
                self.execute("""
                    INSERT INTO ods_inbound_detail 
                    (detail_id, inbound_id, material_id, quantity, unit_price, amount, batch_no, location_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('ID', detail_count),
                    inbound_id,
                    material,
                    quantity,
                    unit_price,
                    amount,
                    f'BATCH{random.randint(1, 1000):06d}',
                    f'LOC{random.randint(1, 100):03d}'
                ))
            if detail_count % 1000 == 0:
                self.commit()
                print(f"  已生成 {detail_count} 条入库明细数据...")
        self.commit()
        print(f"  ✓ 已生成约 {detail_count} 条入库明细数据")
    
    def generate_outbound_orders(self):
        """生成出库单数据"""
        print("\n21. 生成出库单数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 1501):
            outbound_id = generate_id('OUT', i)
            self.outbound_orders.append(outbound_id)
            outbound_date = random_datetime(start_date, end_date)
            self.execute("""
                INSERT INTO ods_outbound_order 
                (outbound_id, outbound_no, outbound_type, warehouse_id, customer_id, outbound_date,
                 outbound_status, operator_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                outbound_id,
                f'OUT{outbound_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(['销售出库', '生产领料', '调拨出库']),
                random.choice(self.warehouses),
                random.choice(self.customers),
                outbound_date,
                random.choice(['待出库', '已出库', '已取消']),
                random.choice(self.employees) if self.employees else None
            ))
            if i % 500 == 0:
                self.commit()
                print(f"  已生成 {i} 条出库单数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(self.outbound_orders)} 条出库单数据")
    
    def generate_outbound_details(self):
        """生成出库明细数据"""
        print("\n22. 生成出库明细数据...")
        detail_count = 0
        for outbound_id in self.outbound_orders:
            detail_count_per_order = random.randint(1, 5)
            for j in range(detail_count_per_order):
                detail_count += 1
                material = random.choice(self.materials[:1000])
                quantity = round(random.uniform(100, 10000), 3)
                unit_price = round(random.uniform(1, 100), 2)
                amount = round(quantity * unit_price, 2)
                self.execute("""
                    INSERT INTO ods_outbound_detail 
                    (detail_id, outbound_id, material_id, quantity, unit_price, amount, batch_no, location_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('OD', detail_count),
                    outbound_id,
                    material,
                    quantity,
                    unit_price,
                    amount,
                    f'BATCH{random.randint(1, 1000):06d}',
                    f'LOC{random.randint(1, 100):03d}'
                ))
            if detail_count % 1000 == 0:
                self.commit()
                print(f"  已生成 {detail_count} 条出库明细数据...")
        self.commit()
        print(f"  ✓ 已生成约 {detail_count} 条出库明细数据")
    
    def generate_equipments(self):
        """生成设备数据"""
        print("\n23. 生成设备数据...")
        for line_id in self.production_lines:
            for j in range(1, 4):
                equipment_id = generate_id('EQ', len(self.equipments) + 1)
                self.equipments.append(equipment_id)
                workshop_id = random.choice(self.workshops)
                self.execute("""
                    INSERT INTO ods_equipment_master 
                    (equipment_id, equipment_code, equipment_name, equipment_type, workshop_id,
                     production_line_id, manufacturer, model, purchase_date, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    equipment_id,
                    generate_code('EQ', len(self.equipments)),
                    f'设备{len(self.equipments)}',
                    random.choice(['加工设备', '装配设备', '检测设备', '包装设备']),
                    workshop_id,
                    line_id,
                    random.choice(['制造商A', '制造商B', '制造商C']),
                    f'MODEL{random.randint(1, 100)}',
                    random_date(datetime(2020, 1, 1), datetime(2023, 12, 31)),
                    '正常'
                ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.equipments)} 条设备数据")
    
    def generate_equipment_runtime(self):
        """生成设备运行记录数据（限制数量，避免超过10000行）"""
        print("\n24. 生成设备运行记录数据...")
        runtime_count = 0
        # 限制设备数量，确保总记录数不超过10000
        max_equipments = min(len(self.equipments), 30)  # 最多30台设备
        days_per_equipment = 10000 // max_equipments  # 每台设备约333条记录
        
        for i, equipment_id in enumerate(self.equipments[:max_equipments]):
            for day in range(days_per_equipment):
                runtime_count += 1
                record_date = datetime(2023, 1, 1) + timedelta(days=day)
                running_hours = round(random.uniform(8, 24), 2)
                downtime_hours = round(random.uniform(0, 4), 2)
                self.execute("""
                    INSERT INTO ods_equipment_runtime 
                    (runtime_id, equipment_id, record_date, start_time, end_time, running_hours,
                     production_quantity, downtime_hours, downtime_reason, energy_consumption)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('RT', runtime_count),
                    equipment_id,
                    record_date,
                    record_date.replace(hour=8, minute=0),
                    record_date.replace(hour=20, minute=0),
                    running_hours,
                    round(random.uniform(100, 10000), 3),
                    downtime_hours,
                    random.choice(['维护', '故障', '待料', '无']) if downtime_hours > 0 else None,
                    round(random.uniform(100, 1000), 2)
                ))
            if (i + 1) % 10 == 0:
                self.commit()
                print(f"  已生成约 {runtime_count} 条设备运行记录数据...")
        self.commit()
        print(f"  ✓ 已生成 {runtime_count} 条设备运行记录数据")
    
    def generate_quality_inspections(self):
        """生成质量检验数据"""
        print("\n25. 生成质量检验数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        # 限制工单数量，确保不超过10000行
        work_orders_to_process = self.work_orders[:1000]
        for i, work_order_id in enumerate(work_orders_to_process):
            inspection_date = random_datetime(start_date, end_date)
            sample_qty = random.randint(10, 100)
            qualified_qty = random.randint(int(sample_qty * 0.85), sample_qty)
            unqualified_qty = sample_qty - qualified_qty
            qualified_rate = round((qualified_qty / sample_qty) * 100, 2) if sample_qty > 0 else 0
            self.execute("""
                INSERT INTO ods_quality_inspection 
                (inspection_id, inspection_no, work_order_id, product_id, inspection_date,
                 inspection_type, sample_quantity, qualified_quantity, unqualified_quantity,
                 qualified_rate, inspector_id, inspection_result)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                generate_id('QI', i + 1),
                f'QI{inspection_date.strftime("%Y%m%d")}{i+1:06d}',
                work_order_id,
                random.choice(self.products),
                inspection_date,
                random.choice(['首检', '过程检', '终检']),
                sample_qty,
                qualified_qty,
                unqualified_qty,
                qualified_rate,
                random.choice(self.employees) if self.employees else None,
                '合格' if qualified_qty >= sample_qty * 0.9 else '不合格'
            ))
            if (i + 1) % 200 == 0:
                self.commit()
                print(f"  已生成 {i + 1} 条质量检验数据...")
        self.commit()
        print(f"  ✓ 已生成 {len(work_orders_to_process)} 条质量检验数据")
    
    def generate_defect_records(self):
        """生成不合格品记录数据"""
        print("\n26. 生成不合格品记录数据...")
        defect_types = ['尺寸超差', '表面缺陷', '功能不良', '外观不良', '其他']
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        for i in range(1, 501):
            defect_date = random_datetime(start_date, end_date)
            self.execute("""
                INSERT INTO ods_defect_record 
                (defect_id, inspection_id, work_order_id, product_id, defect_date, defect_type,
                 defect_code, defect_description, quantity, severity, handler_id, handle_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                generate_id('DEF', i),
                generate_id('QI', random.randint(1, 1000)),
                random.choice(self.work_orders[:1000]) if self.work_orders else None,
                random.choice(self.products),
                defect_date,
                random.choice(defect_types),
                f'DEF{random.randint(1, 100):03d}',
                f'缺陷描述{i}',
                random.randint(1, 50),
                random.choice(['轻微', '一般', '严重']),
                random.choice(self.employees) if self.employees else None,
                random.choice(['返工', '报废', '让步接收'])
            ))
        self.commit()
        print(f"  ✓ 已生成 500 条不合格品记录数据")
    
    def generate_attendance(self):
        """生成考勤数据（限制数量，避免超过10000行）"""
        print("\n27. 生成考勤数据...")
        attendance_count = 0
        # 限制员工数量，确保总记录数不超过10000
        max_employees = min(len(self.employees), 40)  # 最多40个员工
        days_per_employee = 10000 // max_employees  # 每个员工约250条记录
        
        for i, employee_id in enumerate(self.employees[:max_employees]):
            for day in range(days_per_employee):
                attendance_count += 1
                attendance_date = datetime(2023, 1, 1) + timedelta(days=day)
                if attendance_date.weekday() < 5:  # 工作日
                    check_in = attendance_date.replace(hour=8, minute=random.randint(0, 30))
                    check_out = attendance_date.replace(hour=17, minute=random.randint(0, 30))
                    work_hours = round((check_out - check_in).total_seconds() / 3600, 2)
                    overtime_hours = round(random.uniform(0, 4), 2) if random.random() > 0.7 else 0
                    self.execute("""
                        INSERT INTO ods_attendance 
                        (attendance_id, employee_id, attendance_date, check_in_time, check_out_time,
                         work_hours, overtime_hours, attendance_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        generate_id('ATT', attendance_count),
                        employee_id,
                        attendance_date,
                        check_in,
                        check_out,
                        work_hours,
                        overtime_hours,
                        '正常'
                    ))
            if (i + 1) % 10 == 0:
                self.commit()
                print(f"  已生成约 {attendance_count} 条考勤数据...")
        self.commit()
        print(f"  ✓ 已生成约 {attendance_count} 条考勤数据")
    
    def generate_sales_payments(self):
        """生成销售回款数据"""
        print("\n28. 生成销售回款数据...")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        payment_count = 0
        # 限制订单数量，确保不超过10000行
        orders_to_process = self.orders[:2000]
        for i, order_id in enumerate(orders_to_process):
            if random.random() > 0.3:  # 70%的订单有回款
                payment_count += 1
                payment_date = random_datetime(start_date, end_date)
                self.execute("""
                    INSERT INTO ods_sales_payment 
                    (payment_id, payment_no, order_id, customer_id, payment_date, payment_amount,
                     payment_method, payment_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('PAY', payment_count),
                    f'PAY{payment_date.strftime("%Y%m%d")}{payment_count:06d}',
                    order_id,
                    random.choice(self.customers),
                    payment_date,
                    round(random.uniform(1000, 100000), 2),
                    random.choice(['现金', '银行转账', '支票', '承兑汇票']),
                    random.choice(['已收款', '部分收款', '待收款'])
                ))
            if payment_count > 0 and payment_count % 500 == 0:
                self.commit()
                print(f"  已生成 {payment_count} 条销售回款数据...")
        self.commit()
        print(f"  ✓ 已生成约 {payment_count} 条销售回款数据")
    
    def generate_cost_centers(self):
        """生成成本中心数据"""
        print("\n29. 生成成本中心数据...")
        cost_types = ['直接材料', '直接人工', '制造费用', '管理费用', '销售费用']
        for dept_id in self.departments:
            for cost_type in cost_types:
                cost_center_id = generate_id('CC', len(self.cost_centers) + 1)
                self.cost_centers.append(cost_center_id)
                self.execute("""
                    INSERT INTO ods_cost_center 
                    (cost_center_id, cost_center_code, cost_center_name, department_id, cost_type, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    cost_center_id,
                    generate_code('CC', len(self.cost_centers)),
                    f'成本中心{len(self.cost_centers)}',
                    dept_id,
                    cost_type,
                    '正常'
                ))
        self.commit()
        print(f"  ✓ 已生成 {len(self.cost_centers)} 条成本中心数据")
    
    def generate_cost_details(self):
        """生成成本明细数据（限制数量，避免超过10000行）"""
        print("\n30. 生成成本明细数据...")
        cost_detail_count = 0
        # 限制成本中心数量，确保总记录数不超过10000
        max_cost_centers = min(len(self.cost_centers), 100)  # 最多100个成本中心
        days_per_center = 10000 // max_cost_centers  # 每个成本中心约100条记录
        
        cost_types = ['直接材料', '直接人工', '制造费用', '管理费用', '销售费用']
        for i, cost_center_id in enumerate(self.cost_centers[:max_cost_centers]):
            for day in range(days_per_center):
                cost_detail_count += 1
                cost_date = datetime(2023, 1, 1) + timedelta(days=day)
                amount = round(random.uniform(100, 10000), 2)
                quantity = round(random.uniform(1, 1000), 3)
                unit_cost = round(amount / quantity, 2) if quantity > 0 else 0
                self.execute("""
                    INSERT INTO ods_cost_detail 
                    (cost_id, cost_center_id, cost_date, cost_type, cost_item, amount, quantity, unit_cost)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('COST', cost_detail_count),
                    cost_center_id,
                    cost_date,
                    random.choice(cost_types),
                    random.choice(['材料费', '人工费', '水电费', '折旧费', '其他']),
                    amount,
                    quantity,
                    unit_cost
                ))
            if (i + 1) % 20 == 0:
                self.commit()
                print(f"  已生成约 {cost_detail_count} 条成本明细数据...")
        self.commit()
        print(f"  ✓ 已生成约 {cost_detail_count} 条成本明细数据")
    
    def print_statistics(self):
        """打印统计信息"""
        print("\n数据统计:")
        print(f"  - 工厂: {len(self.factories)}")
        print(f"  - 部门: {len(self.departments)}")
        print(f"  - 车间: {len(self.workshops)}")
        print(f"  - 生产线: {len(self.production_lines)}")
        print(f"  - 客户: {len(self.customers)}")
        print(f"  - 供应商: {len(self.suppliers)}")
        print(f"  - 产品: {len(self.products)}")
        print(f"  - 物料: {len(self.materials)} (最大表，10000行)")
        print(f"  - 员工: {len(self.employees)}")
        print(f"  - 仓库: {len(self.warehouses)}")
        print(f"  - 订单: {len(self.orders)}")
        print(f"  - 生产计划: {len(self.plans)}")
        print(f"  - 生产工单: {len(self.work_orders)}")
        print(f"  - 采购订单: {len(self.purchase_orders)}")
        print(f"  - 设备: {len(self.equipments)}")
        print(f"  - 成本中心: {len(self.cost_centers)}")


def generate_for_mysql():
    """为MySQL生成测试数据"""
    if not MYSQL_AVAILABLE:
        print("⚠ MySQL驱动未安装，跳过MySQL数据生成")
        return False
    
    conn = None
    try:
        print("\n" + "=" * 60)
        print("连接MySQL数据库...")
        print("=" * 60)
        conn = mysql.connector.connect(**DB_CONFIG['mysql'])
        if conn.is_connected():
            print("✓ MySQL数据库连接成功")
            generator = DataGenerator(conn, 'mysql')
            generator.generate_all()
            return True
    except MySQLError as e:
        print(f"❌ MySQL数据库错误: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\nMySQL数据库连接已关闭")
    return False


def generate_for_postgresql():
    """为PostgreSQL生成测试数据"""
    if not POSTGRESQL_AVAILABLE:
        print("⚠ PostgreSQL驱动未安装，跳过PostgreSQL数据生成")
        return False
    
    conn = None
    try:
        print("\n" + "=" * 60)
        print("连接PostgreSQL数据库...")
        print("=" * 60)
        conn = psycopg2.connect(**DB_CONFIG['postgresql'])
        print("✓ PostgreSQL数据库连接成功")
        generator = DataGenerator(conn, 'postgresql')
        generator.generate_all()
        return True
    except PostgreSQLError as e:
        print(f"❌ PostgreSQL数据库错误: {e}")
        return False
    finally:
        if conn:
            conn.close()
            print("\nPostgreSQL数据库连接已关闭")
    return False


def main():
    """主函数"""
    print("=" * 60)
    print("制造业数仓测试数据生成工具 - 支持MySQL和PostgreSQL")
    print("最大表行数: 10000行")
    print("=" * 60)
    
    if not MYSQL_AVAILABLE and not POSTGRESQL_AVAILABLE:
        print("\n❌ 错误: 未安装任何数据库驱动")
        print("请安装: pip install mysql-connector-python psycopg2-binary")
        sys.exit(1)
    
    mysql_success = False
    postgresql_success = False
    
    # 生成MySQL数据
    if MYSQL_AVAILABLE:
        try:
            mysql_success = generate_for_mysql()
        except Exception as e:
            print(f"❌ MySQL数据生成失败: {e}")
    
    # 生成PostgreSQL数据
    if POSTGRESQL_AVAILABLE:
        try:
            postgresql_success = generate_for_postgresql()
        except Exception as e:
            print(f"❌ PostgreSQL数据生成失败: {e}")
    
    # 总结
    print("\n" + "=" * 60)
    print("数据生成总结")
    print("=" * 60)
    print(f"MySQL: {'✓ 成功' if mysql_success else '✗ 失败'}")
    print(f"PostgreSQL: {'✓ 成功' if postgresql_success else '✗ 失败'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
