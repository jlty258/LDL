#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
制造业数仓测试数据生成脚本
最大表生成10000行数据
"""

import mysql.connector
from mysql.connector import Error
import random
from datetime import datetime, timedelta
import string

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'database': 'sqlExpert',
    'charset': 'utf8mb4'
}

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

def generate_test_data(conn):
    """生成测试数据"""
    cursor = conn.cursor()
    
    try:
        # 基础数据
        regions = ['华东', '华南', '华北', '华中', '西南', '西北', '东北']
        cities = ['上海', '北京', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']
        statuses = ['正常', '暂停', '关闭']
        order_statuses = ['待确认', '已确认', '生产中', '已完成', '已取消']
        production_statuses = ['计划中', '生产中', '已完成', '已暂停', '已取消']
        
        print("开始生成测试数据...")
        
        # 1. 生成工厂数据
        print("生成工厂数据...")
        factories = []
        for i in range(1, 6):
            factory_id = generate_id('F', i)
            factories.append(factory_id)
            cursor.execute("""
                INSERT INTO ods_factory_master 
                (factory_id, factory_code, factory_name, region, city, address, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                factory_id,
                generate_code('FAC', i),
                f'制造工厂{i}',
                random.choice(regions),
                random.choice(cities),
                f'{random.choice(cities)}市工业园区{i}号',
                '正常'
            ))
        
        # 2. 生成部门数据
        print("生成部门数据...")
        departments = []
        dept_names = ['生产部', '质量部', '采购部', '销售部', '仓储部', '财务部', '人事部']
        for i, factory_id in enumerate(factories):
            for j, dept_name in enumerate(dept_names):
                dept_id = generate_id('DEPT', len(departments) + 1)
                departments.append(dept_id)
                cursor.execute("""
                    INSERT INTO ods_department_master 
                    (department_id, department_code, department_name, factory_id, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    dept_id,
                    generate_code('DEPT', len(departments)),
                    dept_name,
                    factory_id,
                    '正常'
                ))
        
        # 3. 生成车间数据
        print("生成车间数据...")
        workshops = []
        for i, factory_id in enumerate(factories):
            for j in range(1, 4):
                workshop_id = generate_id('WS', len(workshops) + 1)
                workshops.append(workshop_id)
                cursor.execute("""
                    INSERT INTO ods_workshop_master 
                    (workshop_id, workshop_code, workshop_name, factory_id, workshop_type, capacity, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    workshop_id,
                    generate_code('WS', len(workshops)),
                    f'车间{len(workshops)}',
                    factory_id,
                    random.choice(['装配', '加工', '包装']),
                    random.uniform(1000, 10000),
                    '正常'
                ))
        
        # 4. 生成生产线数据
        print("生成生产线数据...")
        production_lines = []
        for workshop_id in workshops:
            for j in range(1, 3):
                line_id = generate_id('PL', len(production_lines) + 1)
                production_lines.append(line_id)
                cursor.execute("""
                    INSERT INTO ods_production_line 
                    (line_id, line_code, line_name, workshop_id, line_type, capacity, efficiency, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    line_id,
                    generate_code('PL', len(production_lines)),
                    f'生产线{len(production_lines)}',
                    workshop_id,
                    random.choice(['自动', '半自动', '手动']),
                    random.uniform(500, 5000),
                    random.uniform(80, 98),
                    '正常'
                ))
        
        # 5. 生成客户数据
        print("生成客户数据...")
        customers = []
        for i in range(1, 201):
            customer_id = generate_id('C', i)
            customers.append(customer_id)
            cursor.execute("""
                INSERT INTO ods_customer_master 
                (customer_id, customer_code, customer_name, customer_type, industry, region, city, 
                 contact_person, contact_phone, credit_level, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                customer_id,
                generate_code('CUST', i),
                f'客户公司{i}',
                random.choice(['企业', '经销商', '代理商']),
                random.choice(['汽车', '电子', '机械', '化工', '纺织']),
                random.choice(regions),
                random.choice(cities),
                f'联系人{i}',
                f'138{random.randint(10000000, 99999999)}',
                random.choice(['A', 'B', 'C', 'D']),
                '正常'
            ))
        
        # 6. 生成供应商数据
        print("生成供应商数据...")
        suppliers = []
        for i in range(1, 101):
            supplier_id = generate_id('S', i)
            suppliers.append(supplier_id)
            cursor.execute("""
                INSERT INTO ods_supplier_master 
                (supplier_id, supplier_code, supplier_name, supplier_type, region, city,
                 contact_person, contact_phone, credit_level, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                supplier_id,
                generate_code('SUP', i),
                f'供应商{i}',
                random.choice(['原材料', '零部件', '设备', '服务']),
                random.choice(regions),
                random.choice(cities),
                f'联系人{i}',
                f'139{random.randint(10000000, 99999999)}',
                random.choice(['A', 'B', 'C', 'D']),
                '正常'
            ))
        
        # 7. 生成产品数据
        print("生成产品数据...")
        products = []
        categories = ['成品', '半成品', '零部件']
        brands = ['品牌A', '品牌B', '品牌C', '品牌D']
        for i in range(1, 301):
            product_id = generate_id('P', i)
            products.append(product_id)
            cursor.execute("""
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
                random.uniform(100, 10000),
                random.uniform(50, 5000),
                random.uniform(0.1, 100),
                random.uniform(0.01, 10),
                '正常'
            ))
        
        # 8. 生成物料数据（最大表，10000行）
        print("生成物料数据（10000行）...")
        materials = []
        material_categories = ['原材料', '辅料', '包装材料', '备件', '工具']
        for i in range(1, 10001):
            material_id = generate_id('M', i)
            materials.append(material_id)
            cursor.execute("""
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
                random.uniform(1, 1000),
                random.uniform(0.5, 500),
                random.choice(suppliers) if suppliers else None,
                random.randint(1, 30),
                random.uniform(100, 1000),
                random.uniform(1000, 10000),
                '正常'
            ))
            if i % 1000 == 0:
                print(f"  已生成 {i} 条物料数据...")
                conn.commit()
        
        # 9. 生成员工数据
        print("生成员工数据...")
        employees = []
        positions = ['工人', '技术员', '质检员', '班组长', '车间主任', '经理']
        for i in range(1, 501):
            employee_id = generate_id('E', i)
            employees.append(employee_id)
            dept_id = random.choice(departments) if departments else None
            workshop_id = random.choice(workshops) if workshops else None
            cursor.execute("""
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
        
        # 10. 生成仓库数据
        print("生成仓库数据...")
        warehouses = []
        for i, factory_id in enumerate(factories):
            for j in range(1, 3):
                warehouse_id = generate_id('W', len(warehouses) + 1)
                warehouses.append(warehouse_id)
                cursor.execute("""
                    INSERT INTO ods_warehouse_master 
                    (warehouse_id, warehouse_code, warehouse_name, warehouse_type, factory_id, capacity, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    warehouse_id,
                    generate_code('WH', len(warehouses)),
                    f'仓库{len(warehouses)}',
                    random.choice(['原料库', '成品库', '半成品库']),
                    factory_id,
                    random.uniform(10000, 100000),
                    '正常'
                ))
        
        # 11. 生成订单数据
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
            """, (
                order_id,
                f'ORD{order_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(customers),
                order_date,
                random.choice(order_statuses),
                random.uniform(1000, 100000),
                'CNY',
                random.choice(employees) if employees else None,
                random.choice(warehouses)
            ))
        
        # 12. 生成订单明细数据
        print("生成订单明细数据...")
        for order_id in orders:
            detail_count = random.randint(1, 5)
            for j in range(detail_count):
                product = random.choice(products)
                quantity = random.uniform(1, 100)
                unit_price = random.uniform(100, 1000)
                cursor.execute("""
                    INSERT INTO ods_order_detail 
                    (detail_id, order_id, product_id, product_code, product_name, quantity, unit_price, amount, unit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('OD', len(orders) * 5 + j),
                    order_id,
                    product,
                    f'PROD{random.randint(1, 300):06d}',
                    f'产品{random.randint(1, 300)}',
                    quantity,
                    unit_price,
                    quantity * unit_price,
                    '件'
                ))
        
        # 13. 生成生产计划数据
        print("生成生产计划数据...")
        plans = []
        for i in range(1, 2001):
            plan_id = generate_id('PLAN', i)
            plans.append(plan_id)
            plan_date = random_date(datetime(2023, 1, 1), datetime(2024, 12, 31))
            cursor.execute("""
                INSERT INTO ods_production_plan 
                (plan_id, plan_no, product_id, plan_date, plan_quantity, actual_quantity, 
                 workshop_id, production_line_id, plan_status, start_time, end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                plan_id,
                f'PLAN{plan_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(products),
                plan_date,
                random.uniform(100, 10000),
                random.uniform(80, 10000),
                random.choice(workshops),
                random.choice(production_lines),
                random.choice(production_statuses),
                random_datetime(plan_date, plan_date + timedelta(days=1)),
                random_datetime(plan_date, plan_date + timedelta(days=7))
            ))
        
        # 14. 生成生产工单数据
        print("生成生产工单数据...")
        work_orders = []
        for i, plan_id in enumerate(plans):
            work_order_id = generate_id('WO', i + 1)
            work_orders.append(work_order_id)
            plan_date = random_date(datetime(2023, 1, 1), datetime(2024, 12, 31))
            cursor.execute("""
                INSERT INTO ods_production_order 
                (work_order_id, work_order_no, plan_id, product_id, order_quantity, completed_quantity,
                 workshop_id, production_line_id, order_status, start_time, end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                work_order_id,
                f'WO{plan_date.strftime("%Y%m%d")}{i+1:06d}',
                plan_id,
                random.choice(products),
                random.uniform(100, 10000),
                random.uniform(0, 10000),
                random.choice(workshops),
                random.choice(production_lines),
                random.choice(production_statuses),
                random_datetime(plan_date, plan_date + timedelta(days=1)),
                random_datetime(plan_date, plan_date + timedelta(days=7))
            ))
        
        # 15. 生成BOM数据
        print("生成BOM数据...")
        for product in products[:100]:  # 为前100个产品生成BOM
            bom_count = random.randint(3, 10)
            for j in range(bom_count):
                material = random.choice(materials[:1000])  # 从前1000个物料中选择
                cursor.execute("""
                    INSERT INTO ods_bom 
                    (bom_id, bom_no, product_id, material_id, material_code, material_name, 
                     quantity, unit, loss_rate, version, effective_date, expire_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('BOM', len(products) * 10 + j),
                    f'BOM{product}V1',
                    product,
                    material,
                    f'MAT{random.randint(1, 1000):06d}',
                    f'物料{random.randint(1, 1000)}',
                    random.uniform(0.1, 10),
                    'kg',
                    random.uniform(0, 5),
                    'V1',
                    datetime(2023, 1, 1),
                    datetime(2025, 12, 31)
                ))
        
        # 16. 生成库存数据
        print("生成库存数据...")
        for i, material in enumerate(materials[:5000]):  # 为前5000个物料生成库存
            for warehouse in warehouses:
                cursor.execute("""
                    INSERT INTO ods_inventory 
                    (inventory_id, material_id, warehouse_id, location_code, quantity, available_quantity,
                     reserved_quantity, unit_cost, total_cost, batch_no)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('INV', i * len(warehouses) + warehouses.index(warehouse) + 1),
                    material,
                    warehouse,
                    f'LOC{random.randint(1, 100):03d}',
                    random.uniform(100, 10000),
                    random.uniform(50, 9000),
                    random.uniform(0, 1000),
                    random.uniform(1, 100),
                    random.uniform(100, 1000000),
                    f'BATCH{random.randint(1, 1000):06d}'
                ))
        
        # 17. 生成采购订单数据
        print("生成采购订单数据...")
        purchase_orders = []
        for i in range(1, 2001):
            purchase_id = generate_id('PO', i)
            purchase_orders.append(purchase_id)
            order_date = random_datetime(start_date, end_date)
            cursor.execute("""
                INSERT INTO ods_purchase_order 
                (purchase_id, purchase_no, supplier_id, order_date, delivery_date, order_status,
                 total_amount, currency, buyer_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                purchase_id,
                f'PO{order_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(suppliers),
                order_date,
                order_date + timedelta(days=random.randint(7, 30)),
                random.choice(order_statuses),
                random.uniform(1000, 50000),
                'CNY',
                random.choice(employees) if employees else None
            ))
        
        # 18. 生成采购明细数据
        print("生成采购明细数据...")
        for purchase_id in purchase_orders:
            detail_count = random.randint(1, 5)
            for j in range(detail_count):
                material = random.choice(materials[:1000])
                quantity = random.uniform(100, 10000)
                unit_price = random.uniform(1, 100)
                cursor.execute("""
                    INSERT INTO ods_purchase_detail 
                    (detail_id, purchase_id, material_id, quantity, unit_price, amount, delivery_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('PD', len(purchase_orders) * 5 + j),
                    purchase_id,
                    material,
                    quantity,
                    unit_price,
                    quantity * unit_price,
                    random_date(datetime(2023, 1, 1), datetime(2024, 12, 31))
                ))
        
        # 19. 生成入库单数据
        print("生成入库单数据...")
        inbound_orders = []
        for i in range(1, 1501):
            inbound_id = generate_id('IN', i)
            inbound_orders.append(inbound_id)
            inbound_date = random_datetime(start_date, end_date)
            cursor.execute("""
                INSERT INTO ods_inbound_order 
                (inbound_id, inbound_no, inbound_type, warehouse_id, supplier_id, inbound_date,
                 inbound_status, total_amount, operator_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                inbound_id,
                f'IN{inbound_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(['采购入库', '生产入库', '退货入库']),
                random.choice(warehouses),
                random.choice(suppliers),
                inbound_date,
                random.choice(['待入库', '已入库', '已取消']),
                random.uniform(1000, 50000),
                random.choice(employees) if employees else None
            ))
        
        # 20. 生成入库明细数据
        print("生成入库明细数据...")
        for inbound_id in inbound_orders:
            detail_count = random.randint(1, 5)
            for j in range(detail_count):
                material = random.choice(materials[:1000])
                quantity = random.uniform(100, 10000)
                unit_price = random.uniform(1, 100)
                cursor.execute("""
                    INSERT INTO ods_inbound_detail 
                    (detail_id, inbound_id, material_id, quantity, unit_price, amount, batch_no, location_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('ID', len(inbound_orders) * 5 + j),
                    inbound_id,
                    material,
                    quantity,
                    unit_price,
                    quantity * unit_price,
                    f'BATCH{random.randint(1, 1000):06d}',
                    f'LOC{random.randint(1, 100):03d}'
                ))
        
        # 21. 生成出库单数据
        print("生成出库单数据...")
        outbound_orders = []
        for i in range(1, 1501):
            outbound_id = generate_id('OUT', i)
            outbound_orders.append(outbound_id)
            outbound_date = random_datetime(start_date, end_date)
            cursor.execute("""
                INSERT INTO ods_outbound_order 
                (outbound_id, outbound_no, outbound_type, warehouse_id, customer_id, outbound_date,
                 outbound_status, operator_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                outbound_id,
                f'OUT{outbound_date.strftime("%Y%m%d")}{i:06d}',
                random.choice(['销售出库', '生产领料', '调拨出库']),
                random.choice(warehouses),
                random.choice(customers),
                outbound_date,
                random.choice(['待出库', '已出库', '已取消']),
                random.choice(employees) if employees else None
            ))
        
        # 22. 生成出库明细数据
        print("生成出库明细数据...")
        for outbound_id in outbound_orders:
            detail_count = random.randint(1, 5)
            for j in range(detail_count):
                material = random.choice(materials[:1000])
                quantity = random.uniform(100, 10000)
                unit_price = random.uniform(1, 100)
                cursor.execute("""
                    INSERT INTO ods_outbound_detail 
                    (detail_id, outbound_id, material_id, quantity, unit_price, amount, batch_no, location_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('OD', len(outbound_orders) * 5 + j),
                    outbound_id,
                    material,
                    quantity,
                    unit_price,
                    quantity * unit_price,
                    f'BATCH{random.randint(1, 1000):06d}',
                    f'LOC{random.randint(1, 100):03d}'
                ))
        
        # 23. 生成设备数据
        print("生成设备数据...")
        equipments = []
        for i, line_id in enumerate(production_lines):
            for j in range(1, 4):
                equipment_id = generate_id('EQ', len(equipments) + 1)
                equipments.append(equipment_id)
                workshop_id = random.choice(workshops)
                cursor.execute("""
                    INSERT INTO ods_equipment_master 
                    (equipment_id, equipment_code, equipment_name, equipment_type, workshop_id,
                     production_line_id, manufacturer, model, purchase_date, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    equipment_id,
                    generate_code('EQ', len(equipments)),
                    f'设备{len(equipments)}',
                    random.choice(['加工设备', '装配设备', '检测设备', '包装设备']),
                    workshop_id,
                    line_id,
                    random.choice(['制造商A', '制造商B', '制造商C']),
                    f'MODEL{random.randint(1, 100)}',
                    random_date(datetime(2020, 1, 1), datetime(2023, 12, 31)),
                    '正常'
                ))
        
        # 24. 生成设备运行记录数据
        print("生成设备运行记录数据...")
        for i, equipment_id in enumerate(equipments):
            for day in range(365):  # 生成一年的数据
                record_date = datetime(2023, 1, 1) + timedelta(days=day)
                running_hours = random.uniform(8, 24)
                downtime_hours = random.uniform(0, 4)
                cursor.execute("""
                    INSERT INTO ods_equipment_runtime 
                    (runtime_id, equipment_id, record_date, start_time, end_time, running_hours,
                     production_quantity, downtime_hours, downtime_reason, energy_consumption)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('RT', i * 365 + day + 1),
                    equipment_id,
                    record_date,
                    record_date.replace(hour=8, minute=0),
                    record_date.replace(hour=20, minute=0),
                    running_hours,
                    random.uniform(100, 10000),
                    downtime_hours,
                    random.choice(['维护', '故障', '待料', '无']) if downtime_hours > 0 else None,
                    random.uniform(100, 1000)
                ))
        
        # 25. 生成质量检验数据
        print("生成质量检验数据...")
        for i, work_order_id in enumerate(work_orders[:1000]):
            inspection_date = random_datetime(start_date, end_date)
            sample_qty = random.randint(10, 100)
            qualified_qty = random.randint(int(sample_qty * 0.85), sample_qty)
            unqualified_qty = sample_qty - qualified_qty
            cursor.execute("""
                INSERT INTO ods_quality_inspection 
                (inspection_id, inspection_no, work_order_id, product_id, inspection_date,
                 inspection_type, sample_quantity, qualified_quantity, unqualified_quantity,
                 qualified_rate, inspector_id, inspection_result)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                generate_id('QI', i + 1),
                f'QI{inspection_date.strftime("%Y%m%d")}{i+1:06d}',
                work_order_id,
                random.choice(products),
                inspection_date,
                random.choice(['首检', '过程检', '终检']),
                sample_qty,
                qualified_qty,
                unqualified_qty,
                (qualified_qty / sample_qty) * 100,
                random.choice(employees) if employees else None,
                '合格' if qualified_qty >= sample_qty * 0.9 else '不合格'
            ))
        
        # 26. 生成不合格品记录数据
        print("生成不合格品记录数据...")
        defect_types = ['尺寸超差', '表面缺陷', '功能不良', '外观不良', '其他']
        for i in range(1, 501):
            defect_date = random_datetime(start_date, end_date)
            cursor.execute("""
                INSERT INTO ods_defect_record 
                (defect_id, inspection_id, work_order_id, product_id, defect_date, defect_type,
                 defect_code, defect_description, quantity, severity, handler_id, handle_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                generate_id('DEF', i),
                generate_id('QI', random.randint(1, 1000)),
                random.choice(work_orders[:1000]),
                random.choice(products),
                defect_date,
                random.choice(defect_types),
                f'DEF{random.randint(1, 100):03d}',
                f'缺陷描述{i}',
                random.randint(1, 50),
                random.choice(['轻微', '一般', '严重']),
                random.choice(employees) if employees else None,
                random.choice(['返工', '报废', '让步接收'])
            ))
        
        # 27. 生成考勤数据
        print("生成考勤数据...")
        for employee_id in employees:
            for day in range(250):  # 生成约一年的工作日数据
                attendance_date = datetime(2023, 1, 1) + timedelta(days=day)
                if attendance_date.weekday() < 5:  # 工作日
                    check_in = attendance_date.replace(hour=8, minute=random.randint(0, 30))
                    check_out = attendance_date.replace(hour=17, minute=random.randint(0, 30))
                    work_hours = (check_out - check_in).total_seconds() / 3600
                    cursor.execute("""
                        INSERT INTO ods_attendance 
                        (attendance_id, employee_id, attendance_date, check_in_time, check_out_time,
                         work_hours, overtime_hours, attendance_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        generate_id('ATT', employees.index(employee_id) * 250 + day + 1),
                        employee_id,
                        attendance_date,
                        check_in,
                        check_out,
                        work_hours,
                        random.uniform(0, 4) if random.random() > 0.7 else 0,
                        '正常'
                    ))
        
        # 28. 生成销售回款数据
        print("生成销售回款数据...")
        for i, order_id in enumerate(orders[:2000]):
            if random.random() > 0.3:  # 70%的订单有回款
                payment_date = random_datetime(start_date, end_date)
                payment_amount = random.uniform(1000, 100000)
                cursor.execute("""
                    INSERT INTO ods_sales_payment 
                    (payment_id, payment_no, order_id, customer_id, payment_date, payment_amount,
                     payment_method, payment_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('PAY', i + 1),
                    f'PAY{payment_date.strftime("%Y%m%d")}{i+1:06d}',
                    order_id,
                    random.choice(customers),
                    payment_date,
                    payment_amount,
                    random.choice(['现金', '银行转账', '支票', '承兑汇票']),
                    random.choice(['已收款', '部分收款', '待收款'])
                ))
        
        # 29. 生成成本中心数据
        print("生成成本中心数据...")
        cost_centers = []
        cost_types = ['直接材料', '直接人工', '制造费用', '管理费用', '销售费用']
        for i, dept_id in enumerate(departments):
            for cost_type in cost_types:
                cost_center_id = generate_id('CC', len(cost_centers) + 1)
                cost_centers.append(cost_center_id)
                cursor.execute("""
                    INSERT INTO ods_cost_center 
                    (cost_center_id, cost_center_code, cost_center_name, department_id, cost_type, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    cost_center_id,
                    generate_code('CC', len(cost_centers)),
                    f'成本中心{len(cost_centers)}',
                    dept_id,
                    cost_type,
                    '正常'
                ))
        
        # 30. 生成成本明细数据
        print("生成成本明细数据...")
        for cost_center_id in cost_centers:
            for day in range(365):  # 生成一年的数据
                cost_date = datetime(2023, 1, 1) + timedelta(days=day)
                amount = random.uniform(100, 10000)
                quantity = random.uniform(1, 1000)
                cursor.execute("""
                    INSERT INTO ods_cost_detail 
                    (cost_id, cost_center_id, cost_date, cost_type, cost_item, amount, quantity, unit_cost)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    generate_id('COST', cost_centers.index(cost_center_id) * 365 + day + 1),
                    cost_center_id,
                    cost_date,
                    random.choice(cost_types),
                    random.choice(['材料费', '人工费', '水电费', '折旧费', '其他']),
                    amount,
                    quantity,
                    amount / quantity if quantity > 0 else 0
                ))
        
        conn.commit()
        print("\n✓ 所有测试数据生成完成！")
        print(f"  - 物料表(ods_material_master): 10000行")
        print(f"  - 其他表: 已生成相应数据")
        
    except Error as e:
        print(f"❌ 错误: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    """主函数"""
    conn = None
    try:
        print("=" * 60)
        print("制造业数仓测试数据生成工具")
        print("=" * 60)
        
        # 连接数据库
        print("\n连接数据库...")
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print("✓ 数据库连接成功")
            
            # 生成测试数据
            generate_test_data(conn)
            
    except Error as e:
        print(f"❌ 数据库错误: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\n数据库连接已关闭")

if __name__ == "__main__":
    main()
