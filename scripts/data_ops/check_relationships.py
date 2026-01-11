#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查ODS表之间的关联关系完整性
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

def check_mysql_relationships():
    """检查MySQL的关联关系"""
    print("=" * 70)
    print("MySQL 关联关系检查")
    print("=" * 70)
    
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    issues = []
    
    # 1. 订单明细 -> 订单主表
    print("\n1. 检查订单明细与订单主表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_order_detail od
        LEFT JOIN ods_order_master om ON od.order_id = om.order_id
        WHERE om.order_id IS NULL
    """)
    orphan_details = cursor.fetchone()[0]
    if orphan_details > 0:
        issues.append(f"订单明细表有 {orphan_details} 条记录关联不到订单主表")
        print(f"  ✗ 发现 {orphan_details} 条孤立订单明细")
    else:
        print(f"  ✓ 订单明细全部关联到订单主表")
    
    # 2. 订单主表 -> 客户表
    print("\n2. 检查订单主表与客户表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_order_master om
        LEFT JOIN ods_customer_master cm ON om.customer_id = cm.customer_id
        WHERE cm.customer_id IS NULL AND om.customer_id IS NOT NULL
    """)
    orphan_orders = cursor.fetchone()[0]
    if orphan_orders > 0:
        issues.append(f"订单主表有 {orphan_orders} 条记录关联不到客户表")
        print(f"  ✗ 发现 {orphan_orders} 条订单关联不到客户")
    else:
        print(f"  ✓ 订单全部关联到客户表")
    
    # 3. 订单明细 -> 产品表
    print("\n3. 检查订单明细与产品表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_order_detail od
        LEFT JOIN ods_product_master pm ON od.product_id = pm.product_id
        WHERE pm.product_id IS NULL AND od.product_id IS NOT NULL
    """)
    orphan_products = cursor.fetchone()[0]
    if orphan_products > 0:
        issues.append(f"订单明细表有 {orphan_products} 条记录关联不到产品表")
        print(f"  ✗ 发现 {orphan_products} 条订单明细关联不到产品")
    else:
        print(f"  ✓ 订单明细全部关联到产品表")
    
    # 4. 生产工单 -> 生产计划
    print("\n4. 检查生产工单与生产计划的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_production_order po
        LEFT JOIN ods_production_plan pp ON po.plan_id = pp.plan_id
        WHERE pp.plan_id IS NULL AND po.plan_id IS NOT NULL
    """)
    orphan_work_orders = cursor.fetchone()[0]
    if orphan_work_orders > 0:
        issues.append(f"生产工单表有 {orphan_work_orders} 条记录关联不到生产计划")
        print(f"  ✗ 发现 {orphan_work_orders} 条生产工单关联不到生产计划")
    else:
        print(f"  ✓ 生产工单全部关联到生产计划")
    
    # 5. 生产计划 -> 产品表
    print("\n5. 检查生产计划与产品表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_production_plan pp
        LEFT JOIN ods_product_master pm ON pp.product_id = pm.product_id
        WHERE pm.product_id IS NULL AND pp.product_id IS NOT NULL
    """)
    orphan_plans = cursor.fetchone()[0]
    if orphan_plans > 0:
        issues.append(f"生产计划表有 {orphan_plans} 条记录关联不到产品表")
        print(f"  ✗ 发现 {orphan_plans} 条生产计划关联不到产品")
    else:
        print(f"  ✓ 生产计划全部关联到产品表")
    
    # 6. BOM -> 产品表
    print("\n6. 检查BOM与产品表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_bom bom
        LEFT JOIN ods_product_master pm ON bom.product_id = pm.product_id
        WHERE pm.product_id IS NULL AND bom.product_id IS NOT NULL
    """)
    orphan_bom_products = cursor.fetchone()[0]
    if orphan_bom_products > 0:
        issues.append(f"BOM表有 {orphan_bom_products} 条记录关联不到产品表")
        print(f"  ✗ 发现 {orphan_bom_products} 条BOM关联不到产品")
    else:
        print(f"  ✓ BOM全部关联到产品表")
    
    # 7. BOM -> 物料表
    print("\n7. 检查BOM与物料表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_bom bom
        LEFT JOIN ods_material_master mm ON bom.material_id = mm.material_id
        WHERE mm.material_id IS NULL AND bom.material_id IS NOT NULL
    """)
    orphan_bom_materials = cursor.fetchone()[0]
    if orphan_bom_materials > 0:
        issues.append(f"BOM表有 {orphan_bom_materials} 条记录关联不到物料表")
        print(f"  ✗ 发现 {orphan_bom_materials} 条BOM关联不到物料")
    else:
        print(f"  ✓ BOM全部关联到物料表")
    
    # 8. 库存 -> 物料表
    print("\n8. 检查库存与物料表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_inventory inv
        LEFT JOIN ods_material_master mm ON inv.material_id = mm.material_id
        WHERE mm.material_id IS NULL AND inv.material_id IS NOT NULL
    """)
    orphan_inv_materials = cursor.fetchone()[0]
    if orphan_inv_materials > 0:
        issues.append(f"库存表有 {orphan_inv_materials} 条记录关联不到物料表")
        print(f"  ✗ 发现 {orphan_inv_materials} 条库存关联不到物料")
    else:
        print(f"  ✓ 库存全部关联到物料表")
    
    # 9. 库存 -> 仓库表
    print("\n9. 检查库存与仓库表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_inventory inv
        LEFT JOIN ods_warehouse_master wm ON inv.warehouse_id = wm.warehouse_id
        WHERE wm.warehouse_id IS NULL AND inv.warehouse_id IS NOT NULL
    """)
    orphan_inv_warehouses = cursor.fetchone()[0]
    if orphan_inv_warehouses > 0:
        issues.append(f"库存表有 {orphan_inv_warehouses} 条记录关联不到仓库表")
        print(f"  ✗ 发现 {orphan_inv_warehouses} 条库存关联不到仓库")
    else:
        print(f"  ✓ 库存全部关联到仓库表")
    
    # 10. 采购明细 -> 采购订单
    print("\n10. 检查采购明细与采购订单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_purchase_detail pd
        LEFT JOIN ods_purchase_order po ON pd.purchase_id = po.purchase_id
        WHERE po.purchase_id IS NULL
    """)
    orphan_purchase_details = cursor.fetchone()[0]
    if orphan_purchase_details > 0:
        issues.append(f"采购明细表有 {orphan_purchase_details} 条记录关联不到采购订单")
        print(f"  ✗ 发现 {orphan_purchase_details} 条采购明细关联不到采购订单")
    else:
        print(f"  ✓ 采购明细全部关联到采购订单")
    
    # 11. 入库明细 -> 入库单
    print("\n11. 检查入库明细与入库单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_inbound_detail id
        LEFT JOIN ods_inbound_order io ON id.inbound_id = io.inbound_id
        WHERE io.inbound_id IS NULL
    """)
    orphan_inbound_details = cursor.fetchone()[0]
    if orphan_inbound_details > 0:
        issues.append(f"入库明细表有 {orphan_inbound_details} 条记录关联不到入库单")
        print(f"  ✗ 发现 {orphan_inbound_details} 条入库明细关联不到入库单")
    else:
        print(f"  ✓ 入库明细全部关联到入库单")
    
    # 12. 出库明细 -> 出库单
    print("\n12. 检查出库明细与出库单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_outbound_detail od
        LEFT JOIN ods_outbound_order oo ON od.outbound_id = oo.outbound_id
        WHERE oo.outbound_id IS NULL
    """)
    orphan_outbound_details = cursor.fetchone()[0]
    if orphan_outbound_details > 0:
        issues.append(f"出库明细表有 {orphan_outbound_details} 条记录关联不到出库单")
        print(f"  ✗ 发现 {orphan_outbound_details} 条出库明细关联不到出库单")
    else:
        print(f"  ✓ 出库明细全部关联到出库单")
    
    # 13. 设备运行记录 -> 设备表
    print("\n13. 检查设备运行记录与设备表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_equipment_runtime er
        LEFT JOIN ods_equipment_master em ON er.equipment_id = em.equipment_id
        WHERE em.equipment_id IS NULL AND er.equipment_id IS NOT NULL
    """)
    orphan_runtime = cursor.fetchone()[0]
    if orphan_runtime > 0:
        issues.append(f"设备运行记录表有 {orphan_runtime} 条记录关联不到设备表")
        print(f"  ✗ 发现 {orphan_runtime} 条设备运行记录关联不到设备")
    else:
        print(f"  ✓ 设备运行记录全部关联到设备表")
    
    # 14. 质量检验 -> 生产工单
    print("\n14. 检查质量检验与生产工单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_quality_inspection qi
        LEFT JOIN ods_production_order po ON qi.work_order_id = po.work_order_id
        WHERE po.work_order_id IS NULL AND qi.work_order_id IS NOT NULL
    """)
    orphan_inspections = cursor.fetchone()[0]
    if orphan_inspections > 0:
        issues.append(f"质量检验表有 {orphan_inspections} 条记录关联不到生产工单")
        print(f"  ✗ 发现 {orphan_inspections} 条质量检验关联不到生产工单")
    else:
        print(f"  ✓ 质量检验全部关联到生产工单")
    
    # 15. 成本明细 -> 成本中心
    print("\n15. 检查成本明细与成本中心的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_cost_detail cd
        LEFT JOIN ods_cost_center cc ON cd.cost_center_id = cc.cost_center_id
        WHERE cc.cost_center_id IS NULL AND cd.cost_center_id IS NOT NULL
    """)
    orphan_cost_details = cursor.fetchone()[0]
    if orphan_cost_details > 0:
        issues.append(f"成本明细表有 {orphan_cost_details} 条记录关联不到成本中心")
        print(f"  ✗ 发现 {orphan_cost_details} 条成本明细关联不到成本中心")
    else:
        print(f"  ✓ 成本明细全部关联到成本中心")
    
    cursor.close()
    conn.close()
    
    return issues

def check_postgresql_relationships():
    """检查PostgreSQL的关联关系"""
    print("\n" + "=" * 70)
    print("PostgreSQL 关联关系检查")
    print("=" * 70)
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    issues = []
    
    # 1. 订单明细 -> 订单主表
    print("\n1. 检查订单明细与订单主表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_order_detail od
        LEFT JOIN ods_order_master om ON od.order_id = om.order_id
        WHERE om.order_id IS NULL
    """)
    orphan_details = cursor.fetchone()[0]
    if orphan_details > 0:
        issues.append(f"订单明细表有 {orphan_details} 条记录关联不到订单主表")
        print(f"  ✗ 发现 {orphan_details} 条孤立订单明细")
    else:
        print(f"  ✓ 订单明细全部关联到订单主表")
    
    # 2. 订单主表 -> 客户表
    print("\n2. 检查订单主表与客户表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_order_master om
        LEFT JOIN ods_customer_master cm ON om.customer_id = cm.customer_id
        WHERE cm.customer_id IS NULL AND om.customer_id IS NOT NULL
    """)
    orphan_orders = cursor.fetchone()[0]
    if orphan_orders > 0:
        issues.append(f"订单主表有 {orphan_orders} 条记录关联不到客户表")
        print(f"  ✗ 发现 {orphan_orders} 条订单关联不到客户")
    else:
        print(f"  ✓ 订单全部关联到客户表")
    
    # 3. 订单明细 -> 产品表
    print("\n3. 检查订单明细与产品表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_order_detail od
        LEFT JOIN ods_product_master pm ON od.product_id = pm.product_id
        WHERE pm.product_id IS NULL AND od.product_id IS NOT NULL
    """)
    orphan_products = cursor.fetchone()[0]
    if orphan_products > 0:
        issues.append(f"订单明细表有 {orphan_products} 条记录关联不到产品表")
        print(f"  ✗ 发现 {orphan_products} 条订单明细关联不到产品")
    else:
        print(f"  ✓ 订单明细全部关联到产品表")
    
    # 4. 生产工单 -> 生产计划
    print("\n4. 检查生产工单与生产计划的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_production_order po
        LEFT JOIN ods_production_plan pp ON po.plan_id = pp.plan_id
        WHERE pp.plan_id IS NULL AND po.plan_id IS NOT NULL
    """)
    orphan_work_orders = cursor.fetchone()[0]
    if orphan_work_orders > 0:
        issues.append(f"生产工单表有 {orphan_work_orders} 条记录关联不到生产计划")
        print(f"  ✗ 发现 {orphan_work_orders} 条生产工单关联不到生产计划")
    else:
        print(f"  ✓ 生产工单全部关联到生产计划")
    
    # 5. 生产计划 -> 产品表
    print("\n5. 检查生产计划与产品表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_production_plan pp
        LEFT JOIN ods_product_master pm ON pp.product_id = pm.product_id
        WHERE pm.product_id IS NULL AND pp.product_id IS NOT NULL
    """)
    orphan_plans = cursor.fetchone()[0]
    if orphan_plans > 0:
        issues.append(f"生产计划表有 {orphan_plans} 条记录关联不到产品表")
        print(f"  ✗ 发现 {orphan_plans} 条生产计划关联不到产品")
    else:
        print(f"  ✓ 生产计划全部关联到产品表")
    
    # 6. BOM -> 产品表
    print("\n6. 检查BOM与产品表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_bom bom
        LEFT JOIN ods_product_master pm ON bom.product_id = pm.product_id
        WHERE pm.product_id IS NULL AND bom.product_id IS NOT NULL
    """)
    orphan_bom_products = cursor.fetchone()[0]
    if orphan_bom_products > 0:
        issues.append(f"BOM表有 {orphan_bom_products} 条记录关联不到产品表")
        print(f"  ✗ 发现 {orphan_bom_products} 条BOM关联不到产品")
    else:
        print(f"  ✓ BOM全部关联到产品表")
    
    # 7. BOM -> 物料表
    print("\n7. 检查BOM与物料表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_bom bom
        LEFT JOIN ods_material_master mm ON bom.material_id = mm.material_id
        WHERE mm.material_id IS NULL AND bom.material_id IS NOT NULL
    """)
    orphan_bom_materials = cursor.fetchone()[0]
    if orphan_bom_materials > 0:
        issues.append(f"BOM表有 {orphan_bom_materials} 条记录关联不到物料表")
        print(f"  ✗ 发现 {orphan_bom_materials} 条BOM关联不到物料")
    else:
        print(f"  ✓ BOM全部关联到物料表")
    
    # 8. 库存 -> 物料表
    print("\n8. 检查库存与物料表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_inventory inv
        LEFT JOIN ods_material_master mm ON inv.material_id = mm.material_id
        WHERE mm.material_id IS NULL AND inv.material_id IS NOT NULL
    """)
    orphan_inv_materials = cursor.fetchone()[0]
    if orphan_inv_materials > 0:
        issues.append(f"库存表有 {orphan_inv_materials} 条记录关联不到物料表")
        print(f"  ✗ 发现 {orphan_inv_materials} 条库存关联不到物料")
    else:
        print(f"  ✓ 库存全部关联到物料表")
    
    # 9. 库存 -> 仓库表
    print("\n9. 检查库存与仓库表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_inventory inv
        LEFT JOIN ods_warehouse_master wm ON inv.warehouse_id = wm.warehouse_id
        WHERE wm.warehouse_id IS NULL AND inv.warehouse_id IS NOT NULL
    """)
    orphan_inv_warehouses = cursor.fetchone()[0]
    if orphan_inv_warehouses > 0:
        issues.append(f"库存表有 {orphan_inv_warehouses} 条记录关联不到仓库表")
        print(f"  ✗ 发现 {orphan_inv_warehouses} 条库存关联不到仓库")
    else:
        print(f"  ✓ 库存全部关联到仓库表")
    
    # 10. 采购明细 -> 采购订单
    print("\n10. 检查采购明细与采购订单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_purchase_detail pd
        LEFT JOIN ods_purchase_order po ON pd.purchase_id = po.purchase_id
        WHERE po.purchase_id IS NULL
    """)
    orphan_purchase_details = cursor.fetchone()[0]
    if orphan_purchase_details > 0:
        issues.append(f"采购明细表有 {orphan_purchase_details} 条记录关联不到采购订单")
        print(f"  ✗ 发现 {orphan_purchase_details} 条采购明细关联不到采购订单")
    else:
        print(f"  ✓ 采购明细全部关联到采购订单")
    
    # 11. 入库明细 -> 入库单
    print("\n11. 检查入库明细与入库单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_inbound_detail id
        LEFT JOIN ods_inbound_order io ON id.inbound_id = io.inbound_id
        WHERE io.inbound_id IS NULL
    """)
    orphan_inbound_details = cursor.fetchone()[0]
    if orphan_inbound_details > 0:
        issues.append(f"入库明细表有 {orphan_inbound_details} 条记录关联不到入库单")
        print(f"  ✗ 发现 {orphan_inbound_details} 条入库明细关联不到入库单")
    else:
        print(f"  ✓ 入库明细全部关联到入库单")
    
    # 12. 出库明细 -> 出库单
    print("\n12. 检查出库明细与出库单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_outbound_detail od
        LEFT JOIN ods_outbound_order oo ON od.outbound_id = oo.outbound_id
        WHERE oo.outbound_id IS NULL
    """)
    orphan_outbound_details = cursor.fetchone()[0]
    if orphan_outbound_details > 0:
        issues.append(f"出库明细表有 {orphan_outbound_details} 条记录关联不到出库单")
        print(f"  ✗ 发现 {orphan_outbound_details} 条出库明细关联不到出库单")
    else:
        print(f"  ✓ 出库明细全部关联到出库单")
    
    # 13. 设备运行记录 -> 设备表
    print("\n13. 检查设备运行记录与设备表的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_equipment_runtime er
        LEFT JOIN ods_equipment_master em ON er.equipment_id = em.equipment_id
        WHERE em.equipment_id IS NULL AND er.equipment_id IS NOT NULL
    """)
    orphan_runtime = cursor.fetchone()[0]
    if orphan_runtime > 0:
        issues.append(f"设备运行记录表有 {orphan_runtime} 条记录关联不到设备表")
        print(f"  ✗ 发现 {orphan_runtime} 条设备运行记录关联不到设备")
    else:
        print(f"  ✓ 设备运行记录全部关联到设备表")
    
    # 14. 质量检验 -> 生产工单
    print("\n14. 检查质量检验与生产工单的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_quality_inspection qi
        LEFT JOIN ods_production_order po ON qi.work_order_id = po.work_order_id
        WHERE po.work_order_id IS NULL AND qi.work_order_id IS NOT NULL
    """)
    orphan_inspections = cursor.fetchone()[0]
    if orphan_inspections > 0:
        issues.append(f"质量检验表有 {orphan_inspections} 条记录关联不到生产工单")
        print(f"  ✗ 发现 {orphan_inspections} 条质量检验关联不到生产工单")
    else:
        print(f"  ✓ 质量检验全部关联到生产工单")
    
    # 15. 成本明细 -> 成本中心
    print("\n15. 检查成本明细与成本中心的关联...")
    cursor.execute("""
        SELECT COUNT(*) FROM ods_cost_detail cd
        LEFT JOIN ods_cost_center cc ON cd.cost_center_id = cc.cost_center_id
        WHERE cc.cost_center_id IS NULL AND cd.cost_center_id IS NOT NULL
    """)
    orphan_cost_details = cursor.fetchone()[0]
    if orphan_cost_details > 0:
        issues.append(f"成本明细表有 {orphan_cost_details} 条记录关联不到成本中心")
        print(f"  ✗ 发现 {orphan_cost_details} 条成本明细关联不到成本中心")
    else:
        print(f"  ✓ 成本明细全部关联到成本中心")
    
    cursor.close()
    conn.close()
    
    return issues

if __name__ == "__main__":
    mysql_issues = check_mysql_relationships()
    postgresql_issues = check_postgresql_relationships()
    
    print("\n" + "=" * 70)
    print("关联关系检查总结")
    print("=" * 70)
    
    if not mysql_issues and not postgresql_issues:
        print("\n✓ 所有关联关系检查通过！")
        print("  - MySQL: 所有关联关系正确")
        print("  - PostgreSQL: 所有关联关系正确")
    else:
        if mysql_issues:
            print("\n✗ MySQL发现以下问题:")
            for issue in mysql_issues:
                print(f"  - {issue}")
        if postgresql_issues:
            print("\n✗ PostgreSQL发现以下问题:")
            for issue in postgresql_issues:
                print(f"  - {issue}")
    
    print("=" * 70)
