#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
展示ODS表之间的关联关系示例
"""

import mysql.connector

MYSQL_CONFIG = {
    'host': 'mysql-db',
    'port': 3306,
    'user': 'sqluser',
    'password': 'sqlpass123',
    'database': 'sqlExpert'
}

def show_relationship_examples():
    """展示关联关系示例"""
    print("=" * 70)
    print("ODS表关联关系示例展示")
    print("=" * 70)
    
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    # 1. 订单 -> 客户 -> 订单明细 -> 产品
    print("\n【示例1】订单完整关联链：订单 -> 客户 -> 订单明细 -> 产品")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            om.order_id,
            om.order_no,
            cm.customer_name,
            COUNT(od.detail_id) as detail_count,
            GROUP_CONCAT(pm.product_name SEPARATOR ', ') as products
        FROM ods_order_master om
        JOIN ods_customer_master cm ON om.customer_id = cm.customer_id
        LEFT JOIN ods_order_detail od ON om.order_id = od.order_id
        LEFT JOIN ods_product_master pm ON od.product_id = pm.product_id
        GROUP BY om.order_id, om.order_no, cm.customer_name
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  订单: {row[1]} | 客户: {row[2]} | 明细数: {row[3]} | 产品: {row[4][:50]}")
    
    # 2. 生产计划 -> 生产工单 -> 产品
    print("\n【示例2】生产关联链：生产计划 -> 生产工单 -> 产品")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            pp.plan_no,
            pp.plan_quantity,
            po.work_order_no,
            po.order_quantity,
            pm.product_name
        FROM ods_production_plan pp
        JOIN ods_production_order po ON pp.plan_id = po.plan_id
        JOIN ods_product_master pm ON pp.product_id = pm.product_id
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  计划: {row[0]} ({row[1]}) -> 工单: {row[2]} ({row[3]}) -> 产品: {row[4]}")
    
    # 3. BOM -> 产品 -> 物料
    print("\n【示例3】BOM关联链：产品 -> BOM -> 物料")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            pm.product_name,
            bom.quantity as bom_quantity,
            mm.material_name,
            bom.loss_rate
        FROM ods_bom bom
        JOIN ods_product_master pm ON bom.product_id = pm.product_id
        JOIN ods_material_master mm ON bom.material_id = mm.material_id
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  产品: {row[0]} -> 物料: {row[2]} (用量: {row[1]}, 损耗率: {row[3]}%)")
    
    # 4. 库存 -> 物料 -> 仓库
    print("\n【示例4】库存关联链：库存 -> 物料 -> 仓库")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            mm.material_name,
            wm.warehouse_name,
            inv.quantity,
            inv.available_quantity,
            inv.total_cost
        FROM ods_inventory inv
        JOIN ods_material_master mm ON inv.material_id = mm.material_id
        JOIN ods_warehouse_master wm ON inv.warehouse_id = wm.warehouse_id
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  物料: {row[0]} | 仓库: {row[1]} | 数量: {row[2]} | 可用: {row[3]} | 成本: {row[4]}")
    
    # 5. 采购订单 -> 采购明细 -> 物料
    print("\n【示例5】采购关联链：采购订单 -> 采购明细 -> 物料")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            po.purchase_no,
            sm.supplier_name,
            COUNT(pd.detail_id) as detail_count,
            SUM(pd.amount) as total_amount
        FROM ods_purchase_order po
        JOIN ods_supplier_master sm ON po.supplier_id = sm.supplier_id
        LEFT JOIN ods_purchase_detail pd ON po.purchase_id = pd.purchase_id
        GROUP BY po.purchase_no, sm.supplier_name
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  采购单: {row[0]} | 供应商: {row[1]} | 明细数: {row[2]} | 总金额: {row[3]}")
    
    # 6. 入库单 -> 入库明细 -> 物料
    print("\n【示例6】入库关联链：入库单 -> 入库明细 -> 物料")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            io.inbound_no,
            io.inbound_type,
            wm.warehouse_name,
            COUNT(id.detail_id) as detail_count,
            SUM(id.amount) as total_amount
        FROM ods_inbound_order io
        JOIN ods_warehouse_master wm ON io.warehouse_id = wm.warehouse_id
        LEFT JOIN ods_inbound_detail id ON io.inbound_id = id.inbound_id
        GROUP BY io.inbound_no, io.inbound_type, wm.warehouse_name
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  入库单: {row[0]} | 类型: {row[1]} | 仓库: {row[2]} | 明细数: {row[3]} | 总金额: {row[4]}")
    
    # 7. 设备运行记录 -> 设备 -> 生产线
    print("\n【示例7】设备关联链：设备运行记录 -> 设备 -> 生产线")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            em.equipment_name,
            pl.line_name,
            er.record_date,
            er.running_hours,
            er.production_quantity
        FROM ods_equipment_runtime er
        JOIN ods_equipment_master em ON er.equipment_id = em.equipment_id
        JOIN ods_production_line pl ON em.production_line_id = pl.line_id
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  设备: {row[0]} | 生产线: {row[1]} | 日期: {row[2]} | 运行: {row[3]}h | 产量: {row[4]}")
    
    # 8. 质量检验 -> 生产工单 -> 产品
    print("\n【示例8】质量关联链：质量检验 -> 生产工单 -> 产品")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            qi.inspection_no,
            po.work_order_no,
            pm.product_name,
            qi.qualified_rate,
            qi.inspection_result
        FROM ods_quality_inspection qi
        JOIN ods_production_order po ON qi.work_order_id = po.work_order_id
        JOIN ods_product_master pm ON qi.product_id = pm.product_id
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  检验单: {row[0]} | 工单: {row[1]} | 产品: {row[2]} | 合格率: {row[3]}% | 结果: {row[4]}")
    
    # 9. 成本明细 -> 成本中心 -> 部门
    print("\n【示例9】成本关联链：成本明细 -> 成本中心 -> 部门")
    print("-" * 70)
    cursor.execute("""
        SELECT 
            cc.cost_center_name,
            dm.department_name,
            cd.cost_type,
            SUM(cd.amount) as total_cost
        FROM ods_cost_detail cd
        JOIN ods_cost_center cc ON cd.cost_center_id = cc.cost_center_id
        JOIN ods_department_master dm ON cc.department_id = dm.department_id
        GROUP BY cc.cost_center_name, dm.department_name, cd.cost_type
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"  成本中心: {row[0]} | 部门: {row[1]} | 类型: {row[2]} | 总成本: {row[3]}")
    
    # 10. 统计关联完整性
    print("\n【统计】主要关联关系完整性统计")
    print("-" * 70)
    
    relationships = [
        ("订单明细 -> 订单主表", "SELECT COUNT(*) FROM ods_order_detail od JOIN ods_order_master om ON od.order_id = om.order_id"),
        ("订单主表 -> 客户表", "SELECT COUNT(*) FROM ods_order_master om JOIN ods_customer_master cm ON om.customer_id = cm.customer_id"),
        ("订单明细 -> 产品表", "SELECT COUNT(*) FROM ods_order_detail od JOIN ods_product_master pm ON od.product_id = pm.product_id"),
        ("生产工单 -> 生产计划", "SELECT COUNT(*) FROM ods_production_order po JOIN ods_production_plan pp ON po.plan_id = pp.plan_id"),
        ("BOM -> 产品表", "SELECT COUNT(*) FROM ods_bom bom JOIN ods_product_master pm ON bom.product_id = pm.product_id"),
        ("BOM -> 物料表", "SELECT COUNT(*) FROM ods_bom bom JOIN ods_material_master mm ON bom.material_id = mm.material_id"),
        ("库存 -> 物料表", "SELECT COUNT(*) FROM ods_inventory inv JOIN ods_material_master mm ON inv.material_id = mm.material_id"),
        ("库存 -> 仓库表", "SELECT COUNT(*) FROM ods_inventory inv JOIN ods_warehouse_master wm ON inv.warehouse_id = wm.warehouse_id"),
    ]
    
    for name, sql in relationships:
        cursor.execute(sql)
        count = cursor.fetchone()[0]
        print(f"  ✓ {name:30} {count:>6} 条有效关联")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("关联关系展示完成！")
    print("=" * 70)

if __name__ == "__main__":
    show_relationship_examples()
