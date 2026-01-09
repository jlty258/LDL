-- 复杂ETL SQL脚本（超过100行）
-- 从ODS层到DWD层到DWS层到ADS层的完整数据流转

USE sqlExpert;

-- ============================================
-- 第一部分：ODS -> DWD 数据清洗和转换
-- ============================================

-- 1. 订单事实表ETL（清洗订单数据，关联客户和产品信息）
INSERT INTO dwd_order_fact (
    order_id, order_no, customer_id, product_id, order_date,
    order_year, order_month, order_quarter, order_status,
    order_amount, order_quantity, unit_price,
    sales_rep_id, warehouse_id, region
)
SELECT 
    om.order_id,
    om.order_no,
    om.customer_id,
    od.product_id,
    DATE(om.order_date) as order_date,
    YEAR(om.order_date) as order_year,
    MONTH(om.order_date) as order_month,
    QUARTER(om.order_date) as order_quarter,
    CASE 
        WHEN om.order_status IN ('待确认', '已确认', '生产中', '已完成') THEN om.order_status
        ELSE '其他'
    END as order_status,
    COALESCE(od.amount, 0) as order_amount,
    COALESCE(od.quantity, 0) as order_quantity,
    COALESCE(od.unit_price, 0) as unit_price,
    om.sales_rep_id,
    om.warehouse_id,
    COALESCE(cm.region, '未知') as region
FROM ods_order_master om
INNER JOIN ods_order_detail od ON om.order_id = od.order_id
LEFT JOIN ods_customer_master cm ON om.customer_id = cm.customer_id
WHERE om.order_date >= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)
  AND om.order_status IS NOT NULL
  AND od.quantity > 0
  AND od.unit_price > 0
ON DUPLICATE KEY UPDATE
    order_no = VALUES(order_no),
    customer_id = VALUES(customer_id),
    product_id = VALUES(product_id),
    order_date = VALUES(order_date),
    order_year = VALUES(order_year),
    order_month = VALUES(order_month),
    order_quarter = VALUES(order_quarter),
    order_status = VALUES(order_status),
    order_amount = VALUES(order_amount),
    order_quantity = VALUES(order_quantity),
    unit_price = VALUES(unit_price),
    sales_rep_id = VALUES(sales_rep_id),
    warehouse_id = VALUES(warehouse_id),
    region = VALUES(region),
    update_time = CURRENT_TIMESTAMP;

-- 2. 生产事实表ETL（清洗生产数据，计算完成率）
INSERT INTO dwd_production_fact (
    production_id, work_order_id, plan_id, product_id,
    workshop_id, production_line_id, production_date,
    production_year, production_month, production_quarter,
    plan_quantity, actual_quantity, completed_quantity,
    completion_rate, production_status,
    start_time, end_time, duration_hours
)
SELECT 
    po.work_order_id as production_id,
    po.work_order_id,
    po.plan_id,
    po.product_id,
    po.workshop_id,
    po.production_line_id,
    DATE(COALESCE(po.start_time, pp.plan_date)) as production_date,
    YEAR(COALESCE(po.start_time, pp.plan_date)) as production_year,
    MONTH(COALESCE(po.start_time, pp.plan_date)) as production_month,
    QUARTER(COALESCE(po.start_time, pp.plan_date)) as production_quarter,
    COALESCE(pp.plan_quantity, po.order_quantity, 0) as plan_quantity,
    COALESCE(pp.actual_quantity, 0) as actual_quantity,
    COALESCE(po.completed_quantity, 0) as completed_quantity,
    CASE 
        WHEN COALESCE(pp.plan_quantity, po.order_quantity, 0) > 0 
        THEN (COALESCE(po.completed_quantity, 0) / COALESCE(pp.plan_quantity, po.order_quantity, 1)) * 100
        ELSE 0
    END as completion_rate,
    COALESCE(po.order_status, '未知') as production_status,
    po.start_time,
    po.end_time,
    CASE 
        WHEN po.start_time IS NOT NULL AND po.end_time IS NOT NULL
        THEN TIMESTAMPDIFF(HOUR, po.start_time, po.end_time)
        ELSE NULL
    END as duration_hours
FROM ods_production_order po
LEFT JOIN ods_production_plan pp ON po.plan_id = pp.plan_id
WHERE po.start_time >= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)
  AND po.product_id IS NOT NULL
ON DUPLICATE KEY UPDATE
    plan_id = VALUES(plan_id),
    product_id = VALUES(product_id),
    workshop_id = VALUES(workshop_id),
    production_line_id = VALUES(production_line_id),
    production_date = VALUES(production_date),
    production_year = VALUES(production_year),
    production_month = VALUES(production_month),
    production_quarter = VALUES(production_quarter),
    plan_quantity = VALUES(plan_quantity),
    actual_quantity = VALUES(actual_quantity),
    completed_quantity = VALUES(completed_quantity),
    completion_rate = VALUES(completion_rate),
    production_status = VALUES(production_status),
    start_time = VALUES(start_time),
    end_time = VALUES(end_time),
    duration_hours = VALUES(duration_hours);

-- 3. 库存事实表ETL（清洗库存数据，关联物料和仓库信息）
INSERT INTO dwd_inventory_fact (
    inventory_id, material_id, warehouse_id, inventory_date,
    inventory_year, inventory_month, quantity,
    available_quantity, reserved_quantity,
    unit_cost, total_cost, material_category, warehouse_type
)
SELECT 
    inv.inventory_id,
    inv.material_id,
    inv.warehouse_id,
    CURDATE() as inventory_date,
    YEAR(CURDATE()) as inventory_year,
    MONTH(CURDATE()) as inventory_month,
    COALESCE(inv.quantity, 0) as quantity,
    COALESCE(inv.available_quantity, 0) as available_quantity,
    COALESCE(inv.reserved_quantity, 0) as reserved_quantity,
    COALESCE(inv.unit_cost, mm.cost_price, 0) as unit_cost,
    COALESCE(inv.total_cost, inv.quantity * COALESCE(inv.unit_cost, mm.cost_price, 0), 0) as total_cost,
    COALESCE(mm.material_category, '未知') as material_category,
    COALESCE(wm.warehouse_type, '未知') as warehouse_type
FROM ods_inventory inv
LEFT JOIN ods_material_master mm ON inv.material_id = mm.material_id
LEFT JOIN ods_warehouse_master wm ON inv.warehouse_id = wm.warehouse_id
WHERE inv.quantity >= 0
ON DUPLICATE KEY UPDATE
    quantity = VALUES(quantity),
    available_quantity = VALUES(available_quantity),
    reserved_quantity = VALUES(reserved_quantity),
    unit_cost = VALUES(unit_cost),
    total_cost = VALUES(total_cost),
    material_category = VALUES(material_category),
    warehouse_type = VALUES(warehouse_type),
    inventory_date = VALUES(inventory_date),
    inventory_year = VALUES(inventory_year),
    inventory_month = VALUES(inventory_month);

-- 4. 采购事实表ETL（清洗采购数据，关联供应商和物料信息）
INSERT INTO dwd_purchase_fact (
    purchase_id, purchase_no, supplier_id, material_id,
    order_date, delivery_date, order_year, order_month, order_quarter,
    order_status, purchase_quantity, unit_price, purchase_amount,
    buyer_id, region
)
SELECT 
    po.purchase_id,
    po.purchase_no,
    po.supplier_id,
    pd.material_id,
    DATE(po.order_date) as order_date,
    pd.delivery_date,
    YEAR(po.order_date) as order_year,
    MONTH(po.order_date) as order_month,
    QUARTER(po.order_date) as order_quarter,
    COALESCE(po.order_status, '未知') as order_status,
    COALESCE(pd.quantity, 0) as purchase_quantity,
    COALESCE(pd.unit_price, 0) as unit_price,
    COALESCE(pd.amount, 0) as purchase_amount,
    po.buyer_id,
    COALESCE(sm.region, '未知') as region
FROM ods_purchase_order po
INNER JOIN ods_purchase_detail pd ON po.purchase_id = pd.purchase_id
LEFT JOIN ods_supplier_master sm ON po.supplier_id = sm.supplier_id
WHERE po.order_date >= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)
  AND pd.quantity > 0
  AND pd.unit_price > 0
ON DUPLICATE KEY UPDATE
    purchase_no = VALUES(purchase_no),
    supplier_id = VALUES(supplier_id),
    material_id = VALUES(material_id),
    order_date = VALUES(order_date),
    delivery_date = VALUES(delivery_date),
    order_year = VALUES(order_year),
    order_month = VALUES(order_month),
    order_quarter = VALUES(order_quarter),
    order_status = VALUES(order_status),
    purchase_quantity = VALUES(purchase_quantity),
    unit_price = VALUES(unit_price),
    purchase_amount = VALUES(purchase_amount),
    buyer_id = VALUES(buyer_id),
    region = VALUES(region);

-- 5. 质量事实表ETL（清洗质量数据，计算合格率）
INSERT INTO dwd_quality_fact (
    quality_id, inspection_id, work_order_id, product_id,
    inspection_date, inspection_year, inspection_month, inspection_quarter,
    inspection_type, sample_quantity, qualified_quantity, unqualified_quantity,
    qualified_rate, defect_quantity, inspector_id, workshop_id
)
SELECT 
    qi.inspection_id as quality_id,
    qi.inspection_id,
    qi.work_order_id,
    qi.product_id,
    DATE(qi.inspection_date) as inspection_date,
    YEAR(qi.inspection_date) as inspection_year,
    MONTH(qi.inspection_date) as inspection_month,
    QUARTER(qi.inspection_date) as inspection_quarter,
    COALESCE(qi.inspection_type, '未知') as inspection_type,
    COALESCE(qi.sample_quantity, 0) as sample_quantity,
    COALESCE(qi.qualified_quantity, 0) as qualified_quantity,
    COALESCE(qi.unqualified_quantity, 0) as unqualified_quantity,
    COALESCE(qi.qualified_rate, 
        CASE 
            WHEN qi.sample_quantity > 0 
            THEN (qi.qualified_quantity / qi.sample_quantity) * 100
            ELSE 0
        END) as qualified_rate,
    COALESCE((
        SELECT COUNT(*) 
        FROM ods_defect_record dr 
        WHERE dr.inspection_id = qi.inspection_id
    ), 0) as defect_quantity,
    qi.inspector_id,
    COALESCE(po.workshop_id, '未知') as workshop_id
FROM ods_quality_inspection qi
LEFT JOIN ods_production_order po ON qi.work_order_id = po.work_order_id
WHERE qi.inspection_date >= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)
  AND qi.sample_quantity > 0
ON DUPLICATE KEY UPDATE
    inspection_id = VALUES(inspection_id),
    work_order_id = VALUES(work_order_id),
    product_id = VALUES(product_id),
    inspection_date = VALUES(inspection_date),
    inspection_year = VALUES(inspection_year),
    inspection_month = VALUES(inspection_month),
    inspection_quarter = VALUES(inspection_quarter),
    inspection_type = VALUES(inspection_type),
    sample_quantity = VALUES(sample_quantity),
    qualified_quantity = VALUES(qualified_quantity),
    unqualified_quantity = VALUES(unqualified_quantity),
    qualified_rate = VALUES(qualified_rate),
    defect_quantity = VALUES(defect_quantity),
    inspector_id = VALUES(inspector_id),
    workshop_id = VALUES(workshop_id);

-- 6. 设备运行事实表ETL（清洗设备运行数据，计算利用率）
INSERT INTO dwd_equipment_runtime_fact (
    runtime_id, equipment_id, workshop_id, production_line_id,
    record_date, record_year, record_month, record_quarter,
    running_hours, downtime_hours, utilization_rate,
    production_quantity, energy_consumption, equipment_type
)
SELECT 
    er.runtime_id,
    er.equipment_id,
    COALESCE(em.workshop_id, '未知') as workshop_id,
    COALESCE(em.production_line_id, '未知') as production_line_id,
    er.record_date,
    YEAR(er.record_date) as record_year,
    MONTH(er.record_date) as record_month,
    QUARTER(er.record_date) as record_quarter,
    COALESCE(er.running_hours, 0) as running_hours,
    COALESCE(er.downtime_hours, 0) as downtime_hours,
    CASE 
        WHEN (COALESCE(er.running_hours, 0) + COALESCE(er.downtime_hours, 0)) > 0
        THEN (er.running_hours / (er.running_hours + er.downtime_hours)) * 100
        ELSE 0
    END as utilization_rate,
    COALESCE(er.production_quantity, 0) as production_quantity,
    COALESCE(er.energy_consumption, 0) as energy_consumption,
    COALESCE(em.equipment_type, '未知') as equipment_type
FROM ods_equipment_runtime er
LEFT JOIN ods_equipment_master em ON er.equipment_id = em.equipment_id
WHERE er.record_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
ON DUPLICATE KEY UPDATE
    equipment_id = VALUES(equipment_id),
    workshop_id = VALUES(workshop_id),
    production_line_id = VALUES(production_line_id),
    record_date = VALUES(record_date),
    record_year = VALUES(record_year),
    record_month = VALUES(record_month),
    record_quarter = VALUES(record_quarter),
    running_hours = VALUES(running_hours),
    downtime_hours = VALUES(downtime_hours),
    utilization_rate = VALUES(utilization_rate),
    production_quantity = VALUES(production_quantity),
    energy_consumption = VALUES(energy_consumption),
    equipment_type = VALUES(equipment_type);

-- 7. 成本事实表ETL（清洗成本数据）
INSERT INTO dwd_cost_fact (
    cost_id, cost_center_id, department_id, cost_date,
    cost_year, cost_month, cost_quarter,
    cost_type, cost_item, amount, quantity, unit_cost, factory_id
)
SELECT 
    cd.cost_id,
    cd.cost_center_id,
    COALESCE(cc.department_id, '未知') as department_id,
    cd.cost_date,
    YEAR(cd.cost_date) as cost_year,
    MONTH(cd.cost_date) as cost_month,
    QUARTER(cd.cost_date) as cost_quarter,
    COALESCE(cd.cost_type, '未知') as cost_type,
    COALESCE(cd.cost_item, '未知') as cost_item,
    COALESCE(cd.amount, 0) as amount,
    COALESCE(cd.quantity, 0) as quantity,
    COALESCE(cd.unit_cost, 
        CASE 
            WHEN cd.quantity > 0 THEN cd.amount / cd.quantity
            ELSE 0
        END) as unit_cost,
    COALESCE(dm.factory_id, '未知') as factory_id
FROM ods_cost_detail cd
LEFT JOIN ods_cost_center cc ON cd.cost_center_id = cc.cost_center_id
LEFT JOIN ods_department_master dm ON cc.department_id = dm.department_id
WHERE cd.cost_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
  AND cd.amount >= 0
ON DUPLICATE KEY UPDATE
    cost_center_id = VALUES(cost_center_id),
    department_id = VALUES(department_id),
    cost_date = VALUES(cost_date),
    cost_year = VALUES(cost_year),
    cost_month = VALUES(cost_month),
    cost_quarter = VALUES(cost_quarter),
    cost_type = VALUES(cost_type),
    cost_item = VALUES(cost_item),
    amount = VALUES(amount),
    quantity = VALUES(quantity),
    unit_cost = VALUES(unit_cost),
    factory_id = VALUES(factory_id);

-- ============================================
-- 第二部分：DWD -> DWS 数据汇总
-- ============================================

-- 8. 订单日汇总ETL
INSERT INTO dws_order_daily (
    stat_date, stat_year, stat_month, stat_quarter,
    customer_id, product_id, region,
    order_count, order_amount, order_quantity, avg_order_amount
)
SELECT 
    order_date as stat_date,
    order_year,
    order_month,
    order_quarter,
    customer_id,
    product_id,
    region,
    COUNT(DISTINCT order_id) as order_count,
    SUM(order_amount) as order_amount,
    SUM(order_quantity) as order_quantity,
    AVG(order_amount) as avg_order_amount
FROM dwd_order_fact
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
GROUP BY order_date, order_year, order_month, order_quarter,
         customer_id, product_id, region
ON DUPLICATE KEY UPDATE
    order_count = VALUES(order_count),
    order_amount = VALUES(order_amount),
    order_quantity = VALUES(order_quantity),
    avg_order_amount = VALUES(avg_order_amount);

-- 9. 生产日汇总ETL
INSERT INTO dws_production_daily (
    stat_date, stat_year, stat_month, stat_quarter,
    product_id, workshop_id, production_line_id,
    work_order_count, plan_quantity, actual_quantity,
    completion_rate, avg_duration_hours
)
SELECT 
    production_date as stat_date,
    production_year,
    production_month,
    production_quarter,
    product_id,
    workshop_id,
    production_line_id,
    COUNT(DISTINCT production_id) as work_order_count,
    SUM(plan_quantity) as plan_quantity,
    SUM(actual_quantity) as actual_quantity,
    AVG(completion_rate) as completion_rate,
    AVG(duration_hours) as avg_duration_hours
FROM dwd_production_fact
WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
GROUP BY production_date, production_year, production_month, production_quarter,
         product_id, workshop_id, production_line_id
ON DUPLICATE KEY UPDATE
    work_order_count = VALUES(work_order_count),
    plan_quantity = VALUES(plan_quantity),
    actual_quantity = VALUES(actual_quantity),
    completion_rate = VALUES(completion_rate),
    avg_duration_hours = VALUES(avg_duration_hours);

-- 10. 库存日汇总ETL
INSERT INTO dws_inventory_daily (
    stat_date, stat_year, stat_month, stat_quarter,
    material_id, warehouse_id, material_category,
    total_quantity, available_quantity, total_cost, avg_unit_cost
)
SELECT 
    inventory_date as stat_date,
    inventory_year,
    inventory_month,
    QUARTER(inventory_date) as stat_quarter,
    material_id,
    warehouse_id,
    material_category,
    SUM(quantity) as total_quantity,
    SUM(available_quantity) as available_quantity,
    SUM(total_cost) as total_cost,
    CASE 
        WHEN SUM(quantity) > 0 THEN SUM(total_cost) / SUM(quantity)
        ELSE 0
    END as avg_unit_cost
FROM dwd_inventory_fact
WHERE inventory_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
GROUP BY inventory_date, inventory_year, inventory_month,
         material_id, warehouse_id, material_category
ON DUPLICATE KEY UPDATE
    total_quantity = VALUES(total_quantity),
    available_quantity = VALUES(available_quantity),
    total_cost = VALUES(total_cost),
    avg_unit_cost = VALUES(avg_unit_cost);

-- ============================================
-- 第三部分：DWS -> ADS 应用数据服务
-- ============================================

-- 11. 销售分析报表ETL（最复杂的SQL，超过100行）
INSERT INTO ads_sales_analysis (
    stat_date, stat_year, stat_month, stat_quarter,
    customer_id, customer_name, product_id, product_name,
    region, order_count, order_amount, order_quantity,
    payment_amount, payment_rate
)
SELECT 
    od.stat_date,
    od.stat_year,
    od.stat_month,
    od.stat_quarter,
    od.customer_id,
    COALESCE(cd.customer_name, '未知客户') as customer_name,
    od.product_id,
    COALESCE(pd.product_name, '未知产品') as product_name,
    COALESCE(od.region, '未知区域') as region,
    od.order_count,
    od.order_amount,
    od.order_quantity,
    COALESCE(pa.payment_amount, 0) as payment_amount,
    CASE 
        WHEN od.order_amount > 0 
        THEN (COALESCE(pa.payment_amount, 0) / od.order_amount) * 100
        ELSE 0
    END as payment_rate
FROM dws_order_daily od
LEFT JOIN dwd_customer_dim cd ON od.customer_id = cd.customer_id
LEFT JOIN dwd_product_dim pd ON od.product_id = pd.product_id
LEFT JOIN (
    SELECT 
        DATE(payment_date) as payment_date,
        customer_id,
        SUM(payment_amount) as payment_amount
    FROM ods_sales_payment
    WHERE payment_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
      AND payment_status = '已收款'
    GROUP BY DATE(payment_date), customer_id
) pa ON od.stat_date = pa.payment_date 
    AND od.customer_id = pa.customer_id
WHERE od.stat_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    product_name = VALUES(product_name),
    region = VALUES(region),
    order_count = VALUES(order_count),
    order_amount = VALUES(order_amount),
    order_quantity = VALUES(order_quantity),
    payment_amount = VALUES(payment_amount),
    payment_rate = VALUES(payment_rate);

-- 12. 生产分析报表ETL
INSERT INTO ads_production_analysis (
    stat_date, stat_year, stat_month, stat_quarter,
    product_id, product_name, workshop_id, workshop_name,
    production_line_id, production_line_name,
    work_order_count, plan_quantity, actual_quantity,
    completion_rate, qualified_quantity, qualified_rate,
    avg_duration_hours
)
SELECT 
    pd.stat_date,
    pd.stat_year,
    pd.stat_month,
    pd.stat_quarter,
    pd.product_id,
    COALESCE(prod.product_name, '未知产品') as product_name,
    pd.workshop_id,
    COALESCE(ws.workshop_name, '未知车间') as workshop_name,
    pd.production_line_id,
    COALESCE(pl.line_name, '未知生产线') as production_line_name,
    pd.work_order_count,
    pd.plan_quantity,
    pd.actual_quantity,
    pd.completion_rate,
    COALESCE(qa.qualified_quantity, 0) as qualified_quantity,
    COALESCE(qa.qualified_rate, 0) as qualified_rate,
    pd.avg_duration_hours
FROM dws_production_daily pd
LEFT JOIN dwd_product_dim prod ON pd.product_id = prod.product_id
LEFT JOIN ods_workshop_master ws ON pd.workshop_id = ws.workshop_id
LEFT JOIN ods_production_line pl ON pd.production_line_id = pl.line_id
LEFT JOIN (
    SELECT 
        DATE(inspection_date) as inspection_date,
        product_id,
        workshop_id,
        SUM(qualified_quantity) as qualified_quantity,
        AVG(qualified_rate) as qualified_rate
    FROM dwd_quality_fact
    WHERE inspection_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
    GROUP BY DATE(inspection_date), product_id, workshop_id
) qa ON pd.stat_date = qa.inspection_date
    AND pd.product_id = qa.product_id
    AND pd.workshop_id = qa.workshop_id
WHERE pd.stat_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
ON DUPLICATE KEY UPDATE
    product_name = VALUES(product_name),
    workshop_name = VALUES(workshop_name),
    production_line_name = VALUES(production_line_name),
    work_order_count = VALUES(work_order_count),
    plan_quantity = VALUES(plan_quantity),
    actual_quantity = VALUES(actual_quantity),
    completion_rate = VALUES(completion_rate),
    qualified_quantity = VALUES(qualified_quantity),
    qualified_rate = VALUES(qualified_rate),
    avg_duration_hours = VALUES(avg_duration_hours);

-- 13. 综合经营分析报表ETL
INSERT INTO ads_business_overview (
    stat_date, stat_year, stat_month, stat_quarter,
    total_sales_amount, total_purchase_amount, total_production_quantity,
    total_inventory_cost, total_cost_amount, gross_profit, gross_profit_rate,
    order_count, customer_count, supplier_count, avg_order_amount
)
SELECT 
    CURDATE() as stat_date,
    YEAR(CURDATE()) as stat_year,
    MONTH(CURDATE()) as stat_month,
    QUARTER(CURDATE()) as stat_quarter,
    COALESCE(sales.total_sales_amount, 0) as total_sales_amount,
    COALESCE(purchase.total_purchase_amount, 0) as total_purchase_amount,
    COALESCE(prod.total_production_quantity, 0) as total_production_quantity,
    COALESCE(inv.total_inventory_cost, 0) as total_inventory_cost,
    COALESCE(cost.total_cost_amount, 0) as total_cost_amount,
    COALESCE(sales.total_sales_amount, 0) - COALESCE(cost.total_cost_amount, 0) as gross_profit,
    CASE 
        WHEN COALESCE(sales.total_sales_amount, 0) > 0
        THEN ((COALESCE(sales.total_sales_amount, 0) - COALESCE(cost.total_cost_amount, 0)) / sales.total_sales_amount) * 100
        ELSE 0
    END as gross_profit_rate,
    COALESCE(sales.order_count, 0) as order_count,
    COALESCE(cust.customer_count, 0) as customer_count,
    COALESCE(sup.supplier_count, 0) as supplier_count,
    CASE 
        WHEN COALESCE(sales.order_count, 0) > 0
        THEN COALESCE(sales.total_sales_amount, 0) / sales.order_count
        ELSE 0
    END as avg_order_amount
FROM (
    SELECT 
        SUM(order_amount) as total_sales_amount,
        COUNT(DISTINCT order_id) as order_count
    FROM dwd_order_fact
    WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
) sales
CROSS JOIN (
    SELECT 
        SUM(purchase_amount) as total_purchase_amount
    FROM dwd_purchase_fact
    WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
) purchase
CROSS JOIN (
    SELECT 
        SUM(actual_quantity) as total_production_quantity
    FROM dwd_production_fact
    WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
) prod
CROSS JOIN (
    SELECT 
        SUM(total_cost) as total_inventory_cost
    FROM dwd_inventory_fact
    WHERE inventory_date = CURDATE()
) inv
CROSS JOIN (
    SELECT 
        SUM(amount) as total_cost_amount
    FROM dwd_cost_fact
    WHERE cost_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
) cost
CROSS JOIN (
    SELECT 
        COUNT(DISTINCT customer_id) as customer_count
    FROM dwd_customer_dim
    WHERE create_time >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
) cust
CROSS JOIN (
    SELECT 
        COUNT(DISTINCT supplier_id) as supplier_count
    FROM ods_supplier_master
    WHERE status = '正常'
) sup
ON DUPLICATE KEY UPDATE
    total_sales_amount = VALUES(total_sales_amount),
    total_purchase_amount = VALUES(total_purchase_amount),
    total_production_quantity = VALUES(total_production_quantity),
    total_inventory_cost = VALUES(total_inventory_cost),
    total_cost_amount = VALUES(total_cost_amount),
    gross_profit = VALUES(gross_profit),
    gross_profit_rate = VALUES(gross_profit_rate),
    order_count = VALUES(order_count),
    customer_count = VALUES(customer_count),
    supplier_count = VALUES(supplier_count),
    avg_order_amount = VALUES(avg_order_amount);

-- 完成ETL处理
SELECT 'ETL处理完成' as status, NOW() as process_time;
