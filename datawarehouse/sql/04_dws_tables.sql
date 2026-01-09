-- DWS层：数据汇总层
-- 基于DWD层数据进行轻度汇总

USE sqlExpert;

-- 1. 订单日汇总表
CREATE TABLE IF NOT EXISTS dws_order_daily (
    stat_date DATE PRIMARY KEY COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    customer_id VARCHAR(50) COMMENT '客户ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    region VARCHAR(100) COMMENT '区域',
    order_count INT COMMENT '订单数量',
    order_amount DECIMAL(18,2) COMMENT '订单金额',
    order_quantity DECIMAL(18,3) COMMENT '订单数量(产品)',
    avg_order_amount DECIMAL(18,2) COMMENT '平均订单金额',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_stat_date (stat_date),
    INDEX idx_customer_id (customer_id),
    INDEX idx_product_id (product_id)
) COMMENT 'DWS-订单日汇总表';

-- 2. 生产日汇总表
CREATE TABLE IF NOT EXISTS dws_production_daily (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    product_id VARCHAR(50) COMMENT '产品ID',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    work_order_count INT COMMENT '工单数量',
    plan_quantity DECIMAL(18,3) COMMENT '计划数量',
    actual_quantity DECIMAL(18,3) COMMENT '实际数量',
    completion_rate DECIMAL(5,2) COMMENT '完成率(%)',
    avg_duration_hours DECIMAL(10,2) COMMENT '平均持续时间(小时)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, product_id, workshop_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_product_id (product_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'DWS-生产日汇总表';

-- 3. 库存日汇总表
CREATE TABLE IF NOT EXISTS dws_inventory_daily (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    material_id VARCHAR(50) COMMENT '物料ID',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    material_category VARCHAR(100) COMMENT '物料类别',
    total_quantity DECIMAL(18,3) COMMENT '总数量',
    available_quantity DECIMAL(18,3) COMMENT '可用数量',
    total_cost DECIMAL(18,2) COMMENT '总成本',
    avg_unit_cost DECIMAL(18,2) COMMENT '平均单位成本',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, material_id, warehouse_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_material_id (material_id),
    INDEX idx_warehouse_id (warehouse_id)
) COMMENT 'DWS-库存日汇总表';

-- 4. 采购日汇总表
CREATE TABLE IF NOT EXISTS dws_purchase_daily (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    supplier_id VARCHAR(50) COMMENT '供应商ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    region VARCHAR(100) COMMENT '区域',
    purchase_count INT COMMENT '采购订单数',
    purchase_quantity DECIMAL(18,3) COMMENT '采购数量',
    purchase_amount DECIMAL(18,2) COMMENT '采购金额',
    avg_unit_price DECIMAL(18,2) COMMENT '平均单价',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, supplier_id, material_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_supplier_id (supplier_id),
    INDEX idx_material_id (material_id)
) COMMENT 'DWS-采购日汇总表';

-- 5. 质量日汇总表
CREATE TABLE IF NOT EXISTS dws_quality_daily (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    product_id VARCHAR(50) COMMENT '产品ID',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    inspection_count INT COMMENT '检验次数',
    sample_quantity INT COMMENT '抽样总数',
    qualified_quantity INT COMMENT '合格总数',
    unqualified_quantity INT COMMENT '不合格总数',
    qualified_rate DECIMAL(5,2) COMMENT '合格率(%)',
    defect_quantity INT COMMENT '缺陷总数',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, product_id, workshop_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_product_id (product_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'DWS-质量日汇总表';

-- 6. 设备运行日汇总表
CREATE TABLE IF NOT EXISTS dws_equipment_runtime_daily (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    equipment_id VARCHAR(50) COMMENT '设备ID',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    equipment_type VARCHAR(50) COMMENT '设备类型',
    total_running_hours DECIMAL(10,2) COMMENT '总运行小时数',
    total_downtime_hours DECIMAL(10,2) COMMENT '总停机小时数',
    utilization_rate DECIMAL(5,2) COMMENT '利用率(%)',
    total_production_quantity DECIMAL(18,3) COMMENT '总产量',
    total_energy_consumption DECIMAL(18,2) COMMENT '总能耗',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, equipment_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_equipment_id (equipment_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'DWS-设备运行日汇总表';

-- 7. 成本日汇总表
CREATE TABLE IF NOT EXISTS dws_cost_daily (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    cost_center_id VARCHAR(50) COMMENT '成本中心ID',
    department_id VARCHAR(50) COMMENT '部门ID',
    cost_type VARCHAR(50) COMMENT '成本类型',
    total_amount DECIMAL(18,2) COMMENT '总金额',
    total_quantity DECIMAL(18,3) COMMENT '总数量',
    avg_unit_cost DECIMAL(18,2) COMMENT '平均单位成本',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, cost_center_id, cost_type),
    INDEX idx_stat_date (stat_date),
    INDEX idx_cost_center_id (cost_center_id),
    INDEX idx_cost_type (cost_type)
) COMMENT 'DWS-成本日汇总表';
