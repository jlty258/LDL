-- ADS层：应用数据服务层
-- 面向应用的数据集市

USE sqlExpert;

-- 1. 销售分析报表
CREATE TABLE IF NOT EXISTS ads_sales_analysis (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    customer_id VARCHAR(50) COMMENT '客户ID',
    customer_name VARCHAR(200) COMMENT '客户名称',
    product_id VARCHAR(50) COMMENT '产品ID',
    product_name VARCHAR(200) COMMENT '产品名称',
    region VARCHAR(100) COMMENT '区域',
    order_count INT COMMENT '订单数量',
    order_amount DECIMAL(18,2) COMMENT '订单金额',
    order_quantity DECIMAL(18,3) COMMENT '订单数量(产品)',
    payment_amount DECIMAL(18,2) COMMENT '回款金额',
    payment_rate DECIMAL(5,2) COMMENT '回款率(%)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, customer_id, product_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_customer_id (customer_id),
    INDEX idx_product_id (product_id)
) COMMENT 'ADS-销售分析报表';

-- 2. 生产分析报表
CREATE TABLE IF NOT EXISTS ads_production_analysis (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    product_id VARCHAR(50) COMMENT '产品ID',
    product_name VARCHAR(200) COMMENT '产品名称',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    workshop_name VARCHAR(200) COMMENT '车间名称',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    production_line_name VARCHAR(200) COMMENT '生产线名称',
    work_order_count INT COMMENT '工单数量',
    plan_quantity DECIMAL(18,3) COMMENT '计划数量',
    actual_quantity DECIMAL(18,3) COMMENT '实际数量',
    completion_rate DECIMAL(5,2) COMMENT '完成率(%)',
    qualified_quantity DECIMAL(18,3) COMMENT '合格数量',
    qualified_rate DECIMAL(5,2) COMMENT '合格率(%)',
    avg_duration_hours DECIMAL(10,2) COMMENT '平均持续时间(小时)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, product_id, workshop_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_product_id (product_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'ADS-生产分析报表';

-- 3. 库存分析报表
CREATE TABLE IF NOT EXISTS ads_inventory_analysis (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    material_id VARCHAR(50) COMMENT '物料ID',
    material_name VARCHAR(200) COMMENT '物料名称',
    material_category VARCHAR(100) COMMENT '物料类别',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    warehouse_name VARCHAR(200) COMMENT '仓库名称',
    total_quantity DECIMAL(18,3) COMMENT '总数量',
    available_quantity DECIMAL(18,3) COMMENT '可用数量',
    total_cost DECIMAL(18,2) COMMENT '总成本',
    turnover_days INT COMMENT '周转天数',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, material_id, warehouse_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_material_id (material_id),
    INDEX idx_warehouse_id (warehouse_id)
) COMMENT 'ADS-库存分析报表';

-- 4. 采购分析报表
CREATE TABLE IF NOT EXISTS ads_purchase_analysis (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    supplier_id VARCHAR(50) COMMENT '供应商ID',
    supplier_name VARCHAR(200) COMMENT '供应商名称',
    material_id VARCHAR(50) COMMENT '物料ID',
    material_name VARCHAR(200) COMMENT '物料名称',
    region VARCHAR(100) COMMENT '区域',
    purchase_count INT COMMENT '采购订单数',
    purchase_quantity DECIMAL(18,3) COMMENT '采购数量',
    purchase_amount DECIMAL(18,2) COMMENT '采购金额',
    avg_unit_price DECIMAL(18,2) COMMENT '平均单价',
    on_time_delivery_rate DECIMAL(5,2) COMMENT '准时交货率(%)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, supplier_id, material_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_supplier_id (supplier_id),
    INDEX idx_material_id (material_id)
) COMMENT 'ADS-采购分析报表';

-- 5. 质量分析报表
CREATE TABLE IF NOT EXISTS ads_quality_analysis (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    product_id VARCHAR(50) COMMENT '产品ID',
    product_name VARCHAR(200) COMMENT '产品名称',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    workshop_name VARCHAR(200) COMMENT '车间名称',
    inspection_count INT COMMENT '检验次数',
    sample_quantity INT COMMENT '抽样总数',
    qualified_quantity INT COMMENT '合格总数',
    unqualified_quantity INT COMMENT '不合格总数',
    qualified_rate DECIMAL(5,2) COMMENT '合格率(%)',
    defect_quantity INT COMMENT '缺陷总数',
    defect_rate DECIMAL(5,2) COMMENT '缺陷率(%)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, product_id, workshop_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_product_id (product_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'ADS-质量分析报表';

-- 6. 设备效率分析报表
CREATE TABLE IF NOT EXISTS ads_equipment_efficiency (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    equipment_id VARCHAR(50) COMMENT '设备ID',
    equipment_name VARCHAR(200) COMMENT '设备名称',
    equipment_type VARCHAR(50) COMMENT '设备类型',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    workshop_name VARCHAR(200) COMMENT '车间名称',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    total_running_hours DECIMAL(10,2) COMMENT '总运行小时数',
    total_downtime_hours DECIMAL(10,2) COMMENT '总停机小时数',
    utilization_rate DECIMAL(5,2) COMMENT '利用率(%)',
    total_production_quantity DECIMAL(18,3) COMMENT '总产量',
    production_efficiency DECIMAL(5,2) COMMENT '生产效率(%)',
    total_energy_consumption DECIMAL(18,2) COMMENT '总能耗',
    energy_efficiency DECIMAL(5,2) COMMENT '能效比',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, equipment_id),
    INDEX idx_stat_date (stat_date),
    INDEX idx_equipment_id (equipment_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'ADS-设备效率分析报表';

-- 7. 成本分析报表
CREATE TABLE IF NOT EXISTS ads_cost_analysis (
    stat_date DATE COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    cost_center_id VARCHAR(50) COMMENT '成本中心ID',
    cost_center_name VARCHAR(200) COMMENT '成本中心名称',
    department_id VARCHAR(50) COMMENT '部门ID',
    department_name VARCHAR(200) COMMENT '部门名称',
    cost_type VARCHAR(50) COMMENT '成本类型',
    total_amount DECIMAL(18,2) COMMENT '总金额',
    total_quantity DECIMAL(18,3) COMMENT '总数量',
    avg_unit_cost DECIMAL(18,2) COMMENT '平均单位成本',
    cost_ratio DECIMAL(5,2) COMMENT '成本占比(%)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (stat_date, cost_center_id, cost_type),
    INDEX idx_stat_date (stat_date),
    INDEX idx_cost_center_id (cost_center_id),
    INDEX idx_cost_type (cost_type)
) COMMENT 'ADS-成本分析报表';

-- 8. 综合经营分析报表
CREATE TABLE IF NOT EXISTS ads_business_overview (
    stat_date DATE PRIMARY KEY COMMENT '统计日期',
    stat_year INT COMMENT '统计年份',
    stat_month INT COMMENT '统计月份',
    stat_quarter INT COMMENT '统计季度',
    total_sales_amount DECIMAL(18,2) COMMENT '总销售额',
    total_purchase_amount DECIMAL(18,2) COMMENT '总采购额',
    total_production_quantity DECIMAL(18,3) COMMENT '总产量',
    total_inventory_cost DECIMAL(18,2) COMMENT '总库存成本',
    total_cost_amount DECIMAL(18,2) COMMENT '总成本',
    gross_profit DECIMAL(18,2) COMMENT '毛利润',
    gross_profit_rate DECIMAL(5,2) COMMENT '毛利率(%)',
    order_count INT COMMENT '订单数量',
    customer_count INT COMMENT '客户数量',
    supplier_count INT COMMENT '供应商数量',
    avg_order_amount DECIMAL(18,2) COMMENT '平均订单金额',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_stat_date (stat_date),
    INDEX idx_stat_year (stat_year),
    INDEX idx_stat_month (stat_month)
) COMMENT 'ADS-综合经营分析报表';
