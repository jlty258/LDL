-- DWD层：数据明细层
-- 对ODS层数据进行清洗、转换、标准化

USE sqlExpert;

-- 1. 订单事实表
CREATE TABLE IF NOT EXISTS dwd_order_fact (
    order_id VARCHAR(50) PRIMARY KEY COMMENT '订单ID',
    order_no VARCHAR(100) NOT NULL COMMENT '订单编号',
    customer_id VARCHAR(50) COMMENT '客户ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    order_date DATE COMMENT '订单日期',
    order_year INT COMMENT '订单年份',
    order_month INT COMMENT '订单月份',
    order_quarter INT COMMENT '订单季度',
    order_status VARCHAR(20) COMMENT '订单状态',
    order_amount DECIMAL(18,2) COMMENT '订单金额',
    order_quantity DECIMAL(18,3) COMMENT '订单数量',
    unit_price DECIMAL(18,2) COMMENT '单价',
    sales_rep_id VARCHAR(50) COMMENT '销售代表ID',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    region VARCHAR(100) COMMENT '区域',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_order_date (order_date),
    INDEX idx_customer_id (customer_id),
    INDEX idx_product_id (product_id),
    INDEX idx_order_status (order_status)
) COMMENT 'DWD-订单事实表';

-- 2. 生产事实表
CREATE TABLE IF NOT EXISTS dwd_production_fact (
    production_id VARCHAR(50) PRIMARY KEY COMMENT '生产ID',
    work_order_id VARCHAR(50) COMMENT '工单ID',
    plan_id VARCHAR(50) COMMENT '计划ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    production_date DATE COMMENT '生产日期',
    production_year INT COMMENT '生产年份',
    production_month INT COMMENT '生产月份',
    production_quarter INT COMMENT '生产季度',
    plan_quantity DECIMAL(18,3) COMMENT '计划数量',
    actual_quantity DECIMAL(18,3) COMMENT '实际数量',
    completed_quantity DECIMAL(18,3) COMMENT '完成数量',
    completion_rate DECIMAL(5,2) COMMENT '完成率(%)',
    production_status VARCHAR(20) COMMENT '生产状态',
    start_time DATETIME COMMENT '开始时间',
    end_time DATETIME COMMENT '结束时间',
    duration_hours DECIMAL(10,2) COMMENT '持续时间(小时)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_production_date (production_date),
    INDEX idx_product_id (product_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'DWD-生产事实表';

-- 3. 库存事实表
CREATE TABLE IF NOT EXISTS dwd_inventory_fact (
    inventory_id VARCHAR(50) PRIMARY KEY COMMENT '库存ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    inventory_date DATE COMMENT '库存日期',
    inventory_year INT COMMENT '库存年份',
    inventory_month INT COMMENT '库存月份',
    quantity DECIMAL(18,3) COMMENT '数量',
    available_quantity DECIMAL(18,3) COMMENT '可用数量',
    reserved_quantity DECIMAL(18,3) COMMENT '预留数量',
    unit_cost DECIMAL(18,2) COMMENT '单位成本',
    total_cost DECIMAL(18,2) COMMENT '总成本',
    material_category VARCHAR(100) COMMENT '物料类别',
    warehouse_type VARCHAR(50) COMMENT '仓库类型',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_inventory_date (inventory_date),
    INDEX idx_material_id (material_id),
    INDEX idx_warehouse_id (warehouse_id)
) COMMENT 'DWD-库存事实表';

-- 4. 采购事实表
CREATE TABLE IF NOT EXISTS dwd_purchase_fact (
    purchase_id VARCHAR(50) PRIMARY KEY COMMENT '采购ID',
    purchase_no VARCHAR(100) NOT NULL COMMENT '采购订单号',
    supplier_id VARCHAR(50) COMMENT '供应商ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    order_date DATE COMMENT '订单日期',
    delivery_date DATE COMMENT '交货日期',
    order_year INT COMMENT '订单年份',
    order_month INT COMMENT '订单月份',
    order_quarter INT COMMENT '订单季度',
    order_status VARCHAR(20) COMMENT '订单状态',
    purchase_quantity DECIMAL(18,3) COMMENT '采购数量',
    unit_price DECIMAL(18,2) COMMENT '单价',
    purchase_amount DECIMAL(18,2) COMMENT '采购金额',
    buyer_id VARCHAR(50) COMMENT '采购员ID',
    region VARCHAR(100) COMMENT '区域',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_order_date (order_date),
    INDEX idx_supplier_id (supplier_id),
    INDEX idx_material_id (material_id)
) COMMENT 'DWD-采购事实表';

-- 5. 质量事实表
CREATE TABLE IF NOT EXISTS dwd_quality_fact (
    quality_id VARCHAR(50) PRIMARY KEY COMMENT '质量ID',
    inspection_id VARCHAR(50) COMMENT '检验ID',
    work_order_id VARCHAR(50) COMMENT '工单ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    inspection_date DATE COMMENT '检验日期',
    inspection_year INT COMMENT '检验年份',
    inspection_month INT COMMENT '检验月份',
    inspection_quarter INT COMMENT '检验季度',
    inspection_type VARCHAR(50) COMMENT '检验类型',
    sample_quantity INT COMMENT '抽样数量',
    qualified_quantity INT COMMENT '合格数量',
    unqualified_quantity INT COMMENT '不合格数量',
    qualified_rate DECIMAL(5,2) COMMENT '合格率(%)',
    defect_quantity INT COMMENT '缺陷数量',
    inspector_id VARCHAR(50) COMMENT '检验员ID',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_inspection_date (inspection_date),
    INDEX idx_product_id (product_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'DWD-质量事实表';

-- 6. 设备运行事实表
CREATE TABLE IF NOT EXISTS dwd_equipment_runtime_fact (
    runtime_id VARCHAR(50) PRIMARY KEY COMMENT '运行记录ID',
    equipment_id VARCHAR(50) COMMENT '设备ID',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    record_date DATE COMMENT '记录日期',
    record_year INT COMMENT '记录年份',
    record_month INT COMMENT '记录月份',
    record_quarter INT COMMENT '记录季度',
    running_hours DECIMAL(10,2) COMMENT '运行小时数',
    downtime_hours DECIMAL(10,2) COMMENT '停机小时数',
    utilization_rate DECIMAL(5,2) COMMENT '利用率(%)',
    production_quantity DECIMAL(18,3) COMMENT '产量',
    energy_consumption DECIMAL(18,2) COMMENT '能耗',
    equipment_type VARCHAR(50) COMMENT '设备类型',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_record_date (record_date),
    INDEX idx_equipment_id (equipment_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'DWD-设备运行事实表';

-- 7. 成本事实表
CREATE TABLE IF NOT EXISTS dwd_cost_fact (
    cost_id VARCHAR(50) PRIMARY KEY COMMENT '成本ID',
    cost_center_id VARCHAR(50) COMMENT '成本中心ID',
    department_id VARCHAR(50) COMMENT '部门ID',
    cost_date DATE COMMENT '成本日期',
    cost_year INT COMMENT '成本年份',
    cost_month INT COMMENT '成本月份',
    cost_quarter INT COMMENT '成本季度',
    cost_type VARCHAR(50) COMMENT '成本类型',
    cost_item VARCHAR(100) COMMENT '成本项目',
    amount DECIMAL(18,2) COMMENT '金额',
    quantity DECIMAL(18,3) COMMENT '数量',
    unit_cost DECIMAL(18,2) COMMENT '单位成本',
    factory_id VARCHAR(50) COMMENT '工厂ID',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_cost_date (cost_date),
    INDEX idx_cost_center_id (cost_center_id),
    INDEX idx_cost_type (cost_type)
) COMMENT 'DWD-成本事实表';

-- 8. 客户维度表
CREATE TABLE IF NOT EXISTS dwd_customer_dim (
    customer_id VARCHAR(50) PRIMARY KEY COMMENT '客户ID',
    customer_code VARCHAR(100) COMMENT '客户编码',
    customer_name VARCHAR(200) COMMENT '客户名称',
    customer_type VARCHAR(50) COMMENT '客户类型',
    industry VARCHAR(100) COMMENT '行业',
    region VARCHAR(100) COMMENT '区域',
    city VARCHAR(100) COMMENT '城市',
    credit_level VARCHAR(20) COMMENT '信用等级',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_customer_code (customer_code),
    INDEX idx_customer_type (customer_type),
    INDEX idx_region (region)
) COMMENT 'DWD-客户维度表';

-- 9. 产品维度表
CREATE TABLE IF NOT EXISTS dwd_product_dim (
    product_id VARCHAR(50) PRIMARY KEY COMMENT '产品ID',
    product_code VARCHAR(100) NOT NULL COMMENT '产品编码',
    product_name VARCHAR(200) COMMENT '产品名称',
    product_category VARCHAR(100) COMMENT '产品类别',
    product_type VARCHAR(50) COMMENT '产品类型',
    brand VARCHAR(100) COMMENT '品牌',
    standard_price DECIMAL(18,2) COMMENT '标准价格',
    cost_price DECIMAL(18,2) COMMENT '成本价',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_product_code (product_code),
    INDEX idx_product_category (product_category),
    INDEX idx_brand (brand)
) COMMENT 'DWD-产品维度表';

-- 10. 物料维度表
CREATE TABLE IF NOT EXISTS dwd_material_dim (
    material_id VARCHAR(50) PRIMARY KEY COMMENT '物料ID',
    material_code VARCHAR(100) NOT NULL COMMENT '物料编码',
    material_name VARCHAR(200) COMMENT '物料名称',
    material_category VARCHAR(100) COMMENT '物料类别',
    material_type VARCHAR(50) COMMENT '物料类型',
    standard_price DECIMAL(18,2) COMMENT '标准价格',
    cost_price DECIMAL(18,2) COMMENT '成本价',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_material_code (material_code),
    INDEX idx_material_category (material_category)
) COMMENT 'DWD-物料维度表';
