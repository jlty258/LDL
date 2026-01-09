-- ODS层：操作数据存储层
-- 30张源数据表，存储来自业务系统的原始数据

USE sqlExpert;

-- 1. 订单主表
CREATE TABLE IF NOT EXISTS ods_order_master (
    order_id VARCHAR(50) PRIMARY KEY COMMENT '订单ID',
    order_no VARCHAR(100) NOT NULL COMMENT '订单编号',
    customer_id VARCHAR(50) COMMENT '客户ID',
    order_date DATETIME COMMENT '订单日期',
    order_status VARCHAR(20) COMMENT '订单状态',
    total_amount DECIMAL(18,2) COMMENT '订单总金额',
    currency VARCHAR(10) COMMENT '币种',
    sales_rep_id VARCHAR(50) COMMENT '销售代表ID',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_customer_id (customer_id),
    INDEX idx_order_date (order_date),
    INDEX idx_order_status (order_status)
) COMMENT 'ODS-订单主表';

-- 2. 订单明细表
CREATE TABLE IF NOT EXISTS ods_order_detail (
    detail_id VARCHAR(50) PRIMARY KEY COMMENT '明细ID',
    order_id VARCHAR(50) NOT NULL COMMENT '订单ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    product_code VARCHAR(100) COMMENT '产品编码',
    product_name VARCHAR(200) COMMENT '产品名称',
    quantity DECIMAL(18,3) COMMENT '数量',
    unit_price DECIMAL(18,2) COMMENT '单价',
    amount DECIMAL(18,2) COMMENT '金额',
    unit VARCHAR(20) COMMENT '单位',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_order_id (order_id),
    INDEX idx_product_id (product_id)
) COMMENT 'ODS-订单明细表';

-- 3. 客户主表
CREATE TABLE IF NOT EXISTS ods_customer_master (
    customer_id VARCHAR(50) PRIMARY KEY COMMENT '客户ID',
    customer_code VARCHAR(100) COMMENT '客户编码',
    customer_name VARCHAR(200) COMMENT '客户名称',
    customer_type VARCHAR(50) COMMENT '客户类型',
    industry VARCHAR(100) COMMENT '行业',
    region VARCHAR(100) COMMENT '区域',
    city VARCHAR(100) COMMENT '城市',
    address TEXT COMMENT '地址',
    contact_person VARCHAR(100) COMMENT '联系人',
    contact_phone VARCHAR(50) COMMENT '联系电话',
    credit_level VARCHAR(20) COMMENT '信用等级',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_customer_code (customer_code),
    INDEX idx_customer_type (customer_type),
    INDEX idx_region (region)
) COMMENT 'ODS-客户主表';

-- 4. 产品主表
CREATE TABLE IF NOT EXISTS ods_product_master (
    product_id VARCHAR(50) PRIMARY KEY COMMENT '产品ID',
    product_code VARCHAR(100) NOT NULL COMMENT '产品编码',
    product_name VARCHAR(200) COMMENT '产品名称',
    product_category VARCHAR(100) COMMENT '产品类别',
    product_type VARCHAR(50) COMMENT '产品类型',
    brand VARCHAR(100) COMMENT '品牌',
    unit VARCHAR(20) COMMENT '单位',
    standard_price DECIMAL(18,2) COMMENT '标准价格',
    cost_price DECIMAL(18,2) COMMENT '成本价',
    weight DECIMAL(18,3) COMMENT '重量(kg)',
    volume DECIMAL(18,3) COMMENT '体积(m³)',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_product_code (product_code),
    INDEX idx_product_category (product_category),
    INDEX idx_brand (brand)
) COMMENT 'ODS-产品主表';

-- 5. 生产计划表
CREATE TABLE IF NOT EXISTS ods_production_plan (
    plan_id VARCHAR(50) PRIMARY KEY COMMENT '计划ID',
    plan_no VARCHAR(100) NOT NULL COMMENT '计划编号',
    product_id VARCHAR(50) COMMENT '产品ID',
    plan_date DATE COMMENT '计划日期',
    plan_quantity DECIMAL(18,3) COMMENT '计划数量',
    actual_quantity DECIMAL(18,3) COMMENT '实际数量',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    plan_status VARCHAR(20) COMMENT '计划状态',
    start_time DATETIME COMMENT '开始时间',
    end_time DATETIME COMMENT '结束时间',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_plan_date (plan_date),
    INDEX idx_product_id (product_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'ODS-生产计划表';

-- 6. 生产工单表
CREATE TABLE IF NOT EXISTS ods_production_order (
    work_order_id VARCHAR(50) PRIMARY KEY COMMENT '工单ID',
    work_order_no VARCHAR(100) NOT NULL COMMENT '工单编号',
    plan_id VARCHAR(50) COMMENT '计划ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    order_quantity DECIMAL(18,3) COMMENT '订单数量',
    completed_quantity DECIMAL(18,3) COMMENT '完成数量',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    order_status VARCHAR(20) COMMENT '工单状态',
    start_time DATETIME COMMENT '开始时间',
    end_time DATETIME COMMENT '结束时间',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_plan_id (plan_id),
    INDEX idx_product_id (product_id),
    INDEX idx_order_status (order_status)
) COMMENT 'ODS-生产工单表';

-- 7. 物料清单表
CREATE TABLE IF NOT EXISTS ods_bom (
    bom_id VARCHAR(50) PRIMARY KEY COMMENT 'BOM ID',
    bom_no VARCHAR(100) NOT NULL COMMENT 'BOM编号',
    product_id VARCHAR(50) COMMENT '产品ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    material_code VARCHAR(100) COMMENT '物料编码',
    material_name VARCHAR(200) COMMENT '物料名称',
    quantity DECIMAL(18,6) COMMENT '用量',
    unit VARCHAR(20) COMMENT '单位',
    loss_rate DECIMAL(5,2) COMMENT '损耗率(%)',
    version VARCHAR(20) COMMENT '版本',
    effective_date DATE COMMENT '生效日期',
    expire_date DATE COMMENT '失效日期',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_product_id (product_id),
    INDEX idx_material_id (material_id),
    INDEX idx_bom_no (bom_no)
) COMMENT 'ODS-物料清单表';

-- 8. 物料主表
CREATE TABLE IF NOT EXISTS ods_material_master (
    material_id VARCHAR(50) PRIMARY KEY COMMENT '物料ID',
    material_code VARCHAR(100) NOT NULL COMMENT '物料编码',
    material_name VARCHAR(200) COMMENT '物料名称',
    material_category VARCHAR(100) COMMENT '物料类别',
    material_type VARCHAR(50) COMMENT '物料类型',
    unit VARCHAR(20) COMMENT '单位',
    standard_price DECIMAL(18,2) COMMENT '标准价格',
    cost_price DECIMAL(18,2) COMMENT '成本价',
    supplier_id VARCHAR(50) COMMENT '供应商ID',
    lead_time INT COMMENT '提前期(天)',
    min_stock DECIMAL(18,3) COMMENT '最小库存',
    max_stock DECIMAL(18,3) COMMENT '最大库存',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_material_code (material_code),
    INDEX idx_material_category (material_category),
    INDEX idx_supplier_id (supplier_id)
) COMMENT 'ODS-物料主表';

-- 9. 库存表
CREATE TABLE IF NOT EXISTS ods_inventory (
    inventory_id VARCHAR(50) PRIMARY KEY COMMENT '库存ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    location_code VARCHAR(100) COMMENT '库位编码',
    quantity DECIMAL(18,3) COMMENT '数量',
    available_quantity DECIMAL(18,3) COMMENT '可用数量',
    reserved_quantity DECIMAL(18,3) COMMENT '预留数量',
    unit_cost DECIMAL(18,2) COMMENT '单位成本',
    total_cost DECIMAL(18,2) COMMENT '总成本',
    batch_no VARCHAR(100) COMMENT '批次号',
    production_date DATE COMMENT '生产日期',
    expire_date DATE COMMENT '过期日期',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_material_id (material_id),
    INDEX idx_warehouse_id (warehouse_id),
    INDEX idx_batch_no (batch_no)
) COMMENT 'ODS-库存表';

-- 10. 入库单表
CREATE TABLE IF NOT EXISTS ods_inbound_order (
    inbound_id VARCHAR(50) PRIMARY KEY COMMENT '入库单ID',
    inbound_no VARCHAR(100) NOT NULL COMMENT '入库单号',
    inbound_type VARCHAR(50) COMMENT '入库类型',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    supplier_id VARCHAR(50) COMMENT '供应商ID',
    inbound_date DATETIME COMMENT '入库日期',
    inbound_status VARCHAR(20) COMMENT '入库状态',
    total_amount DECIMAL(18,2) COMMENT '总金额',
    operator_id VARCHAR(50) COMMENT '操作员ID',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_inbound_date (inbound_date),
    INDEX idx_warehouse_id (warehouse_id),
    INDEX idx_supplier_id (supplier_id)
) COMMENT 'ODS-入库单表';

-- 11. 入库明细表
CREATE TABLE IF NOT EXISTS ods_inbound_detail (
    detail_id VARCHAR(50) PRIMARY KEY COMMENT '明细ID',
    inbound_id VARCHAR(50) NOT NULL COMMENT '入库单ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    quantity DECIMAL(18,3) COMMENT '数量',
    unit_price DECIMAL(18,2) COMMENT '单价',
    amount DECIMAL(18,2) COMMENT '金额',
    batch_no VARCHAR(100) COMMENT '批次号',
    location_code VARCHAR(100) COMMENT '库位编码',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_inbound_id (inbound_id),
    INDEX idx_material_id (material_id)
) COMMENT 'ODS-入库明细表';

-- 12. 出库单表
CREATE TABLE IF NOT EXISTS ods_outbound_order (
    outbound_id VARCHAR(50) PRIMARY KEY COMMENT '出库单ID',
    outbound_no VARCHAR(100) NOT NULL COMMENT '出库单号',
    outbound_type VARCHAR(50) COMMENT '出库类型',
    warehouse_id VARCHAR(50) COMMENT '仓库ID',
    customer_id VARCHAR(50) COMMENT '客户ID',
    outbound_date DATETIME COMMENT '出库日期',
    outbound_status VARCHAR(20) COMMENT '出库状态',
    operator_id VARCHAR(50) COMMENT '操作员ID',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_outbound_date (outbound_date),
    INDEX idx_warehouse_id (warehouse_id),
    INDEX idx_customer_id (customer_id)
) COMMENT 'ODS-出库单表';

-- 13. 出库明细表
CREATE TABLE IF NOT EXISTS ods_outbound_detail (
    detail_id VARCHAR(50) PRIMARY KEY COMMENT '明细ID',
    outbound_id VARCHAR(50) NOT NULL COMMENT '出库单ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    quantity DECIMAL(18,3) COMMENT '数量',
    unit_price DECIMAL(18,2) COMMENT '单价',
    amount DECIMAL(18,2) COMMENT '金额',
    batch_no VARCHAR(100) COMMENT '批次号',
    location_code VARCHAR(100) COMMENT '库位编码',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_outbound_id (outbound_id),
    INDEX idx_material_id (material_id)
) COMMENT 'ODS-出库明细表';

-- 14. 供应商主表
CREATE TABLE IF NOT EXISTS ods_supplier_master (
    supplier_id VARCHAR(50) PRIMARY KEY COMMENT '供应商ID',
    supplier_code VARCHAR(100) COMMENT '供应商编码',
    supplier_name VARCHAR(200) COMMENT '供应商名称',
    supplier_type VARCHAR(50) COMMENT '供应商类型',
    region VARCHAR(100) COMMENT '区域',
    city VARCHAR(100) COMMENT '城市',
    address TEXT COMMENT '地址',
    contact_person VARCHAR(100) COMMENT '联系人',
    contact_phone VARCHAR(50) COMMENT '联系电话',
    credit_level VARCHAR(20) COMMENT '信用等级',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_supplier_code (supplier_code),
    INDEX idx_supplier_type (supplier_type),
    INDEX idx_region (region)
) COMMENT 'ODS-供应商主表';

-- 15. 采购订单表
CREATE TABLE IF NOT EXISTS ods_purchase_order (
    purchase_id VARCHAR(50) PRIMARY KEY COMMENT '采购订单ID',
    purchase_no VARCHAR(100) NOT NULL COMMENT '采购订单号',
    supplier_id VARCHAR(50) COMMENT '供应商ID',
    order_date DATETIME COMMENT '订单日期',
    delivery_date DATE COMMENT '交货日期',
    order_status VARCHAR(20) COMMENT '订单状态',
    total_amount DECIMAL(18,2) COMMENT '订单总金额',
    currency VARCHAR(10) COMMENT '币种',
    buyer_id VARCHAR(50) COMMENT '采购员ID',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_order_date (order_date),
    INDEX idx_supplier_id (supplier_id),
    INDEX idx_order_status (order_status)
) COMMENT 'ODS-采购订单表';

-- 16. 采购明细表
CREATE TABLE IF NOT EXISTS ods_purchase_detail (
    detail_id VARCHAR(50) PRIMARY KEY COMMENT '明细ID',
    purchase_id VARCHAR(50) NOT NULL COMMENT '采购订单ID',
    material_id VARCHAR(50) COMMENT '物料ID',
    quantity DECIMAL(18,3) COMMENT '数量',
    unit_price DECIMAL(18,2) COMMENT '单价',
    amount DECIMAL(18,2) COMMENT '金额',
    delivery_date DATE COMMENT '交货日期',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_purchase_id (purchase_id),
    INDEX idx_material_id (material_id)
) COMMENT 'ODS-采购明细表';

-- 17. 车间主表
CREATE TABLE IF NOT EXISTS ods_workshop_master (
    workshop_id VARCHAR(50) PRIMARY KEY COMMENT '车间ID',
    workshop_code VARCHAR(100) NOT NULL COMMENT '车间编码',
    workshop_name VARCHAR(200) COMMENT '车间名称',
    factory_id VARCHAR(50) COMMENT '工厂ID',
    workshop_type VARCHAR(50) COMMENT '车间类型',
    manager_id VARCHAR(50) COMMENT '车间主任ID',
    capacity DECIMAL(18,2) COMMENT '产能',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_workshop_code (workshop_code),
    INDEX idx_factory_id (factory_id)
) COMMENT 'ODS-车间主表';

-- 18. 生产线表
CREATE TABLE IF NOT EXISTS ods_production_line (
    line_id VARCHAR(50) PRIMARY KEY COMMENT '生产线ID',
    line_code VARCHAR(100) NOT NULL COMMENT '生产线编码',
    line_name VARCHAR(200) COMMENT '生产线名称',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    line_type VARCHAR(50) COMMENT '生产线类型',
    capacity DECIMAL(18,2) COMMENT '产能',
    efficiency DECIMAL(5,2) COMMENT '效率(%)',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_workshop_id (workshop_id),
    INDEX idx_line_code (line_code)
) COMMENT 'ODS-生产线表';

-- 19. 设备主表
CREATE TABLE IF NOT EXISTS ods_equipment_master (
    equipment_id VARCHAR(50) PRIMARY KEY COMMENT '设备ID',
    equipment_code VARCHAR(100) NOT NULL COMMENT '设备编码',
    equipment_name VARCHAR(200) COMMENT '设备名称',
    equipment_type VARCHAR(50) COMMENT '设备类型',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    production_line_id VARCHAR(50) COMMENT '生产线ID',
    manufacturer VARCHAR(200) COMMENT '制造商',
    model VARCHAR(100) COMMENT '型号',
    purchase_date DATE COMMENT '采购日期',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_equipment_code (equipment_code),
    INDEX idx_workshop_id (workshop_id),
    INDEX idx_status (status)
) COMMENT 'ODS-设备主表';

-- 20. 设备运行记录表
CREATE TABLE IF NOT EXISTS ods_equipment_runtime (
    runtime_id VARCHAR(50) PRIMARY KEY COMMENT '运行记录ID',
    equipment_id VARCHAR(50) COMMENT '设备ID',
    record_date DATE COMMENT '记录日期',
    start_time DATETIME COMMENT '开始时间',
    end_time DATETIME COMMENT '结束时间',
    running_hours DECIMAL(10,2) COMMENT '运行小时数',
    production_quantity DECIMAL(18,3) COMMENT '产量',
    downtime_hours DECIMAL(10,2) COMMENT '停机小时数',
    downtime_reason VARCHAR(200) COMMENT '停机原因',
    energy_consumption DECIMAL(18,2) COMMENT '能耗',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_equipment_id (equipment_id),
    INDEX idx_record_date (record_date)
) COMMENT 'ODS-设备运行记录表';

-- 21. 质量检验表
CREATE TABLE IF NOT EXISTS ods_quality_inspection (
    inspection_id VARCHAR(50) PRIMARY KEY COMMENT '检验ID',
    inspection_no VARCHAR(100) NOT NULL COMMENT '检验单号',
    work_order_id VARCHAR(50) COMMENT '工单ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    inspection_date DATETIME COMMENT '检验日期',
    inspection_type VARCHAR(50) COMMENT '检验类型',
    sample_quantity INT COMMENT '抽样数量',
    qualified_quantity INT COMMENT '合格数量',
    unqualified_quantity INT COMMENT '不合格数量',
    qualified_rate DECIMAL(5,2) COMMENT '合格率(%)',
    inspector_id VARCHAR(50) COMMENT '检验员ID',
    inspection_result VARCHAR(20) COMMENT '检验结果',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_inspection_date (inspection_date),
    INDEX idx_work_order_id (work_order_id),
    INDEX idx_product_id (product_id)
) COMMENT 'ODS-质量检验表';

-- 22. 不合格品记录表
CREATE TABLE IF NOT EXISTS ods_defect_record (
    defect_id VARCHAR(50) PRIMARY KEY COMMENT '不合格品ID',
    inspection_id VARCHAR(50) COMMENT '检验ID',
    work_order_id VARCHAR(50) COMMENT '工单ID',
    product_id VARCHAR(50) COMMENT '产品ID',
    defect_date DATETIME COMMENT '发现日期',
    defect_type VARCHAR(50) COMMENT '缺陷类型',
    defect_code VARCHAR(50) COMMENT '缺陷代码',
    defect_description TEXT COMMENT '缺陷描述',
    quantity INT COMMENT '数量',
    severity VARCHAR(20) COMMENT '严重程度',
    handler_id VARCHAR(50) COMMENT '处理人ID',
    handle_method VARCHAR(50) COMMENT '处理方法',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_inspection_id (inspection_id),
    INDEX idx_defect_date (defect_date),
    INDEX idx_defect_type (defect_type)
) COMMENT 'ODS-不合格品记录表';

-- 23. 员工主表
CREATE TABLE IF NOT EXISTS ods_employee_master (
    employee_id VARCHAR(50) PRIMARY KEY COMMENT '员工ID',
    employee_code VARCHAR(100) NOT NULL COMMENT '员工编码',
    employee_name VARCHAR(100) COMMENT '员工姓名',
    department_id VARCHAR(50) COMMENT '部门ID',
    position VARCHAR(100) COMMENT '职位',
    workshop_id VARCHAR(50) COMMENT '车间ID',
    hire_date DATE COMMENT '入职日期',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_employee_code (employee_code),
    INDEX idx_department_id (department_id),
    INDEX idx_workshop_id (workshop_id)
) COMMENT 'ODS-员工主表';

-- 24. 考勤记录表
CREATE TABLE IF NOT EXISTS ods_attendance (
    attendance_id VARCHAR(50) PRIMARY KEY COMMENT '考勤ID',
    employee_id VARCHAR(50) COMMENT '员工ID',
    attendance_date DATE COMMENT '考勤日期',
    check_in_time DATETIME COMMENT '签到时间',
    check_out_time DATETIME COMMENT '签退时间',
    work_hours DECIMAL(5,2) COMMENT '工作小时数',
    overtime_hours DECIMAL(5,2) COMMENT '加班小时数',
    attendance_status VARCHAR(20) COMMENT '考勤状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_employee_id (employee_id),
    INDEX idx_attendance_date (attendance_date)
) COMMENT 'ODS-考勤记录表';

-- 25. 仓库主表
CREATE TABLE IF NOT EXISTS ods_warehouse_master (
    warehouse_id VARCHAR(50) PRIMARY KEY COMMENT '仓库ID',
    warehouse_code VARCHAR(100) NOT NULL COMMENT '仓库编码',
    warehouse_name VARCHAR(200) COMMENT '仓库名称',
    warehouse_type VARCHAR(50) COMMENT '仓库类型',
    factory_id VARCHAR(50) COMMENT '工厂ID',
    address TEXT COMMENT '地址',
    manager_id VARCHAR(50) COMMENT '仓库管理员ID',
    capacity DECIMAL(18,2) COMMENT '容量',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_warehouse_code (warehouse_code),
    INDEX idx_factory_id (factory_id)
) COMMENT 'ODS-仓库主表';

-- 26. 销售回款表
CREATE TABLE IF NOT EXISTS ods_sales_payment (
    payment_id VARCHAR(50) PRIMARY KEY COMMENT '回款ID',
    payment_no VARCHAR(100) NOT NULL COMMENT '回款单号',
    order_id VARCHAR(50) COMMENT '订单ID',
    customer_id VARCHAR(50) COMMENT '客户ID',
    payment_date DATETIME COMMENT '回款日期',
    payment_amount DECIMAL(18,2) COMMENT '回款金额',
    payment_method VARCHAR(50) COMMENT '回款方式',
    payment_status VARCHAR(20) COMMENT '回款状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_payment_date (payment_date),
    INDEX idx_order_id (order_id),
    INDEX idx_customer_id (customer_id)
) COMMENT 'ODS-销售回款表';

-- 27. 成本中心表
CREATE TABLE IF NOT EXISTS ods_cost_center (
    cost_center_id VARCHAR(50) PRIMARY KEY COMMENT '成本中心ID',
    cost_center_code VARCHAR(100) NOT NULL COMMENT '成本中心编码',
    cost_center_name VARCHAR(200) COMMENT '成本中心名称',
    department_id VARCHAR(50) COMMENT '部门ID',
    cost_type VARCHAR(50) COMMENT '成本类型',
    manager_id VARCHAR(50) COMMENT '负责人ID',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_cost_center_code (cost_center_code),
    INDEX idx_department_id (department_id)
) COMMENT 'ODS-成本中心表';

-- 28. 成本明细表
CREATE TABLE IF NOT EXISTS ods_cost_detail (
    cost_id VARCHAR(50) PRIMARY KEY COMMENT '成本ID',
    cost_center_id VARCHAR(50) COMMENT '成本中心ID',
    cost_date DATE COMMENT '成本日期',
    cost_type VARCHAR(50) COMMENT '成本类型',
    cost_item VARCHAR(100) COMMENT '成本项目',
    amount DECIMAL(18,2) COMMENT '金额',
    quantity DECIMAL(18,3) COMMENT '数量',
    unit_cost DECIMAL(18,2) COMMENT '单位成本',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_cost_center_id (cost_center_id),
    INDEX idx_cost_date (cost_date),
    INDEX idx_cost_type (cost_type)
) COMMENT 'ODS-成本明细表';

-- 29. 工厂主表
CREATE TABLE IF NOT EXISTS ods_factory_master (
    factory_id VARCHAR(50) PRIMARY KEY COMMENT '工厂ID',
    factory_code VARCHAR(100) NOT NULL COMMENT '工厂编码',
    factory_name VARCHAR(200) COMMENT '工厂名称',
    region VARCHAR(100) COMMENT '区域',
    city VARCHAR(100) COMMENT '城市',
    address TEXT COMMENT '地址',
    manager_id VARCHAR(50) COMMENT '厂长ID',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_factory_code (factory_code),
    INDEX idx_region (region)
) COMMENT 'ODS-工厂主表';

-- 30. 部门主表
CREATE TABLE IF NOT EXISTS ods_department_master (
    department_id VARCHAR(50) PRIMARY KEY COMMENT '部门ID',
    department_code VARCHAR(100) NOT NULL COMMENT '部门编码',
    department_name VARCHAR(200) COMMENT '部门名称',
    factory_id VARCHAR(50) COMMENT '工厂ID',
    parent_department_id VARCHAR(50) COMMENT '上级部门ID',
    manager_id VARCHAR(50) COMMENT '部门经理ID',
    status VARCHAR(20) COMMENT '状态',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_department_code (department_code),
    INDEX idx_factory_id (factory_id),
    INDEX idx_parent_department_id (parent_department_id)
) COMMENT 'ODS-部门主表';
