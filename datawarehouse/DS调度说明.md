# DolphinScheduler调度任务创建说明

## 当前状态

- ✅ DolphinScheduler服务正常运行（http://localhost:12345）
- ✅ 项目"制造业数仓"已创建成功
- ⚠️ 创建工作流时遇到API端点问题

## 解决方案

### 方法1: 通过Web界面手动创建（推荐）

1. **访问DolphinScheduler Web界面**
   - URL: http://localhost:12345
   - 用户名: `admin`
   - 密码: `dolphinscheduler123`

2. **进入项目**
   - 点击左侧菜单"项目管理"
   - 选择"制造业数仓"项目

3. **创建工作流**
   - 点击"工作流定义"
   - 点击"创建工作流"
   - 为每个ETL任务创建工作流（共30个）

### 方法2: 使用Python脚本（需要修复）

如果Python环境可用，可以执行：
```bash
python datawarehouse/schedulers/create_dolphinscheduler_tasks.py
```

### 方法3: 使用REST API（需要修复API端点）

当前PowerShell脚本已创建，但需要修复API端点。可以：
1. 检查DolphinScheduler API文档
2. 修复 `datawarehouse/schedulers/create_ds_tasks.ps1` 中的API端点
3. 重新执行脚本

## 30个调度任务列表

### ODS层 (10个)
1. ods_01_order_master_etl
2. ods_02_order_detail_etl
3. ods_03_customer_etl
4. ods_04_product_etl
5. ods_05_production_plan_etl
6. ods_06_production_order_etl
7. ods_07_bom_etl
8. ods_08_material_etl
9. ods_09_inventory_etl
10. ods_10_purchase_etl

### DWD层 (7个)
11. dwd_01_order_fact_etl
12. dwd_02_production_fact_etl
13. dwd_03_inventory_fact_etl
14. dwd_04_purchase_fact_etl
15. dwd_05_quality_fact_etl
16. dwd_06_equipment_runtime_etl
17. dwd_07_cost_fact_etl

### DWS层 (7个)
18. dws_01_order_daily_etl
19. dws_02_production_daily_etl
20. dws_03_inventory_daily_etl
21. dws_04_purchase_daily_etl
22. dws_05_quality_daily_etl
23. dws_06_equipment_runtime_daily_etl
24. dws_07_cost_daily_etl

### ADS层 (6个)
25. ads_01_sales_analysis_etl
26. ads_02_production_analysis_etl
27. ads_03_inventory_analysis_etl
28. ads_04_purchase_analysis_etl
29. ads_05_quality_analysis_etl
30. ads_06_business_overview_etl

## 注意事项

1. **数据源配置**: 在创建SQL任务前，需要在DolphinScheduler中配置MySQL数据源
2. **任务类型**: 可以使用SHELL任务执行SQL，或配置SQL任务类型
3. **调度时间**: 建议设置与Airflow相同的调度时间（ODS: 2点, DWD: 3点, DWS: 4点, ADS: 5点）

## 快速访问

- **DolphinScheduler Web**: http://localhost:12345
- **Airflow Web**: http://localhost:8080

---

**建议**: 由于API端点问题，推荐使用Web界面手动创建30个工作流，这样更直观且不容易出错。
