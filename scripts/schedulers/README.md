# 调度任务说明

## DolphinScheduler任务
运行 `create_dolphinscheduler_tasks.py` 创建30个调度任务

## Airflow DAG任务
30个DAG文件已创建在 `airflow/dags/` 目录下

## 任务列表

### ODS层任务 (1-10)
1. ods_01_order_master_etl - 订单主表ETL
2. ods_02_order_detail_etl - 订单明细表ETL
3. ods_03_customer_etl - 客户主表ETL
4. ods_04_product_etl - 产品主表ETL
5. ods_05_production_plan_etl - 生产计划表ETL
6. ods_06_production_order_etl - 生产工单表ETL
7. ods_07_bom_etl - 物料清单表ETL
8. ods_08_material_etl - 物料主表ETL
9. ods_09_inventory_etl - 库存表ETL
10. ods_10_purchase_etl - 采购订单表ETL

### DWD层任务 (11-17)
11. dwd_01_order_fact_etl - 订单事实表ETL
12. dwd_02_production_fact_etl - 生产事实表ETL
13. dwd_03_inventory_fact_etl - 库存事实表ETL
14. dwd_04_purchase_fact_etl - 采购事实表ETL
15. dwd_05_quality_fact_etl - 质量事实表ETL
16. dwd_06_equipment_runtime_etl - 设备运行事实表ETL
17. dwd_07_cost_fact_etl - 成本事实表ETL

### DWS层任务 (18-24)
18. dws_01_order_daily_etl - 订单日汇总ETL
19. dws_02_production_daily_etl - 生产日汇总ETL
20. dws_03_inventory_daily_etl - 库存日汇总ETL
21. dws_04_purchase_daily_etl - 采购日汇总ETL
22. dws_05_quality_daily_etl - 质量日汇总ETL
23. dws_06_equipment_runtime_daily_etl - 设备运行日汇总ETL
24. dws_07_cost_daily_etl - 成本日汇总ETL

### ADS层任务 (25-30)
25. ads_01_sales_analysis_etl - 销售分析报表ETL
26. ads_02_production_analysis_etl - 生产分析报表ETL
27. ads_03_inventory_analysis_etl - 库存分析报表ETL
28. ads_04_purchase_analysis_etl - 采购分析报表ETL
29. ads_05_quality_analysis_etl - 质量分析报表ETL
30. ads_06_business_overview_etl - 综合经营分析报表ETL
