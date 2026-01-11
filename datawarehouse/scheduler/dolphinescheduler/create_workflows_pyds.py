#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用 PyDolphinScheduler 创建30个DolphinScheduler调度任务
制造业数仓ETL任务

使用方法:
    python create_workflows_pyds.py

环境变量配置:
    DS_USERNAME: DolphinScheduler 用户名（默认: admin）
    DS_PASSWORD: DolphinScheduler 密码（默认: dolphinscheduler123）
    DS_HOST: DolphinScheduler API 服务器地址（默认: localhost）
    DS_PORT: DolphinScheduler API 服务器端口（默认: 12345）
    DS_PROJECT_NAME: 项目名称（默认: 制造业数仓）
    DS_DATASOURCE_NAME: 数据源名称（默认: MySQL）
    DS_DATASOURCE_ID: 数据源ID（默认: 1）
"""

import os
import sys

# 添加当前目录到路径
sys.path.append(os.path.dirname(__file__))

from pydolphinscheduler_client import PyDolphinSchedulerClient, get_client_from_env, PYDOLPHINSCHEDULER_AVAILABLE

if not PYDOLPHINSCHEDULER_AVAILABLE:
    print("❌ PyDolphinScheduler 未安装")
    print("   请运行: pip install apache-dolphinscheduler")
    sys.exit(1)


def create_all_workflows():
    """创建所有30个工作流"""
    # 从环境变量获取配置
    client = get_client_from_env()
    
    datasource_name = os.getenv('DS_DATASOURCE_NAME', 'MySQL')
    datasource_id = int(os.getenv('DS_DATASOURCE_ID', '1'))
    
    print(f"\n数据源配置: {datasource_name} (ID: {datasource_id})")
    print("=" * 60)
    
    # 定义30个调度任务
    workflows = [
        # ODS层ETL任务 (1-10)
        {
            "name": "ods_01_order_master_etl",
            "description": "ODS层-订单主表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_02_order_detail_etl",
            "description": "ODS层-订单明细表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_03_customer_etl",
            "description": "ODS层-客户主表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_04_product_etl",
            "description": "ODS层-产品主表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_05_production_plan_etl",
            "description": "ODS层-生产计划表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_06_production_order_etl",
            "description": "ODS层-生产工单表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_07_bom_etl",
            "description": "ODS层-物料清单表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_08_material_etl",
            "description": "ODS层-物料主表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_09_inventory_etl",
            "description": "ODS层-库存表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ods_10_purchase_etl",
            "description": "ODS层-采购订单表ETL",
            "sql_file": "02_ods_tables.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        
        # DWD层ETL任务 (11-17)
        {
            "name": "dwd_01_order_fact_etl",
            "description": "DWD层-订单事实表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dwd_02_production_fact_etl",
            "description": "DWD层-生产事实表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dwd_03_inventory_fact_etl",
            "description": "DWD层-库存事实表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dwd_04_purchase_fact_etl",
            "description": "DWD层-采购事实表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dwd_05_quality_fact_etl",
            "description": "DWD层-质量事实表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dwd_06_equipment_runtime_etl",
            "description": "DWD层-设备运行事实表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dwd_07_cost_fact_etl",
            "description": "DWD层-成本事实表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        
        # DWS层ETL任务 (18-24)
        {
            "name": "dws_01_order_daily_etl",
            "description": "DWS层-订单日汇总ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dws_02_production_daily_etl",
            "description": "DWS层-生产日汇总ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dws_03_inventory_daily_etl",
            "description": "DWS层-库存日汇总ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dws_04_purchase_daily_etl",
            "description": "DWS层-采购日汇总ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dws_05_quality_daily_etl",
            "description": "DWS层-质量日汇总ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dws_06_equipment_runtime_daily_etl",
            "description": "DWS层-设备运行日汇总ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "dws_07_cost_daily_etl",
            "description": "DWS层-成本日汇总ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        
        # ADS层ETL任务 (25-30)
        {
            "name": "ads_01_sales_analysis_etl",
            "description": "ADS层-销售分析报表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ads_02_production_analysis_etl",
            "description": "ADS层-生产分析报表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ads_03_inventory_analysis_etl",
            "description": "ADS层-库存分析报表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ads_04_purchase_analysis_etl",
            "description": "ADS层-采购分析报表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ads_05_quality_analysis_etl",
            "description": "ADS层-质量分析报表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
        {
            "name": "ads_06_business_overview_etl",
            "description": "ADS层-综合经营分析报表ETL",
            "sql_file": "07_complex_etl.sql",
            "schedule": None  # 先不设置调度，创建后再手动配置
        },
    ]
    
    # 创建工作流
    created_count = 0
    failed_count = 0
    
    print(f"\n开始创建 {len(workflows)} 个工作流...\n")
    
    for i, workflow_info in enumerate(workflows, 1):
        try:
            success = client.create_sql_workflow_from_file(
                workflow_name=workflow_info["name"],
                description=workflow_info["description"],
                sql_file=workflow_info["sql_file"],
                schedule=workflow_info.get("schedule"),
                datasource_name=datasource_name,
                datasource_id=datasource_id
            )
            
            if success:
                print(f"✓ [{i:2d}/{len(workflows)}] {workflow_info['name']}")
                created_count += 1
            else:
                print(f"❌ [{i:2d}/{len(workflows)}] {workflow_info['name']}")
                failed_count += 1
                
        except Exception as e:
            print(f"❌ [{i:2d}/{len(workflows)}] {workflow_info['name']} - {e}")
            failed_count += 1
    
    print("\n" + "=" * 60)
    print(f"完成! 成功: {created_count}, 失败: {failed_count}, 总计: {len(workflows)}")
    print("=" * 60)
    
    return created_count


if __name__ == "__main__":
    print("=" * 60)
    print("使用 PyDolphinScheduler 创建 DolphinScheduler 调度任务")
    print("=" * 60)
    
    try:
        create_all_workflows()
    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
