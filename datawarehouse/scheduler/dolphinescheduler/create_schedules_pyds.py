#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用 PyDolphinScheduler 为工作流创建定时调度

注意：PyDolphinScheduler 4.1.0 在创建 Workflow 时可以设置 schedule，
但需要正确的 cron 格式。由于之前创建时 schedule 为 None，
这里我们需要重新创建工作流并设置正确的调度，或者使用其他方式。
"""

import os
import sys

sys.path.append(os.path.dirname(__file__))
from pydolphinscheduler_client import get_client_from_env, PYDOLPHINSCHEDULER_AVAILABLE

if not PYDOLPHINSCHEDULER_AVAILABLE:
    print("❌ PyDolphinScheduler 未安装")
    print("   请运行: pip install apache-dolphinscheduler")
    sys.exit(1)

# 工作流调度配置
WORKFLOW_SCHEDULES = [
    # ODS层ETL任务 - 每天凌晨2点执行
    {"name": "ods_01_order_master_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_02_order_detail_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_03_customer_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_04_product_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_05_production_plan_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_06_production_order_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_07_bom_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_08_material_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_09_inventory_etl", "cron": "0 0 2 * * ?"},
    {"name": "ods_10_purchase_etl", "cron": "0 0 2 * * ?"},
    
    # DWD层ETL任务 - 每天凌晨3点执行
    {"name": "dwd_01_order_fact_etl", "cron": "0 0 3 * * ?"},
    {"name": "dwd_02_production_fact_etl", "cron": "0 0 3 * * ?"},
    {"name": "dwd_03_inventory_fact_etl", "cron": "0 0 3 * * ?"},
    {"name": "dwd_04_purchase_fact_etl", "cron": "0 0 3 * * ?"},
    {"name": "dwd_05_quality_fact_etl", "cron": "0 0 3 * * ?"},
    {"name": "dwd_06_equipment_runtime_etl", "cron": "0 0 3 * * ?"},
    {"name": "dwd_07_cost_fact_etl", "cron": "0 0 3 * * ?"},
    
    # DWS层ETL任务 - 每天凌晨4点执行
    {"name": "dws_01_order_daily_etl", "cron": "0 0 4 * * ?"},
    {"name": "dws_02_production_daily_etl", "cron": "0 0 4 * * ?"},
    {"name": "dws_03_inventory_daily_etl", "cron": "0 0 4 * * ?"},
    {"name": "dws_04_purchase_daily_etl", "cron": "0 0 4 * * ?"},
    {"name": "dws_05_quality_daily_etl", "cron": "0 0 4 * * ?"},
    {"name": "dws_06_equipment_runtime_daily_etl", "cron": "0 0 4 * * ?"},
    {"name": "dws_07_cost_daily_etl", "cron": "0 0 4 * * ?"},
    
    # ADS层ETL任务 - 每天凌晨5点执行
    {"name": "ads_01_sales_analysis_etl", "cron": "0 0 5 * * ?"},
    {"name": "ads_02_production_analysis_etl", "cron": "0 0 5 * * ?"},
    {"name": "ads_03_inventory_analysis_etl", "cron": "0 0 5 * * ?"},
    {"name": "ads_04_purchase_analysis_etl", "cron": "0 0 5 * * ?"},
    {"name": "ads_05_quality_analysis_etl", "cron": "0 0 5 * * ?"},
    {"name": "ads_06_business_overview_etl", "cron": "0 0 5 * * ?"},
]

def update_workflow_schedule():
    """
    更新工作流的调度配置
    
    注意：PyDolphinScheduler 4.1.0 不支持直接更新已存在工作流的调度。
    我们需要通过 Java Gateway 的 API 来更新，或者重新创建工作流。
    这里我们尝试使用 PyDolphinScheduler 的底层 API。
    """
    from pydolphinscheduler.core.workflow import Workflow
    from pydolphinscheduler.java_gateway import GatewayEntryPoint
    
    client = get_client_from_env()
    
    print("=" * 60)
    print("使用 PyDolphinScheduler 配置工作流调度")
    print("=" * 60)
    print(f"\n项目: {client.project_name}")
    print(f"开始为 {len(WORKFLOW_SCHEDULES)} 个工作流配置调度...\n")
    
    success_count = 0
    failed_count = 0
    
    # 获取 Java Gateway
    gateway = GatewayEntryPoint()
    
    for i, schedule_info in enumerate(WORKFLOW_SCHEDULES, 1):
        workflow_name = schedule_info['name']
        cron_expression = schedule_info['cron']
        
        try:
            # 尝试通过 Java Gateway 更新调度
            # 首先需要获取工作流代码
            # 注意：这需要访问 DolphinScheduler 的内部 API
            
            # 由于 PyDolphinScheduler 4.1.0 的限制，我们尝试另一种方式：
            # 使用 Workflow 对象来更新调度配置
            
            # 创建一个临时的 Workflow 对象来获取调度配置
            # 但实际上我们需要更新已存在的工作流
            
            # PyDolphinScheduler 4.1.0 可能不支持直接更新已存在工作流的调度
            # 建议：在 Web UI 中手动配置，或者重新创建工作流时设置 schedule
            
            print(f"⚠ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name}")
            print(f"   PyDolphinScheduler 4.1.0 不支持直接更新已存在工作流的调度")
            print(f"   建议：在 DolphinScheduler Web UI 中手动配置调度")
            print(f"   调度表达式: {cron_expression}")
            failed_count += 1
            
        except Exception as e:
            print(f"❌ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name} - {e}")
            failed_count += 1
    
    print("\n" + "=" * 60)
    print("注意：PyDolphinScheduler 4.1.0 不支持直接更新已存在工作流的调度")
    print("=" * 60)
    print("\n建议方案：")
    print("1. 在 DolphinScheduler Web UI 中手动配置调度")
    print("2. 或者重新创建工作流时设置 schedule 参数")
    print("3. 或者使用 REST API 创建调度（需要 DolphinScheduler 3.4.0+）")
    
    return success_count

def create_workflows_with_schedule():
    """
    重新创建工作流并设置调度
    
    注意：这会删除并重新创建工作流，请谨慎使用
    """
    from pydolphinscheduler_client import PyDolphinSchedulerClient
    
    client = get_client_from_env()
    
    datasource_name = os.getenv('DS_DATASOURCE_NAME', 'MySQL')
    datasource_id = int(os.getenv('DS_DATASOURCE_ID', '1'))
    
    print("=" * 60)
    print("重新创建工作流并设置调度")
    print("=" * 60)
    print("\n⚠ 警告：这将重新创建所有工作流！")
    print("如果工作流已存在，可能会被覆盖。\n")
    
    success_count = 0
    failed_count = 0
    
    for i, schedule_info in enumerate(WORKFLOW_SCHEDULES, 1):
        workflow_name = schedule_info['name']
        cron_expression = schedule_info['cron']
        
        # 根据工作流名称确定 SQL 文件
        if workflow_name.startswith('ods_'):
            sql_file = "02_ods_tables.sql"
        else:
            sql_file = "07_complex_etl.sql"
        
        try:
            # 重新创建工作流并设置调度
            success = client.create_sql_workflow_from_file(
                workflow_name=workflow_name,
                description=f"{workflow_name} - 已配置调度",
                sql_file=sql_file,
                schedule=cron_expression,  # 设置调度
                datasource_name=datasource_name,
                datasource_id=datasource_id
            )
            
            if success:
                print(f"✓ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name} - 调度: {cron_expression}")
                success_count += 1
            else:
                print(f"❌ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name}")
                failed_count += 1
                
        except Exception as e:
            print(f"❌ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name} - {e}")
            failed_count += 1
    
    print("\n" + "=" * 60)
    print(f"完成! 成功: {success_count}, 失败: {failed_count}, 总计: {len(WORKFLOW_SCHEDULES)}")
    print("=" * 60)
    
    return success_count

if __name__ == "__main__":
    print("=" * 60)
    print("使用 PyDolphinScheduler 配置工作流调度")
    print("=" * 60)
    print("\n选项：")
    print("1. 尝试更新已存在工作流的调度（可能不支持）")
    print("2. 重新创建工作流并设置调度（会覆盖现有工作流）")
    print("\n由于 PyDolphinScheduler 4.1.0 的限制，")
    print("建议使用方案 2 重新创建工作流并设置调度。")
    print("\n开始执行方案 2...\n")
    
    try:
        create_workflows_with_schedule()
    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
