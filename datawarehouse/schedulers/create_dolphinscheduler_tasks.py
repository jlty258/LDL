#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建30个DolphinScheduler调度任务
制造业数仓ETL任务
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../scripts'))

from dolphinscheduler_rest_api import DolphinSchedulerRESTClient
import json

# DolphinScheduler配置
DS_CONFIG = {
    'base_url': 'http://localhost:12345',
    'username': 'admin',
    'password': 'dolphinscheduler123'
}

# MySQL连接配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'database': 'sqlExpert',
    'username': 'sqluser',
    'password': 'sqlpass123'
}

def create_sql_task(task_name, sql_file, description):
    """创建SQL任务定义"""
    sql_path = f"/datawarehouse/sql/{sql_file}"
    
    # 读取SQL文件内容
    sql_content = ""
    try:
        sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', sql_file)
        if os.path.exists(sql_file_path):
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
    except Exception as e:
        print(f"  警告: 无法读取SQL文件 {sql_file}: {e}")
        sql_content = f"-- {description}\nSELECT 1;"
    
    return {
        "name": task_name,
        "description": description,
        "taskType": "SQL",
        "taskParams": {
            "type": "MYSQL",
            "datasource": 1,  # 数据源ID，需要在实际环境中配置
            "sql": sql_content[:5000] if len(sql_content) > 5000 else sql_content,  # 限制长度
            "sqlType": "0",  # 0: 非查询, 1: 查询
            "sendEmail": False,
            "displayRows": 10,
            "localParams": [],
            "resourceList": [],
            "dependence": {},
            "conditionResult": {
                "successNode": [],
                "failedNode": []
            },
            "waitStartTimeout": {}
        },
        "flag": "YES",
        "taskPriority": "MEDIUM",
        "workerGroup": "default",
        "timeoutFlag": "CLOSE",
        "timeoutNotifyStrategy": None,
        "timeout": 0
    }

def create_shell_task(task_name, command, description):
    """创建Shell任务定义"""
    return {
        "name": task_name,
        "description": description,
        "taskType": "SHELL",
        "taskParams": {
            "resourceList": [],
            "localParams": [],
            "rawScript": command,
            "dependence": {},
            "conditionResult": {
                "successNode": [],
                "failedNode": []
            },
            "waitStartTimeout": {}
        },
        "flag": "YES",
        "taskPriority": "MEDIUM",
        "workerGroup": "default",
        "timeoutFlag": "CLOSE",
        "timeoutNotifyStrategy": None,
        "timeout": 0
    }

def create_workflow_definition(workflow_name, tasks, description):
    """创建工作流定义"""
    return {
        "name": workflow_name,
        "description": description,
        "globalParams": [],
        "tasks": tasks,
        "tenantId": 1,
        "timeout": 0,
        "releaseState": "ONLINE",
        "param": None,
        "executionType": "PARALLEL"
    }

def create_all_workflows():
    """创建所有30个工作流"""
    client = DolphinSchedulerRESTClient(**DS_CONFIG)
    
    # 获取或创建项目
    projects = client.get_projects()
    project_list = projects.get('data', {}).get('totalList', [])
    
    if not project_list:
        print("创建项目: 制造业数仓")
        result = client.create_project("制造业数仓", "制造业数据仓库ETL项目")
        if result.get('code') == 0:
            projects = client.get_projects()
            project_list = projects.get('data', {}).get('totalList', [])
        else:
            print(f"❌ 项目创建失败: {result.get('msg')}")
            return
    
    project_code = project_list[0].get('code')
    print(f"使用项目: {project_list[0].get('name')} (代码: {project_code})")
    
    # 定义30个调度任务
    workflows = [
        # ODS层ETL任务 (1-10)
        {
            "name": "ods_01_order_master_etl",
            "description": "ODS层-订单主表ETL",
            "task": create_sql_task("订单主表ETL", "02_ods_tables.sql", "订单主表数据加载")
        },
        {
            "name": "ods_02_order_detail_etl",
            "description": "ODS层-订单明细表ETL",
            "task": create_sql_task("订单明细表ETL", "02_ods_tables.sql", "订单明细表数据加载")
        },
        {
            "name": "ods_03_customer_etl",
            "description": "ODS层-客户主表ETL",
            "task": create_sql_task("客户主表ETL", "02_ods_tables.sql", "客户主表数据加载")
        },
        {
            "name": "ods_04_product_etl",
            "description": "ODS层-产品主表ETL",
            "task": create_sql_task("产品主表ETL", "02_ods_tables.sql", "产品主表数据加载")
        },
        {
            "name": "ods_05_production_plan_etl",
            "description": "ODS层-生产计划表ETL",
            "task": create_sql_task("生产计划表ETL", "02_ods_tables.sql", "生产计划表数据加载")
        },
        {
            "name": "ods_06_production_order_etl",
            "description": "ODS层-生产工单表ETL",
            "task": create_sql_task("生产工单表ETL", "02_ods_tables.sql", "生产工单表数据加载")
        },
        {
            "name": "ods_07_bom_etl",
            "description": "ODS层-物料清单表ETL",
            "task": create_sql_task("物料清单表ETL", "02_ods_tables.sql", "物料清单表数据加载")
        },
        {
            "name": "ods_08_material_etl",
            "description": "ODS层-物料主表ETL",
            "task": create_sql_task("物料主表ETL", "02_ods_tables.sql", "物料主表数据加载")
        },
        {
            "name": "ods_09_inventory_etl",
            "description": "ODS层-库存表ETL",
            "task": create_sql_task("库存表ETL", "02_ods_tables.sql", "库存表数据加载")
        },
        {
            "name": "ods_10_purchase_etl",
            "description": "ODS层-采购订单表ETL",
            "task": create_sql_task("采购订单表ETL", "02_ods_tables.sql", "采购订单表数据加载")
        },
        
        # DWD层ETL任务 (11-17)
        {
            "name": "dwd_01_order_fact_etl",
            "description": "DWD层-订单事实表ETL",
            "task": create_sql_task("订单事实表ETL", "07_complex_etl.sql", "订单事实表数据清洗转换")
        },
        {
            "name": "dwd_02_production_fact_etl",
            "description": "DWD层-生产事实表ETL",
            "task": create_sql_task("生产事实表ETL", "07_complex_etl.sql", "生产事实表数据清洗转换")
        },
        {
            "name": "dwd_03_inventory_fact_etl",
            "description": "DWD层-库存事实表ETL",
            "task": create_sql_task("库存事实表ETL", "07_complex_etl.sql", "库存事实表数据清洗转换")
        },
        {
            "name": "dwd_04_purchase_fact_etl",
            "description": "DWD层-采购事实表ETL",
            "task": create_sql_task("采购事实表ETL", "07_complex_etl.sql", "采购事实表数据清洗转换")
        },
        {
            "name": "dwd_05_quality_fact_etl",
            "description": "DWD层-质量事实表ETL",
            "task": create_sql_task("质量事实表ETL", "07_complex_etl.sql", "质量事实表数据清洗转换")
        },
        {
            "name": "dwd_06_equipment_runtime_etl",
            "description": "DWD层-设备运行事实表ETL",
            "task": create_sql_task("设备运行事实表ETL", "07_complex_etl.sql", "设备运行事实表数据清洗转换")
        },
        {
            "name": "dwd_07_cost_fact_etl",
            "description": "DWD层-成本事实表ETL",
            "task": create_sql_task("成本事实表ETL", "07_complex_etl.sql", "成本事实表数据清洗转换")
        },
        
        # DWS层ETL任务 (18-24)
        {
            "name": "dws_01_order_daily_etl",
            "description": "DWS层-订单日汇总ETL",
            "task": create_sql_task("订单日汇总ETL", "07_complex_etl.sql", "订单日汇总数据计算")
        },
        {
            "name": "dws_02_production_daily_etl",
            "description": "DWS层-生产日汇总ETL",
            "task": create_sql_task("生产日汇总ETL", "07_complex_etl.sql", "生产日汇总数据计算")
        },
        {
            "name": "dws_03_inventory_daily_etl",
            "description": "DWS层-库存日汇总ETL",
            "task": create_sql_task("库存日汇总ETL", "07_complex_etl.sql", "库存日汇总数据计算")
        },
        {
            "name": "dws_04_purchase_daily_etl",
            "description": "DWS层-采购日汇总ETL",
            "task": create_sql_task("采购日汇总ETL", "07_complex_etl.sql", "采购日汇总数据计算")
        },
        {
            "name": "dws_05_quality_daily_etl",
            "description": "DWS层-质量日汇总ETL",
            "task": create_sql_task("质量日汇总ETL", "07_complex_etl.sql", "质量日汇总数据计算")
        },
        {
            "name": "dws_06_equipment_runtime_daily_etl",
            "description": "DWS层-设备运行日汇总ETL",
            "task": create_sql_task("设备运行日汇总ETL", "07_complex_etl.sql", "设备运行日汇总数据计算")
        },
        {
            "name": "dws_07_cost_daily_etl",
            "description": "DWS层-成本日汇总ETL",
            "task": create_sql_task("成本日汇总ETL", "07_complex_etl.sql", "成本日汇总数据计算")
        },
        
        # ADS层ETL任务 (25-30)
        {
            "name": "ads_01_sales_analysis_etl",
            "description": "ADS层-销售分析报表ETL",
            "task": create_sql_task("销售分析报表ETL", "07_complex_etl.sql", "销售分析报表数据生成")
        },
        {
            "name": "ads_02_production_analysis_etl",
            "description": "ADS层-生产分析报表ETL",
            "task": create_sql_task("生产分析报表ETL", "07_complex_etl.sql", "生产分析报表数据生成")
        },
        {
            "name": "ads_03_inventory_analysis_etl",
            "description": "ADS层-库存分析报表ETL",
            "task": create_sql_task("库存分析报表ETL", "07_complex_etl.sql", "库存分析报表数据生成")
        },
        {
            "name": "ads_04_purchase_analysis_etl",
            "description": "ADS层-采购分析报表ETL",
            "task": create_sql_task("采购分析报表ETL", "07_complex_etl.sql", "采购分析报表数据生成")
        },
        {
            "name": "ads_05_quality_analysis_etl",
            "description": "ADS层-质量分析报表ETL",
            "task": create_sql_task("质量分析报表ETL", "07_complex_etl.sql", "质量分析报表数据生成")
        },
        {
            "name": "ads_06_business_overview_etl",
            "description": "ADS层-综合经营分析报表ETL",
            "task": create_sql_task("综合经营分析报表ETL", "07_complex_etl.sql", "综合经营分析报表数据生成")
        },
    ]
    
    # 创建30个工作流
    created_count = 0
    for i, workflow_info in enumerate(workflows, 1):
        try:
            workflow_def = create_workflow_definition(
                workflow_info["name"],
                [workflow_info["task"]],
                workflow_info["description"]
            )
            
            result = client.create_process_definition(project_code, workflow_def)
            
            if result.get('code') == 0:
                print(f"✓ [{i:2d}/30] 创建工作流成功: {workflow_info['name']}")
                created_count += 1
            else:
                print(f"❌ [{i:2d}/30] 创建工作流失败: {workflow_info['name']} - {result.get('msg')}")
        except Exception as e:
            print(f"❌ [{i:2d}/30] 创建工作流异常: {workflow_info['name']} - {e}")
    
    print(f"\n完成! 成功创建 {created_count}/30 个工作流")
    return created_count

if __name__ == "__main__":
    print("=" * 60)
    print("创建DolphinScheduler调度任务")
    print("=" * 60)
    create_all_workflows()
