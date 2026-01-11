#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成DolphinScheduler工作流定义JSON文件
用于在Web界面中导入工作流（UTF-8编码）
"""

import os
import json

def read_sql_file(sql_file_name: str) -> str:
    """读取SQL文件内容（UTF-8编码）"""
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
    sql_path = os.path.join(sql_dir, sql_file_name)
    
    if not os.path.exists(sql_path):
        return f"-- {sql_file_name}\nSELECT 1;"
    
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"警告: 读取SQL文件失败: {e}")
        return f"-- {sql_file_name}\nSELECT 1;"

def create_workflow_json(workflow_name: str, description: str, sql_content: str, 
                        datasource_id: int = 1, cron: str = None) -> dict:
    """创建工作流JSON定义"""
    workflow = {
        "name": workflow_name,
        "description": description,
        "globalParams": [],
        "tasks": [
            {
                "name": f"{workflow_name}_task",
                "description": description,
                "taskType": "SQL",
                "taskParams": {
                    "type": "MYSQL",
                    "datasource": datasource_id,
                    "sql": sql_content[:5000] if len(sql_content) > 5000 else sql_content,
                    "sqlType": "0",
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
        ],
        "tenantId": 1,
        "timeout": 0,
        "releaseState": "ONLINE",
        "param": None,
        "executionType": "PARALLEL"
    }
    
    if cron:
        workflow["schedule"] = {
            "crontab": cron,
            "timezoneId": "Asia/Shanghai"
        }
    
    return workflow

def main():
    """生成所有工作流的JSON文件"""
    print("=" * 60)
    print("生成DolphinScheduler工作流定义JSON文件")
    print("=" * 60)
    
    output_dir = os.path.join(os.path.dirname(__file__), 'workflow_definitions')
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取ETL SQL
    etl_sql = read_sql_file("07_complex_etl.sql")
    
    workflows = [
        {
            "name": "ods_01_order_etl",
            "description": "ODS层-订单数据ETL",
            "sql": "-- 订单相关表数据加载\nSELECT COUNT(*) FROM ods_order_master;",
            "cron": "0 0 2 * * ?"
        },
        {
            "name": "ods_02_production_etl",
            "description": "ODS层-生产数据ETL",
            "sql": "-- 生产相关表数据加载\nSELECT COUNT(*) FROM ods_production_order;",
            "cron": "0 0 2 * * ?"
        },
        {
            "name": "ods_03_inventory_etl",
            "description": "ODS层-库存数据ETL",
            "sql": "-- 库存相关表数据加载\nSELECT COUNT(*) FROM ods_inventory;",
            "cron": "0 0 2 * * ?"
        },
        {
            "name": "ods_04_purchase_etl",
            "description": "ODS层-采购数据ETL",
            "sql": "-- 采购相关表数据加载\nSELECT COUNT(*) FROM ods_purchase_order;",
            "cron": "0 0 2 * * ?"
        },
        {
            "name": "ods_05_quality_etl",
            "description": "ODS层-质量数据ETL",
            "sql": "-- 质量相关表数据加载\nSELECT COUNT(*) FROM ods_quality_inspection;",
            "cron": "0 0 2 * * ?"
        },
        {
            "name": "dwd_01_order_fact_etl",
            "description": "DWD层-订单事实表ETL",
            "sql": etl_sql[:5000],  # 使用实际ETL SQL
            "cron": "0 0 3 * * ?"
        },
        {
            "name": "dwd_02_production_fact_etl",
            "description": "DWD层-生产事实表ETL",
            "sql": "-- 生产事实表数据清洗转换\nINSERT INTO dwd_production_fact SELECT * FROM ods_production_order;",
            "cron": "0 0 3 * * ?"
        },
        {
            "name": "dwd_03_inventory_fact_etl",
            "description": "DWD层-库存事实表ETL",
            "sql": "-- 库存事实表数据清洗转换\nINSERT INTO dwd_inventory_fact SELECT * FROM ods_inventory;",
            "cron": "0 0 3 * * ?"
        },
        {
            "name": "dwd_04_purchase_fact_etl",
            "description": "DWD层-采购事实表ETL",
            "sql": "-- 采购事实表数据清洗转换\nINSERT INTO dwd_purchase_fact SELECT * FROM ods_purchase_order;",
            "cron": "0 0 3 * * ?"
        },
        {
            "name": "dws_01_order_daily_etl",
            "description": "DWS层-订单日汇总ETL",
            "sql": "-- 订单日汇总数据计算\nINSERT INTO dws_order_daily SELECT * FROM dwd_order_fact;",
            "cron": "0 0 4 * * ?"
        },
        {
            "name": "dws_02_production_daily_etl",
            "description": "DWS层-生产日汇总ETL",
            "sql": "-- 生产日汇总数据计算\nINSERT INTO dws_production_daily SELECT * FROM dwd_production_fact;",
            "cron": "0 0 4 * * ?"
        },
        {
            "name": "dws_03_inventory_daily_etl",
            "description": "DWS层-库存日汇总ETL",
            "sql": "-- 库存日汇总数据计算\nINSERT INTO dws_inventory_daily SELECT * FROM dwd_inventory_fact;",
            "cron": "0 0 4 * * ?"
        },
        {
            "name": "ads_01_sales_analysis_etl",
            "description": "ADS层-销售分析报表ETL",
            "sql": "-- 销售分析报表数据生成\nINSERT INTO ads_sales_analysis SELECT * FROM dws_order_daily;",
            "cron": "0 0 5 * * ?"
        },
        {
            "name": "ads_02_production_analysis_etl",
            "description": "ADS层-生产分析报表ETL",
            "sql": "-- 生产分析报表数据生成\nINSERT INTO ads_production_analysis SELECT * FROM dws_production_daily;",
            "cron": "0 0 5 * * ?"
        },
        {
            "name": "ads_03_business_overview_etl",
            "description": "ADS层-综合经营分析报表ETL",
            "sql": "-- 综合经营分析报表数据生成\nINSERT INTO ads_business_overview SELECT * FROM dws_order_daily;",
            "cron": "0 0 5 * * ?"
        },
    ]
    
    print(f"\n生成 {len(workflows)} 个工作流定义文件...")
    
    for i, wf_info in enumerate(workflows, 1):
        workflow_json = create_workflow_json(
            workflow_name=wf_info['name'],
            description=wf_info['description'],
            sql_content=wf_info['sql'],
            cron=wf_info.get('cron')
        )
        
        output_file = os.path.join(output_dir, f"{wf_info['name']}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(workflow_json, f, ensure_ascii=False, indent=2)
        
        print(f"  ✓ [{i:2d}/{len(workflows)}] {wf_info['name']}.json")
    
    print(f"\n✓ 所有工作流定义文件已生成到: {output_dir}")
    print("\n提示: 可以在DolphinScheduler Web界面中参考这些JSON文件创建工作流")

if __name__ == "__main__":
    main()
