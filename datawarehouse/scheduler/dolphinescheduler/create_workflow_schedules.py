#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为 DolphinScheduler 工作流创建定时调度

使用 REST API 为已创建的工作流配置调度时间
"""

import os
import sys
import datetime
import requests

# 配置
DS_BASE_URL = os.getenv('DS_BASE_URL', 'http://dolphinscheduler:12345')
DS_USERNAME = os.getenv('DS_USERNAME', 'admin')
DS_PASSWORD = os.getenv('DS_PASSWORD', 'dolphinscheduler123')
DS_PROJECT_NAME = os.getenv('DS_PROJECT_NAME', '制造业数仓')

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

def login():
    """登录获取session"""
    session = requests.Session()
    login_url = f"{DS_BASE_URL}/dolphinscheduler/login"
    data = {
        "userName": DS_USERNAME,
        "userPassword": DS_PASSWORD
    }
    response = session.post(login_url, data=data, timeout=10)
    response.raise_for_status()
    result = response.json()
    if result.get('code') == 0:
        return session
    else:
        raise Exception(f"登录失败: {result.get('msg')}")

def get_project_code(session):
    """获取项目代码"""
    endpoint = f"{DS_BASE_URL}/dolphinscheduler/projects"
    params = {
        "pageNo": 1,
        "pageSize": 10,
        "searchVal": DS_PROJECT_NAME
    }
    response = session.get(endpoint, params=params, timeout=30)
    response.raise_for_status()
    result = response.json()
    if result.get('code') == 0:
        project_list = result.get('data', {}).get('totalList', [])
        for project in project_list:
            if project.get('name') == DS_PROJECT_NAME:
                return project.get('code')
    return None

def get_workflow_code(session, project_code, workflow_name):
    """根据工作流名称获取工作流代码"""
    endpoint = f"{DS_BASE_URL}/dolphinscheduler/projects/{project_code}/process-definition"
    params = {
        "pageNo": 1,
        "pageSize": 100
    }
    try:
        response = session.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        # 检查响应内容类型
        if response.headers.get('content-type', '').startswith('application/json'):
            result = response.json()
            if result.get('code') == 0:
                workflow_list = result.get('data', {}).get('totalList', [])
                for workflow in workflow_list:
                    if workflow.get('name') == workflow_name:
                        return workflow.get('code')
        else:
            print(f"   警告: 响应不是JSON格式: {response.text[:100]}")
    except Exception as e:
        print(f"   获取工作流列表失败: {e}")
    return None

def create_schedule_for_workflow(session, project_code, workflow_code, cron_expression):
    """为工作流创建调度"""
    endpoint = f"{DS_BASE_URL}/dolphinscheduler/projects/{project_code}/schedules"
    
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = "2099-12-31 23:59:59"
    
    schedule_data = {
        "processDefinitionCode": workflow_code,
        "schedule": {
            "crontab": cron_expression,
            "timezoneId": "Asia/Shanghai",
            "startTime": start_time,
            "endTime": end_time,
            "failureStrategy": "END",
            "warningType": "NONE",
            "warningGroupId": 0,
            "processInstancePriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": None
        }
    }
    
    try:
        response = session.post(endpoint, json=schedule_data, timeout=30)
        response.raise_for_status()
        result = response.json()
        return result
    except Exception as e:
        print(f"   错误: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                print(f"   响应: {e.response.text[:200]}")
            except:
                pass
        return None

def main():
    """主函数"""
    print("=" * 60)
    print("为 DolphinScheduler 工作流创建定时调度")
    print("=" * 60)
    
    # 登录
    try:
        session = login()
        print("✓ 登录成功")
    except Exception as e:
        print(f"❌ 登录失败: {e}")
        sys.exit(1)
    
    # 获取项目代码
    project_code = get_project_code(session)
    if not project_code:
        print(f"❌ 未找到项目: {DS_PROJECT_NAME}")
        sys.exit(1)
    
    print(f"\n✓ 项目代码: {project_code}")
    print(f"\n开始为 {len(WORKFLOW_SCHEDULES)} 个工作流配置调度...\n")
    
    success_count = 0
    failed_count = 0
    
    for i, schedule_info in enumerate(WORKFLOW_SCHEDULES, 1):
        workflow_name = schedule_info['name']
        cron_expression = schedule_info['cron']
        
        # 获取工作流代码
        workflow_code = get_workflow_code(session, project_code, workflow_name)
        if not workflow_code:
            print(f"❌ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name} - 未找到工作流")
            failed_count += 1
            continue
        
        # 创建调度
        result = create_schedule_for_workflow(session, project_code, workflow_code, cron_expression)
        
        if result and result.get('code') == 0:
            print(f"✓ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name} - 调度: {cron_expression}")
            success_count += 1
        else:
            msg = result.get('msg', '未知错误') if result else '请求失败'
            # 检查是否已存在调度
            if "already exists" in msg.lower() or "已存在" in msg or "duplicate" in msg.lower():
                print(f"⚠ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name} - 调度已存在")
                success_count += 1
            else:
                print(f"❌ [{i:2d}/{len(WORKFLOW_SCHEDULES)}] {workflow_name} - {msg}")
                failed_count += 1
    
    print("\n" + "=" * 60)
    print(f"完成! 成功: {success_count}, 失败: {failed_count}, 总计: {len(WORKFLOW_SCHEDULES)}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
