#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PyDolphinScheduler 使用示例

这是一个简单的示例，展示如何使用 PyDolphinScheduler 创建工作流
"""

import os
import sys

# 添加当前目录到路径
sys.path.append(os.path.dirname(__file__))

from pydolphinscheduler_client import PyDolphinSchedulerClient, PYDOLPHINSCHEDULER_AVAILABLE

if not PYDOLPHINSCHEDULER_AVAILABLE:
    print("❌ PyDolphinScheduler 未安装")
    print("   请运行: pip install apache-dolphinscheduler")
    sys.exit(1)


def example_simple_workflow():
    """示例1: 创建一个简单的工作流"""
    print("=" * 60)
    print("示例1: 创建简单工作流")
    print("=" * 60)
    
    # 创建客户端
    client = PyDolphinSchedulerClient(
        user=os.getenv('DS_USERNAME', 'admin'),
        password=os.getenv('DS_PASSWORD', 'dolphinscheduler123'),
        host=os.getenv('DS_HOST', 'localhost'),
        port=int(os.getenv('DS_PORT', '12345')),
        project_name=os.getenv('DS_PROJECT_NAME', '制造业数仓')
    )
    
    # 从 SQL 文件创建工作流
    success = client.create_sql_workflow_from_file(
        workflow_name="example_simple_etl",
        description="示例工作流 - 简单ETL",
        sql_file="02_ods_tables.sql",
        schedule="0 0 2 * * ?"  # 每天凌晨2点执行
    )
    
    if success:
        print("✓ 工作流创建成功")
    else:
        print("❌ 工作流创建失败")


def example_custom_workflow():
    """示例2: 创建自定义工作流"""
    print("\n" + "=" * 60)
    print("示例2: 创建自定义工作流")
    print("=" * 60)
    
    client = PyDolphinSchedulerClient(
        user=os.getenv('DS_USERNAME', 'admin'),
        password=os.getenv('DS_PASSWORD', 'dolphinscheduler123'),
        host=os.getenv('DS_HOST', 'localhost'),
        port=int(os.getenv('DS_PORT', '12345')),
        project_name=os.getenv('DS_PROJECT_NAME', '制造业数仓')
    )
    
    # 读取 SQL
    sql_content = client.read_sql_file("02_ods_tables.sql")
    
    # 创建 SQL 任务
    task = client.create_sql_task(
        task_name="custom_sql_task",
        sql_content=sql_content,
        datasource_name="MySQL",
        datasource_id=1
    )
    
    # 创建并提交工作流
    success = client.create_and_submit_workflow(
        workflow_name="example_custom_etl",
        description="示例工作流 - 自定义ETL",
        tasks=[task],
        schedule="0 0 3 * * ?"  # 每天凌晨3点执行
    )
    
    if success:
        print("✓ 自定义工作流创建成功")
    else:
        print("❌ 自定义工作流创建失败")


if __name__ == "__main__":
    print("PyDolphinScheduler 使用示例")
    print("=" * 60)
    
    try:
        # 运行示例1
        example_simple_workflow()
        
        # 运行示例2
        # example_custom_workflow()  # 取消注释以运行
        
    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
