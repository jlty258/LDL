#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试创建工作流
"""

import os
import sys
sys.path.append(os.path.dirname(__file__))

from pydolphinscheduler_client import get_client_from_env

def test_create_workflow():
    """测试创建一个简单的工作流"""
    client = get_client_from_env()
    
    # 尝试创建一个简单的工作流
    success = client.create_sql_workflow_from_file(
        workflow_name="test_workflow_001",
        description="测试工作流",
        sql_file="02_ods_tables.sql",
        schedule=None,
        datasource_name="MySQL",
        datasource_id=1
    )
    
    if success:
        print("✓ 测试工作流创建成功")
    else:
        print("❌ 测试工作流创建失败")
        print("\n提示: 如果错误信息显示找不到数据源，请先在 DolphinScheduler Web UI 中创建数据源：")
        print("1. 登录 DolphinScheduler Web UI: http://localhost:12345")
        print("2. 进入 '数据源中心'")
        print("3. 创建 MySQL 数据源，名称为 'MySQL'")
        print("4. 然后重新运行此脚本")

if __name__ == "__main__":
    test_create_workflow()
