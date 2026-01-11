#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在 DolphinScheduler 中创建 MySQL 数据源

注意: 此脚本使用 REST API，但 DolphinScheduler 3.3.0 可能不支持通过 API 创建数据源
如果失败，请手动在 Web UI 中创建数据源
"""

import os
import sys
import requests

# 配置
DS_BASE_URL = os.getenv('DS_BASE_URL', 'http://dolphinscheduler:12345')
DS_USERNAME = os.getenv('DS_USERNAME', 'admin')
DS_PASSWORD = os.getenv('DS_PASSWORD', 'dolphinscheduler123')

MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql-db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'sqlExpert')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME', 'sqluser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'sqlpass123')

def login():
    """登录获取 session"""
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
        print("✓ 登录成功")
        return session
    else:
        raise Exception(f"登录失败: {result.get('msg')}")

def create_datasource(session, datasource_name="MySQL"):
    """创建 MySQL 数据源"""
    # DolphinScheduler 数据源创建 API
    endpoint = f"{DS_BASE_URL}/dolphinscheduler/datasources"
    
    datasource_data = {
        "name": datasource_name,
        "type": "MYSQL",
        "note": "MySQL数据源",
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "database": MYSQL_DATABASE,
        "userName": MYSQL_USERNAME,
        "password": MYSQL_PASSWORD,
        "other": {
            "useSSL": "false",
            "serverTimezone": "Asia/Shanghai"
        }
    }
    
    try:
        response = session.post(endpoint, json=datasource_data, timeout=30)
        result = response.json()
        
        # 201 或 200 状态码都可能是成功
        if response.status_code in [200, 201]:
            if result.get('code') == 0:
                datasource_id = result.get('data', {}).get('id', 'N/A')
                print(f"✓ 数据源 '{datasource_name}' 创建成功 (ID: {datasource_id})")
                return True
            else:
                msg = result.get('msg', '未知错误')
                print(f"⚠ 数据源创建返回错误: {msg}")
                # 可能数据源已存在
                if "already exists" in msg.lower() or "已存在" in msg or "duplicate" in msg.lower():
                    print(f"  数据源 '{datasource_name}' 可能已存在")
                    return True
                return False
        else:
            print(f"⚠ API 调用失败: {response.status_code}")
            print(f"  响应: {response.text[:200]}")
            # 即使状态码不是 200/201，如果 code=0 也可能是成功
            if result.get('code') == 0:
                print(f"  但返回 code=0，可能已创建成功")
                return True
            return False
    except Exception as e:
        print(f"❌ 创建数据源失败: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("创建 DolphinScheduler MySQL 数据源")
    print("=" * 60)
    print(f"\n数据源配置:")
    print(f"  名称: MySQL")
    print(f"  主机: {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"  数据库: {MYSQL_DATABASE}")
    print(f"  用户: {MYSQL_USERNAME}")
    
    try:
        session = login()
        success = create_datasource(session)
        
        if success:
            print("\n✓ 数据源创建完成")
            print("\n提示: 如果数据源创建失败，请手动在 DolphinScheduler Web UI 中创建：")
            print(f"1. 访问: http://{os.getenv('TAILSCALE_IP', '100.126.111.70')}:12345")
            print("2. 进入 '数据源中心'")
            print("3. 创建 MySQL 数据源，名称为 'MySQL'")
        else:
            print("\n❌ 数据源创建失败")
            print("\n请手动在 DolphinScheduler Web UI 中创建数据源")
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        print("\n请手动在 DolphinScheduler Web UI 中创建数据源：")
        print(f"1. 访问: http://{os.getenv('TAILSCALE_IP', '100.126.111.70')}:12345")
        print("2. 进入 '数据源中心'")
        print("3. 创建 MySQL 数据源，名称为 'MySQL'")
        sys.exit(1)
