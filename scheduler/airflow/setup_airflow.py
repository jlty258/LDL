#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Airflow 配置脚本
使用 Python 自动配置 Airflow 连接、检查 DAG 状态等
"""

import requests
import json
import sys
import os
from typing import Dict, Optional
from requests.auth import HTTPBasicAuth

# Airflow 配置
# Use Tailscale IP for remote access, or localhost for local access
TAILSCALE_IP = os.getenv("TAILSCALE_IP", "100.126.111.70")
AIRFLOW_BASE_URL = f"http://{TAILSCALE_IP}:8080"
AIRFLOW_API_URL = f"{AIRFLOW_BASE_URL}/api/v1"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

# MySQL 连接配置
MYSQL_CONNECTION = {
    "connection_id": "mysql_default",
    "conn_type": "mysql",
    "host": "mysql-db",  # 容器网络内使用容器名，宿主机使用 Tailscale IP
    "schema": "sqlExpert",
    "login": "sqluser",
    "password": "sqlpass123",
    "port": 3306,
    "description": "MySQL connection for datawarehouse ETL"
}


class AirflowClient:
    """Airflow REST API 客户端"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        
    def check_connection(self) -> bool:
        """检查 Airflow 服务是否可用"""
        try:
            response = self.session.get(f"{self.base_url}/health")
            if response.status_code == 200:
                print("✓ Airflow 服务连接正常")
                return True
            else:
                print(f"✗ Airflow 服务响应异常: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print("✗ 无法连接到 Airflow 服务，请确保服务正在运行")
            return False
        except Exception as e:
            print(f"✗ 检查连接时出错: {e}")
            return False
    
    def get_connection(self, connection_id: str) -> Optional[Dict]:
        """获取连接信息"""
        try:
            response = self.session.get(f"{self.base_url}/connections/{connection_id}")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                print(f"✗ 获取连接失败: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"✗ 获取连接时出错: {e}")
            return None
    
    def create_or_update_connection(self, connection_config: Dict) -> bool:
        """创建或更新连接"""
        connection_id = connection_config["connection_id"]
        existing = self.get_connection(connection_id)
        
        # 准备连接数据
        connection_data = {
            "connection_id": connection_id,
            "conn_type": connection_config["conn_type"],
            "host": connection_config["host"],
            "schema": connection_config["schema"],
            "login": connection_config["login"],
            "password": connection_config["password"],
            "port": connection_config["port"],
            "description": connection_config.get("description", ""),
        }
        
        try:
            if existing:
                # 更新现有连接
                response = self.session.patch(
                    f"{self.base_url}/connections/{connection_id}",
                    json=connection_data,
                    headers={"Content-Type": "application/json"}
                )
                if response.status_code == 200:
                    print(f"✓ 连接 '{connection_id}' 已更新")
                    return True
                else:
                    print(f"✗ 更新连接失败: {response.status_code} - {response.text}")
                    return False
            else:
                # 创建新连接
                response = self.session.post(
                    f"{self.base_url}/connections",
                    json=connection_data,
                    headers={"Content-Type": "application/json"}
                )
                if response.status_code == 200:
                    print(f"✓ 连接 '{connection_id}' 已创建")
                    return True
                else:
                    print(f"✗ 创建连接失败: {response.status_code} - {response.text}")
                    return False
        except Exception as e:
            print(f"✗ 配置连接时出错: {e}")
            return False
    
    def list_dags(self) -> list:
        """列出所有 DAG"""
        try:
            response = self.session.get(f"{self.base_url}/dags")
            if response.status_code == 200:
                data = response.json()
                return data.get("dags", [])
            else:
                print(f"✗ 获取 DAG 列表失败: {response.status_code}")
                return []
        except Exception as e:
            print(f"✗ 获取 DAG 列表时出错: {e}")
            return []
    
    def get_dag(self, dag_id: str) -> Optional[Dict]:
        """获取 DAG 详情"""
        try:
            response = self.session.get(f"{self.base_url}/dags/{dag_id}")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                print(f"✗ 获取 DAG 失败: {response.status_code}")
                return None
        except Exception as e:
            print(f"✗ 获取 DAG 时出错: {e}")
            return None
    
    def update_dag(self, dag_id: str, is_paused: bool = None) -> bool:
        """更新 DAG（启用/禁用）"""
        try:
            patch_data = {}
            if is_paused is not None:
                patch_data["is_paused"] = is_paused
            
            response = self.session.patch(
                f"{self.base_url}/dags/{dag_id}",
                json=patch_data,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 200:
                status = "启用" if not is_paused else "禁用"
                print(f"✓ DAG '{dag_id}' 已{status}")
                return True
            else:
                print(f"✗ 更新 DAG 失败: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"✗ 更新 DAG 时出错: {e}")
            return False
    
    def get_dag_import_errors(self) -> Dict:
        """获取 DAG 导入错误"""
        try:
            response = self.session.get(f"{self.base_url}/dags/importErrors")
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except Exception as e:
            print(f"✗ 获取 DAG 错误时出错: {e}")
            return {}


def setup_mysql_connection(client: AirflowClient) -> bool:
    """配置 MySQL 连接"""
    print("\n" + "=" * 60)
    print("配置 MySQL 连接")
    print("=" * 60)
    
    # 检查现有连接
    existing = client.get_connection(MYSQL_CONNECTION["connection_id"])
    if existing:
        print(f"发现现有连接: {MYSQL_CONNECTION['connection_id']}")
        print(f"  类型: {existing.get('conn_type')}")
        print(f"  主机: {existing.get('host')}")
    
    # 创建或更新连接
    return client.create_or_update_connection(MYSQL_CONNECTION)


def check_dags(client: AirflowClient):
    """检查 DAG 状态"""
    print("\n" + "=" * 60)
    print("检查 DAG 状态")
    print("=" * 60)
    
    dags = client.list_dags()
    if not dags:
        print("未找到任何 DAG，请检查 DAG 文件是否正确加载")
        return
    
    print(f"\n找到 {len(dags)} 个 DAG:")
    
    # 按层分类统计
    layers = {"ods": 0, "dwd": 0, "dws": 0, "ads": 0, "other": 0}
    paused_count = 0
    active_count = 0
    
    for dag in dags:
        dag_id = dag.get("dag_id", "")
        is_paused = dag.get("is_paused", True)
        
        if is_paused:
            paused_count += 1
        else:
            active_count += 1
        
        # 统计各层 DAG 数量
        if dag_id.startswith("ods_"):
            layers["ods"] += 1
        elif dag_id.startswith("dwd_"):
            layers["dwd"] += 1
        elif dag_id.startswith("dws_"):
            layers["dws"] += 1
        elif dag_id.startswith("ads_"):
            layers["ads"] += 1
        else:
            layers["other"] += 1
    
    print(f"\n按层统计:")
    print(f"  ODS 层: {layers['ods']} 个")
    print(f"  DWD 层: {layers['dwd']} 个")
    print(f"  DWS 层: {layers['dws']} 个")
    print(f"  ADS 层: {layers['ads']} 个")
    if layers["other"] > 0:
        print(f"  其他: {layers['other']} 个")
    
    print(f"\n状态统计:")
    print(f"  已暂停: {paused_count} 个")
    print(f"  已启用: {active_count} 个")
    
    # 检查导入错误
    errors = client.get_dag_import_errors()
    if errors and errors.get("import_errors"):
        print(f"\n⚠ 发现 {len(errors['import_errors'])} 个 DAG 导入错误:")
        for error in errors["import_errors"]:
            print(f"  - {error['dag_id']}: {error['import_error'][:100]}...")
    else:
        print("\n✓ 未发现 DAG 导入错误")


def main():
    """主函数"""
    print("=" * 60)
    print("Airflow 配置脚本")
    print("=" * 60)
    
    # 创建客户端
    client = AirflowClient(AIRFLOW_API_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    # 检查连接
    if not client.check_connection():
        print("\n请确保 Airflow 服务正在运行:")
        print("  cd scheduler/airflow")
        print("  docker-compose up -d")
        sys.exit(1)
    
    # 配置 MySQL 连接
    if not setup_mysql_connection(client):
        print("\n⚠ MySQL 连接配置失败，但可以继续检查 DAG 状态")
    
    # 检查 DAG 状态
    check_dags(client)
    
    print("\n" + "=" * 60)
    print("配置完成！")
    print("=" * 60)
    print("\n提示:")
    print(f"  - 访问 Web UI: {AIRFLOW_BASE_URL}")
    print("  - 用户名/密码: airflow/airflow")
    print("  - 如需启用 DAG，请在 Web UI 中操作或使用此脚本的扩展功能")


if __name__ == "__main__":
    main()
