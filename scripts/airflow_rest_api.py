#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Airflow REST API 客户端脚本
用于通过 REST API 创建、管理和触发 DAG
"""

import requests
import json
import base64
from typing import Optional, Dict, Any


class AirflowRESTClient:
    """Airflow REST API 客户端"""
    
    def __init__(self, base_url: str = "http://localhost:8080", 
                 username: str = "airflow", 
                 password: str = "airflow"):
        """
        初始化 Airflow REST API 客户端
        
        Args:
            base_url: Airflow Web 服务器地址
            username: 用户名
            password: 密码
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self._setup_auth()
    
    def _setup_auth(self):
        """设置基本认证"""
        credentials = f"{self.username}:{self.password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        self.session.headers.update({
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/json'
        })
    
    def get_version(self) -> Dict[str, Any]:
        """获取 Airflow 版本信息"""
        url = f"{self.base_url}/api/v2/version"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def list_dags(self) -> Dict[str, Any]:
        """列出所有 DAG"""
        url = f"{self.base_url}/api/v2/dags"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """获取指定 DAG 的详细信息"""
        url = f"{self.base_url}/api/v2/dags/{dag_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def pause_dag(self, dag_id: str) -> Dict[str, Any]:
        """暂停 DAG"""
        url = f"{self.base_url}/api/v2/dags/{dag_id}"
        data = {"is_paused": True}
        response = self.session.patch(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def unpause_dag(self, dag_id: str) -> Dict[str, Any]:
        """取消暂停 DAG"""
        url = f"{self.base_url}/api/v2/dags/{dag_id}"
        data = {"is_paused": False}
        response = self.session.patch(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None, 
                    run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        触发 DAG 运行
        
        Args:
            dag_id: DAG ID
            conf: 配置参数（可选）
            run_id: 运行 ID（可选）
        """
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
        data = {}
        if conf:
            data["conf"] = conf
        if run_id:
            data["dag_run_id"] = run_id
        
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def get_dag_runs(self, dag_id: str, limit: int = 10) -> Dict[str, Any]:
        """获取 DAG 运行历史"""
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
        params = {"limit": limit}
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """获取特定 DAG 运行的详细信息"""
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_task_instances(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """获取任务实例"""
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()


def main():
    """主函数 - 演示如何使用 REST API"""
    print("=" * 60)
    print("Airflow REST API 客户端演示")
    print("=" * 60)
    
    # 创建客户端
    client = AirflowRESTClient()
    
    try:
        # 1. 获取版本信息
        print("\n1. 获取 Airflow 版本信息...")
        version = client.get_version()
        print(f"   Airflow 版本: {version.get('version', 'Unknown')}")
        
        # 2. 列出所有 DAG
        print("\n2. 列出所有 DAG...")
        dags = client.list_dags()
        dag_list = dags.get('dags', [])
        print(f"   找到 {len(dag_list)} 个 DAG:")
        for dag in dag_list[:5]:  # 只显示前5个
            print(f"   - {dag.get('dag_id')} (暂停: {dag.get('is_paused', False)})")
        
        # 3. 如果有 DAG，尝试触发一个
        if dag_list:
            test_dag_id = dag_list[0].get('dag_id')
            print(f"\n3. 触发 DAG: {test_dag_id}...")
            
            # 取消暂停（如果需要）
            if dag_list[0].get('is_paused', False):
                print(f"   取消暂停 DAG: {test_dag_id}...")
                client.unpause_dag(test_dag_id)
            
            # 触发 DAG
            result = client.trigger_dag(test_dag_id, conf={"test": "value"})
            print(f"   ✓ DAG 触发成功!")
            print(f"   运行 ID: {result.get('dag_run_id')}")
            print(f"   状态: {result.get('state')}")
            
            # 4. 获取运行状态
            print(f"\n4. 获取 DAG 运行状态...")
            dag_runs = client.get_dag_runs(test_dag_id, limit=1)
            if dag_runs.get('dag_runs'):
                latest_run = dag_runs['dag_runs'][0]
                print(f"   最新运行 ID: {latest_run.get('dag_run_id')}")
                print(f"   状态: {latest_run.get('state')}")
                print(f"   开始时间: {latest_run.get('start_date')}")
        else:
            print("\n3. 没有找到可用的 DAG，请先创建 DAG 文件")
        
        print("\n" + "=" * 60)
        print("演示完成!")
        print("=" * 60)
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ 错误: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   响应内容: {e.response.text}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())



