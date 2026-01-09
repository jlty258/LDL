#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinScheduler REST API 客户端脚本
用于通过 REST API 创建、管理和运行工作流
"""

import requests
import json
from typing import Optional, Dict, Any


class DolphinSchedulerRESTClient:
    """DolphinScheduler REST API 客户端"""
    
    def __init__(self, base_url: str = "http://localhost:12345", 
                 username: str = "admin", 
                 password: str = "dolphinscheduler123"):
        """
        初始化 DolphinScheduler REST API 客户端
        
        Args:
            base_url: DolphinScheduler Web 服务器地址
            username: 用户名
            password: 密码
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session_id = None
        self._login()
    
    def _login(self):
        """登录获取 session ID"""
        url = f"{self.base_url}/dolphinscheduler/login"
        data = {
            "userName": self.username,
            "userPassword": self.password
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        result = response.json()
        
        if result.get('code') == 0:
            self.session_id = result.get('data', {}).get('sessionId')
            # 设置 session ID 到 cookie
            self.session.cookies.set('sessionId', self.session_id)
            print(f"✓ 登录成功，Session ID: {self.session_id}")
        else:
            raise Exception(f"登录失败: {result.get('msg', 'Unknown error')}")
    
    def get_projects(self) -> Dict[str, Any]:
        """获取项目列表"""
        url = f"{self.base_url}/dolphinscheduler/projects/list"
        params = {"pageNo": 1, "pageSize": 10}
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def create_project(self, project_name: str, description: str = "") -> Dict[str, Any]:
        """创建项目"""
        url = f"{self.base_url}/dolphinscheduler/projects/create"
        data = {
            "projectName": project_name,
            "description": description
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def get_process_definitions(self, project_code: str) -> Dict[str, Any]:
        """获取工作流定义列表"""
        url = f"{self.base_url}/dolphinscheduler/projects/{project_code}/process-definition/list"
        params = {"pageNo": 1, "pageSize": 10}
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def create_process_definition(self, project_code: str, process_definition: Dict) -> Dict[str, Any]:
        """
        创建工作流定义
        
        Args:
            project_code: 项目代码
            process_definition: 工作流定义 JSON
        """
        url = f"{self.base_url}/dolphinscheduler/projects/{project_code}/process-definition"
        response = self.session.post(url, json=process_definition)
        response.raise_for_status()
        return response.json()
    
    def run_process_instance(self, project_code: str, process_definition_code: str,
                            failure_strategy: str = "END",
                            warning_type: str = "NONE",
                            warning_group_id: int = 0,
                            schedule_time: Optional[str] = None,
                            worker_group: str = "default",
                            environment_code: int = -1,
                            timeout: int = 0,
                            run_mode: str = "RUN_MODE_SERIAL",
                            expected_parallelism_number: int = 0,
                            dry_run: int = 0,
                            complement_dependent_mode: str = "OFF_MODE",
                            exec_type: str = "START_PROCESS",
                            start_node_list: Optional[list] = None,
                            task_depend_type: str = "TASK_POST",
                            dependent_mode: str = "OFF_MODE",
                            resource_limit: int = 0) -> Dict[str, Any]:
        """
        运行工作流实例
        
        Args:
            project_code: 项目代码
            process_definition_code: 工作流定义代码
            其他参数: 工作流运行参数
        """
        url = f"{self.base_url}/dolphinscheduler/projects/{project_code}/executors/start-process-instance"
        data = {
            "processDefinitionCode": process_definition_code,
            "failureStrategy": failure_strategy,
            "warningType": warning_type,
            "warningGroupId": warning_group_id,
            "scheduleTime": schedule_time,
            "workerGroup": worker_group,
            "environmentCode": environment_code,
            "timeout": timeout,
            "runMode": run_mode,
            "expectedParallelismNumber": expected_parallelism_number,
            "dryRun": dry_run,
            "complementDependentMode": complement_dependent_mode,
            "execType": exec_type,
            "startNodeList": start_node_list or [],
            "taskDependType": task_depend_type,
            "dependentMode": dependent_mode,
            "resourceLimit": resource_limit
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def get_process_instance(self, project_code: str, process_instance_id: int) -> Dict[str, Any]:
        """获取工作流实例详情"""
        url = f"{self.base_url}/dolphinscheduler/projects/{project_code}/process-instance/{process_instance_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_process_instance_list(self, project_code: str, page_no: int = 1, 
                                  page_size: int = 10) -> Dict[str, Any]:
        """获取工作流实例列表"""
        url = f"{self.base_url}/dolphinscheduler/projects/{project_code}/process-instance/list"
        params = {
            "pageNo": page_no,
            "pageSize": page_size
        }
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()


def create_simple_workflow_definition(name: str = "test_workflow") -> Dict[str, Any]:
    """
    创建简单的工作流定义
    
    Args:
        name: 工作流名称
    """
    return {
        "name": name,
        "description": f"测试工作流 - {name}",
        "globalParams": [],
        "tasks": [
            {
                "name": "task1",
                "description": "任务1",
                "taskType": "SHELL",
                "taskParams": {
                    "resourceList": [],
                    "localParams": [],
                    "rawScript": "echo 'Hello from DolphinScheduler!'",
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


def main():
    """主函数 - 演示如何使用 REST API"""
    print("=" * 60)
    print("DolphinScheduler REST API 客户端演示")
    print("=" * 60)
    
    # 创建客户端
    try:
        client = DolphinSchedulerRESTClient()
    except Exception as e:
        print(f"\n❌ 连接失败: {e}")
        print("   请确保 DolphinScheduler 服务正在运行")
        return 1
    
    try:
        # 1. 获取项目列表
        print("\n1. 获取项目列表...")
        projects = client.get_projects()
        project_list = projects.get('data', {}).get('totalList', [])
        print(f"   找到 {len(project_list)} 个项目")
        
        if not project_list:
            print("   没有找到项目，创建默认项目...")
            result = client.create_project("default", "默认项目")
            if result.get('code') == 0:
                print("   ✓ 项目创建成功")
                # 重新获取项目列表
                projects = client.get_projects()
                project_list = projects.get('data', {}).get('totalList', [])
            else:
                print(f"   ❌ 项目创建失败: {result.get('msg')}")
                return 1
        
        if project_list:
            project = project_list[0]
            project_code = project.get('code')
            project_name = project.get('name')
            print(f"   使用项目: {project_name} (代码: {project_code})")
            
            # 2. 获取工作流定义
            print("\n2. 获取工作流定义列表...")
            process_defs = client.get_process_definitions(project_code)
            process_list = process_defs.get('data', {}).get('totalList', [])
            print(f"   找到 {len(process_list)} 个工作流定义")
            
            # 3. 如果没有工作流，创建一个简单的示例
            if not process_list:
                print("\n3. 创建示例工作流...")
                workflow_def = create_simple_workflow_definition("test_workflow")
                result = client.create_process_definition(project_code, workflow_def)
                if result.get('code') == 0:
                    print("   ✓ 工作流创建成功")
                    process_definition_code = result.get('data')
                    print(f"   工作流代码: {process_definition_code}")
                else:
                    print(f"   ❌ 工作流创建失败: {result.get('msg')}")
                    print("   注意: 创建工作流可能需要通过 Web UI 完成")
                    return 1
            else:
                process_definition_code = process_list[0].get('code')
                print(f"   使用工作流: {process_list[0].get('name')} (代码: {process_definition_code})")
            
            # 4. 运行工作流实例
            if process_definition_code:
                print(f"\n4. 运行工作流实例 (代码: {process_definition_code})...")
                result = client.run_process_instance(project_code, process_definition_code)
                if result.get('code') == 0:
                    process_instance_id = result.get('data')
                    print(f"   ✓ 工作流实例启动成功!")
                    print(f"   实例 ID: {process_instance_id}")
                else:
                    print(f"   ❌ 工作流实例启动失败: {result.get('msg')}")
                    print("   注意: 可能需要先通过 Web UI 创建工作流定义")
                    return 1
        
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



