#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinScheduler REST API客户端

重要说明：
根据 Apache DolphinScheduler 官方文档，3.3.0 版本的 REST API 不支持直接创建工作流定义。
该功能在 3.4.0 或更高版本中提供。

替代方案：
1. 使用 generate_workflow_json.py 生成JSON文件，然后在Web UI中导入
2. 升级到 DolphinScheduler 3.4.0+ 版本
3. 使用 PyDolphinScheduler（官方 Python API）
   参考: https://dolphinscheduler.apache.org/python/3.0.0/index.html
"""

import requests
import json


class DolphinSchedulerRESTClient:
    """DolphinScheduler REST API客户端"""
    
    def __init__(self, base_url, username, password):
        """
        初始化客户端
        
        Args:
            base_url: DolphinScheduler API基础URL，例如: http://dolphinscheduler:12345
            username: 用户名
            password: 密码
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.token = None
        self._login()
    
    def _login(self):
        """登录获取token"""
        login_url = f"{self.base_url}/dolphinscheduler/login"
        data = {
            "userName": self.username,
            "userPassword": self.password
        }
        
        try:
            # DolphinScheduler登录API需要表单数据格式
            response = self.session.post(login_url, data=data, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            if result.get('code') == 0:
                # 尝试多种可能的token字段名
                data_obj = result.get('data', {})
                self.token = (data_obj.get('sessionId') or 
                             data_obj.get('token') or 
                             data_obj.get('sessionid'))
                
                if self.token:
                    # 同时检查响应中的cookie
                    if response.cookies:
                        print(f"✓ 登录成功: {self.username}, token: {self.token[:20]}...")
                    else:
                        print(f"✓ 登录成功: {self.username}, token: {self.token[:20]}...")
                else:
                    print(f"⚠ 登录响应中未找到token，响应数据: {result}")
            else:
                print(f"❌ 登录失败: {result.get('msg', '未知错误')}")
        except Exception as e:
            print(f"❌ 登录异常: {e}")
            raise
    
    def _request(self, method, endpoint, **kwargs):
        """发送HTTP请求"""
        url = f"{self.base_url}{endpoint}"
        method_upper = method.upper()
        
        # 提取参数
        params = kwargs.pop('params', {})
        json_data = kwargs.pop('json', None)
        headers = kwargs.pop('headers', {})
        
        # 对于非GET请求，设置Content-Type
        if method_upper != 'GET':
            headers['Content-Type'] = 'application/json'
        
        try:
            # 使用session的方法发送请求，cookie会自动包含
            if method_upper == 'GET':
                response = self.session.get(url, params=params, headers=headers, timeout=30, **kwargs)
            elif method_upper == 'POST':
                response = self.session.post(url, params=params, json=json_data, headers=headers, timeout=30, **kwargs)
            elif method_upper == 'PUT':
                response = self.session.put(url, params=params, json=json_data, headers=headers, timeout=30, **kwargs)
            elif method_upper == 'DELETE':
                response = self.session.delete(url, params=params, headers=headers, timeout=30, **kwargs)
            else:
                response = self.session.request(method, url, params=params, json=json_data, headers=headers, timeout=30, **kwargs)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ 请求失败 {method} {endpoint}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    print(f"   响应内容: {e.response.text[:200]}")
                    print(f"   请求URL: {url}")
                    print(f"   Cookies: {dict(self.session.cookies)}")
                except:
                    pass
            raise
    
    def get_projects(self, page_no=1, page_size=10, search_val=""):
        """
        获取项目列表
        
        Args:
            page_no: 页码
            page_size: 每页大小
            search_val: 搜索关键词
        """
        endpoint = "/dolphinscheduler/projects"
        params = {
            "pageNo": page_no,
            "pageSize": page_size,
            "searchVal": search_val
        }
        return self._request('GET', endpoint, params=params)
    
    def create_project(self, project_name, description=""):
        """
        创建项目
        
        Args:
            project_name: 项目名称
            description: 项目描述
        """
        endpoint = "/dolphinscheduler/projects"
        # DolphinScheduler API需要表单数据格式
        data = {
            "projectName": project_name,
            "description": description
        }
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, data=data, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def create_process_definition(self, project_code, process_definition, project_id=None):
        """
        创建工作流定义 - DolphinScheduler 3.3.0
        
        重要说明（根据 Apache DolphinScheduler 官方文档）：
        DolphinScheduler 3.3.0 版本的 REST API 不支持直接创建工作流定义。
        该功能在 3.4.0 或更高版本中提供。
        
        如果API调用失败，建议使用以下替代方案：
        1. 使用 generate_workflow_json.py 生成JSON文件，然后在Web UI中导入工作流
        2. 升级到 DolphinScheduler 3.4.0+ 版本以支持REST API创建工作流
        3. 使用 PyDolphinScheduler（官方 Python API）进行"工作流即代码"定义
           参考: https://dolphinscheduler.apache.org/python/3.0.0/index.html
        
        Args:
            project_code: 项目代码
            process_definition: 工作流定义字典（可以是标准格式或dag格式）
            project_id: 项目ID（可选）
        """
        # 根据文档，尝试使用/workflows端点，使用dag格式
        # 如果process_definition是标准格式，转换为dag格式
        if "dag" not in process_definition:
            # 转换为dag格式
            dag_definition = {
                "name": process_definition.get("name"),
                "description": process_definition.get("description", ""),
                "dag": {
                    "nodes": process_definition.get("tasks", []),
                    "edges": process_definition.get("connects", [])
                },
                "globalParams": process_definition.get("globalParams", [])
            }
        else:
            dag_definition = process_definition
        
        # 尝试多个端点
        endpoints = [
            f"/dolphinscheduler/projects/{project_code}/workflows",
            f"/dolphinscheduler/projects/{project_code}/process-definition"
        ]
        if project_id:
            endpoints.insert(0, f"/dolphinscheduler/projects/{project_id}/workflows")
            endpoints.insert(1, f"/dolphinscheduler/projects/{project_id}/process-definition")
        
        last_error = None
        error_details = []
        for endpoint in endpoints:
            url = f"{self.base_url}{endpoint}"
            try:
                # 使用JSON格式发送请求（session会自动处理cookie认证）
                response = self.session.post(url, json=dag_definition, timeout=30)
                if response.status_code == 200:
                    return response.json()
                else:
                    error_details.append(f"{endpoint}: {response.status_code} - {response.text[:200]}")
                    if response.status_code not in [405, 400]:  # 400可能是格式错误，继续尝试
                        response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                error_msg = f"{endpoint}: {e.response.status_code if e.response else 'Unknown'}"
                if e.response:
                    error_msg += f" - {e.response.text[:200]}"
                error_details.append(error_msg)
                if e.response and e.response.status_code in [405, 400]:
                    last_error = e
                    continue
                raise
        
        # 输出所有尝试的错误信息
        error_summary = "\n".join(error_details)
        error_message = (
            f"❌ DolphinScheduler 3.3.0 不支持通过 REST API 直接创建工作流定义（官方文档确认）。\n"
            f"该功能在 3.4.0 或更高版本中提供。\n"
            f"所有API端点都返回错误:\n{error_summary}\n\n"
            f"💡 替代方案（根据官方文档）：\n"
            f"1. 使用 generate_workflow_json.py 生成JSON文件，然后在Web UI中导入工作流\n"
            f"2. 升级到 DolphinScheduler 3.4.0+ 版本以支持REST API创建工作流\n"
            f"3. 使用 PyDolphinScheduler 进行"工作流即代码"定义\n"
            f"   官方文档: https://dolphinscheduler.apache.org/python/3.0.0/index.html\n"
            f"4. 通过Web UI手动创建工作流"
        )
        if last_error:
            raise Exception(error_message)
        raise Exception(error_message)
    
    def get_process_definitions(self, project_code, page_no=1, page_size=10):
        """
        获取工作流定义列表
        
        Args:
            project_code: 项目代码
            page_no: 页码
            page_size: 每页大小
        """
        endpoint = f"/dolphinscheduler/projects/{project_code}/process-definition"
        params = {
            "pageNo": page_no,
            "pageSize": page_size
        }
        return self._request('GET', endpoint, params=params)
