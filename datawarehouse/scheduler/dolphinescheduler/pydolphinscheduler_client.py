#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PyDolphinScheduler 客户端
使用 PyDolphinScheduler（官方 Python API）创建工作流
支持 DolphinScheduler 3.3.0+

参考: https://dolphinscheduler.apache.org/python/3.0.0/index.html
"""

import os
import sys

try:
    from pydolphinscheduler.core.workflow import Workflow
    from pydolphinscheduler.tasks.sql import Sql
    from pydolphinscheduler.tasks.shell import Shell
    # PyDolphinScheduler 4.1.0 可能不再需要单独的 Project 导入
    PYDOLPHINSCHEDULER_AVAILABLE = True
except ImportError as e:
    PYDOLPHINSCHEDULER_AVAILABLE = False
    print(f"⚠ 警告: PyDolphinScheduler 模块导入失败: {e}")
    print("   请运行: pip install apache-dolphinscheduler")


class PyDolphinSchedulerClient:
    """PyDolphinScheduler 客户端封装"""
    
    def __init__(self, user="admin", password="dolphinscheduler123", 
                 host=os.getenv("TAILSCALE_IP", "100.126.111.70"), port=12345, project_name="制造业数仓"):
        """
        初始化 PyDolphinScheduler 客户端
        
        Args:
            user: DolphinScheduler 用户名
            password: DolphinScheduler 密码
            host: DolphinScheduler API 服务器地址
            port: DolphinScheduler API 服务器端口
            project_name: 项目名称
        """
        if not PYDOLPHINSCHEDULER_AVAILABLE:
            raise ImportError(
                "PyDolphinScheduler 未安装。请运行: pip install apache-dolphinscheduler"
            )
        
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.project_name = project_name
        
        # PyDolphinScheduler 4.1.0 配置方式
        # 设置环境变量用于配置
        os.environ["PYDOLPHINSCHEDULER_USER"] = user
        os.environ["PYDOLPHINSCHEDULER_PASSWORD"] = password
        os.environ["PYDOLPHINSCHEDULER_HOST"] = host
        os.environ["PYDOLPHINSCHEDULER_PORT"] = str(port)
        
        # Java Gateway 配置（如果环境变量存在，PyDolphinScheduler 会自动使用）
        java_gateway_address = os.getenv("PYDS_JAVA_GATEWAY_ADDRESS")
        java_gateway_port = os.getenv("PYDS_JAVA_GATEWAY_PORT")
        if java_gateway_address and java_gateway_port:
            print(f"✓ Java Gateway 配置已检测: {java_gateway_address}:{java_gateway_port}")
        else:
            print(f"⚠ 警告: Java Gateway 配置未找到，PyDolphinScheduler 可能无法正常工作")
            print(f"  请设置环境变量: PYDS_JAVA_GATEWAY_ADDRESS 和 PYDS_JAVA_GATEWAY_PORT")
        
        # PyDolphinScheduler 4.1.0 中，项目名称直接作为字符串使用
        # Workflow 会在提交时自动创建项目（如果不存在）
        self.project = project_name
        
        print(f"✓ PyDolphinScheduler 客户端初始化成功")
        print(f"  用户: {user}")
        print(f"  服务器: {host}:{port}")
        print(f"  项目: {project_name}")
    
    def read_sql_file(self, sql_file_name: str) -> str:
        """读取SQL文件内容"""
        sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
        sql_path = os.path.join(sql_dir, sql_file_name)
        
        # 也尝试容器路径
        container_path = f"/workspace/datawarehouse/sql/{sql_file_name}"
        
        if os.path.exists(sql_path):
            file_path = sql_path
        elif os.path.exists(container_path):
            file_path = container_path
        else:
            print(f"⚠ 警告: SQL文件不存在: {sql_file_name}")
            return f"-- {sql_file_name}\nSELECT 1;"
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"⚠ 警告: 读取SQL文件失败: {e}")
            return f"-- {sql_file_name}\nSELECT 1;"
    
    def create_sql_task(self, task_name: str, sql_content: str, 
                       datasource_name: str = "MySQL", datasource_id: int = 1,
                       sql_type: int = 0, workflow=None) -> Sql:
        """
        创建 SQL 任务
        
        Args:
            task_name: 任务名称
            sql_content: SQL 内容
            datasource_name: 数据源名称（如 "MySQL", "PostgreSQL"）
            datasource_id: 数据源ID（需要在 DolphinScheduler 中预先配置）
            sql_type: SQL类型，0=非查询，1=查询
            workflow: Workflow 对象（可选，如果提供则在该 workflow 上下文中创建任务）
            
        注意: PyDolphinScheduler 4.1.0 中，任务需要在 workflow 上下文中创建
        如果遇到错误，请参考官方文档调整参数
        """
        # 限制 SQL 长度（如果太长可能需要使用资源文件）
        if len(sql_content) > 5000:
            sql_content = sql_content[:5000] + "\n-- SQL内容已截断..."
        
        # 创建 SQL 任务
        # PyDolphinScheduler 4.1.0 中，任务需要在 workflow 上下文中创建
        # 如果没有提供 workflow，返回一个可调用对象，在 workflow 上下文中调用
        if workflow is None:
            # 返回一个函数，在 workflow 上下文中创建任务
            def create_task_in_context(wf):
                try:
                    return Sql(
                        name=task_name,
                        datasource_name=datasource_name,
                        datasource_id=datasource_id,
                        sql=sql_content,
                        sql_type=sql_type
                    )
                except TypeError:
                    # 尝试简化参数
                    try:
                        return Sql(
                            name=task_name,
                            datasource_name=datasource_name,
                            sql=sql_content
                        )
                    except TypeError:
                        # 最简参数
                        return Sql(
                            name=task_name,
                            sql=sql_content
                        )
            return create_task_in_context
        else:
            # 在 workflow 上下文中创建任务
            try:
                task = Sql(
                    name=task_name,
                    datasource_name=datasource_name,
                    datasource_id=datasource_id,
                    sql=sql_content,
                    sql_type=sql_type
                )
            except TypeError:
                try:
                    task = Sql(
                        name=task_name,
                        datasource_name=datasource_name,
                        sql=sql_content
                    )
                except TypeError:
                    task = Sql(
                        name=task_name,
                        sql=sql_content
                    )
            return task
    
    def create_workflow(self, workflow_name: str, description: str,
                       tasks: list, schedule: str = None, 
                       timeout: int = 0) -> Workflow:
        """
        创建工作流
        
        Args:
            workflow_name: 工作流名称
            description: 工作流描述
            tasks: 任务列表（Sql 或 Shell 任务对象）
            schedule: 调度表达式（cron格式），如 "0 0 2 * * ?"
            timeout: 超时时间（秒），0表示不超时
            
        注意: PyDolphinScheduler 4.1.0 需要使用 with 语句和 submit() 方法
        """
        # 创建工作流 - PyDolphinScheduler 4.1.0 方式
        # 注意: 需要使用 project 对象，而不是项目名称字符串
        project_obj = self.project if self.project else self.project_name
        
        try:
            workflow = Workflow(
                name=workflow_name,
                description=description,
                schedule=schedule,
                project=project_obj
            )
        except (TypeError, AttributeError):
            # 如果 project 对象不工作，尝试使用项目名称
            try:
                workflow = Workflow(
                    name=workflow_name,
                    description=description,
                    schedule=schedule,
                    project=self.project_name
                )
            except (TypeError, AttributeError):
                # 最简参数
                workflow = Workflow(
                    name=workflow_name,
                    schedule=schedule
                )
        
        # 添加任务
        for task in tasks:
            workflow.add_task(task)
        
        return workflow
    
    def create_and_submit_workflow(self, workflow_name: str, description: str,
                                   tasks: list, schedule: str = None,
                                   timeout: int = 0) -> bool:
        """
        创建并提交工作流到 DolphinScheduler
        
        Args:
            workflow_name: 工作流名称
            description: 工作流描述
            tasks: 任务列表（可以是任务对象或任务创建函数）
            schedule: 调度表达式
            timeout: 超时时间
            
        Returns:
            bool: 是否成功提交
        """
        try:
            # PyDolphinScheduler 4.1.0 需要使用 with 语句
            # 任务必须在 workflow 上下文中创建
            project_obj = self.project if self.project else self.project_name
            
            # 使用 with 语句创建工作流（推荐方式）
            # PyDolphinScheduler 4.1.0 需要 start_time 和 end_time
            import datetime
            start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            end_time = "2099-12-31 23:59:59"
            
            # 如果 schedule 是 6 字段格式，转换为 7 字段格式（添加年份）
            if schedule:
                schedule_parts = schedule.split()
                if len(schedule_parts) == 6:
                    # 6 字段格式：秒 分 时 日 月 周
                    # 转换为 7 字段格式：秒 分 时 日 月 周 年
                    schedule = f"{schedule} *"
            
            with Workflow(
                name=workflow_name,
                description=description,
                schedule=schedule,
                start_time=start_time,
                end_time=end_time,
                project=project_obj
            ) as workflow:
                # 添加任务
                # 如果任务是函数（延迟创建），在 workflow 上下文中调用
                # 如果是任务对象，直接添加
                for task in tasks:
                    if callable(task) and not isinstance(task, (Sql, Shell)):
                        # 这是一个任务创建函数，在 workflow 上下文中调用
                        task_obj = task(workflow)
                        workflow.add_task(task_obj)
                    else:
                        # 直接添加任务对象
                        workflow.add_task(task)
                
                # 提交工作流
                workflow.submit()
            
            print(f"✓ 工作流提交成功: {workflow_name}")
            return True
            
        except Exception as e:
            print(f"❌ 工作流提交失败: {workflow_name} - {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_sql_workflow_from_file(self, workflow_name: str, description: str,
                                     sql_file: str, schedule: str = None,
                                     datasource_name: str = "MySQL",
                                     datasource_id: int = 1) -> bool:
        """
        从 SQL 文件创建工作流（便捷方法）
        
        Args:
            workflow_name: 工作流名称
            description: 工作流描述
            sql_file: SQL 文件名
            schedule: 调度表达式
            datasource_name: 数据源名称
            datasource_id: 数据源ID
            
        Returns:
            bool: 是否成功提交
        """
        sql_content = self.read_sql_file(sql_file)
        
        # 创建任务创建函数（延迟创建，在 workflow 上下文中创建）
        def create_task(workflow):
            # 限制 SQL 长度
            if len(sql_content) > 5000:
                content = sql_content[:5000] + "\n-- SQL内容已截断..."
            else:
                content = sql_content
            
            # 在 workflow 上下文中创建任务
            # 注意: PyDolphinScheduler 4.1.0 可能需要使用 datasource_id 而不是 datasource_name
            try:
                # 尝试使用 datasource_id
                return Sql(
                    name=f"{workflow_name}_task",
                    datasource_id=datasource_id,
                    sql=content,
                    sql_type=0
                )
            except (TypeError, AttributeError):
                try:
                    # 尝试使用 datasource_name
                    return Sql(
                        name=f"{workflow_name}_task",
                        datasource_name=datasource_name,
                        sql=content
                    )
                except (TypeError, AttributeError):
                    # 最简参数（可能需要在 Web UI 中手动配置数据源）
                    return Sql(
                        name=f"{workflow_name}_task",
                        sql=content
                    )
        
        return self.create_and_submit_workflow(
            workflow_name=workflow_name,
            description=description,
            tasks=[create_task],
            schedule=schedule
        )


def get_client_from_env():
    """
    从环境变量创建客户端（支持环境变量配置）
    
    环境变量:
        DS_USERNAME: DolphinScheduler 用户名（默认: admin）
        DS_PASSWORD: DolphinScheduler 密码（默认: dolphinscheduler123）
        DS_HOST: DolphinScheduler API 服务器地址（默认: localhost）
        DS_PORT: DolphinScheduler API 服务器端口（默认: 12345）
        DS_PROJECT_NAME: 项目名称（默认: 制造业数仓）
    """
    return PyDolphinSchedulerClient(
        user=os.getenv('DS_USERNAME', 'admin'),
        password=os.getenv('DS_PASSWORD', 'dolphinscheduler123'),
        host=os.getenv('DS_HOST', os.getenv('TAILSCALE_IP', '100.126.111.70')),
        port=int(os.getenv('DS_PORT', '12345')),
        project_name=os.getenv('DS_PROJECT_NAME', '制造业数仓')
    )


if __name__ == "__main__":
    # 测试代码
    if not PYDOLPHINSCHEDULER_AVAILABLE:
        print("❌ PyDolphinScheduler 未安装")
        print("   请运行: pip install apache-dolphinscheduler")
        sys.exit(1)
    
    print("=" * 60)
    print("PyDolphinScheduler 客户端测试")
    print("=" * 60)
    
    try:
        client = get_client_from_env()
        print("\n✓ 客户端创建成功")
    except Exception as e:
        print(f"\n❌ 客户端创建失败: {e}")
        sys.exit(1)
