#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试调度平台 REST API 脚本
用于测试 Airflow 和 DolphinScheduler 的 REST API 功能
"""

import sys
import os

# 添加脚本目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow_rest_api import AirflowRESTClient
from dolphinscheduler_rest_api import DolphinSchedulerRESTClient


def test_airflow():
    """测试 Airflow REST API"""
    print("\n" + "=" * 60)
    print("测试 Airflow REST API")
    print("=" * 60)
    
    try:
        client = AirflowRESTClient()
        
        # 获取版本
        version = client.get_version()
        print(f"✓ Airflow 版本: {version.get('version', 'Unknown')}")
        
        # 列出 DAG
        dags = client.list_dags()
        dag_list = dags.get('dags', [])
        print(f"✓ 找到 {len(dag_list)} 个 DAG")
        
        # 如果有 DAG，触发一个
        if dag_list:
            test_dag = dag_list[0]
            dag_id = test_dag.get('dag_id')
            
            # 取消暂停
            if test_dag.get('is_paused', False):
                client.unpause_dag(dag_id)
                print(f"✓ 取消暂停 DAG: {dag_id}")
            
            # 触发 DAG
            result = client.trigger_dag(dag_id)
            print(f"✓ 触发 DAG: {dag_id}")
            print(f"  运行 ID: {result.get('dag_run_id')}")
            return True
        else:
            print("⚠ 没有找到 DAG，请先创建 DAG 文件")
            return False
            
    except Exception as e:
        print(f"❌ Airflow 测试失败: {e}")
        return False


def test_dolphinscheduler():
    """测试 DolphinScheduler REST API"""
    print("\n" + "=" * 60)
    print("测试 DolphinScheduler REST API")
    print("=" * 60)
    
    try:
        client = DolphinSchedulerRESTClient()
        
        # 获取项目
        projects = client.get_projects()
        project_list = projects.get('data', {}).get('totalList', [])
        print(f"✓ 找到 {len(project_list)} 个项目")
        
        if project_list:
            project = project_list[0]
            project_code = project.get('code')
            project_name = project.get('name')
            print(f"✓ 使用项目: {project_name}")
            
            # 获取工作流
            process_defs = client.get_process_definitions(project_code)
            process_list = process_defs.get('data', {}).get('totalList', [])
            print(f"✓ 找到 {len(process_list)} 个工作流定义")
            
            if process_list:
                process_def = process_list[0]
                process_code = process_def.get('code')
                process_name = process_def.get('name')
                print(f"✓ 使用工作流: {process_name}")
                
                # 运行工作流
                result = client.run_process_instance(project_code, process_code)
                if result.get('code') == 0:
                    instance_id = result.get('data')
                    print(f"✓ 工作流实例启动成功，实例 ID: {instance_id}")
                    return True
                else:
                    print(f"⚠ 工作流启动失败: {result.get('msg')}")
                    return False
            else:
                print("⚠ 没有找到工作流定义，请先通过 Web UI 创建工作流")
                return False
        else:
            print("⚠ 没有找到项目")
            return False
            
    except Exception as e:
        print(f"❌ DolphinScheduler 测试失败: {e}")
        return False


def main():
    """主函数"""
    print("=" * 60)
    print("调度平台 REST API 测试")
    print("=" * 60)
    
    results = {
        'airflow': False,
        'dolphinscheduler': False
    }
    
    # 测试 Airflow
    results['airflow'] = test_airflow()
    
    # 测试 DolphinScheduler
    results['dolphinscheduler'] = test_dolphinscheduler()
    
    # 总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    print(f"Airflow: {'✓ 成功' if results['airflow'] else '✗ 失败'}")
    print(f"DolphinScheduler: {'✓ 成功' if results['dolphinscheduler'] else '✗ 失败'}")
    
    if all(results.values()):
        print("\n✓ 所有测试通过!")
        return 0
    else:
        print("\n⚠ 部分测试失败，请检查服务状态")
        return 1


if __name__ == "__main__":
    exit(main())



