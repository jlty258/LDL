#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证Airflow DAG是否已加载
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../scripts'))

try:
    from airflow_rest_api import AirflowRESTClient
    
    client = AirflowRESTClient()
    dags = client.get_dags()
    
    etl_dags = [dag for dag in dags.get('dags', []) if 'etl' in dag.get('dag_id', '').lower()]
    
    print("=" * 60)
    print("Airflow DAG验证")
    print("=" * 60)
    print(f"\n找到 {len(etl_dags)} 个ETL相关的DAG:")
    
    for i, dag in enumerate(sorted(etl_dags, key=lambda x: x.get('dag_id', '')), 1):
        dag_id = dag.get('dag_id', '')
        is_paused = dag.get('is_paused', True)
        status = "⏸ 暂停" if is_paused else "▶ 运行"
        print(f"  [{i:2d}] {dag_id:40s} {status}")
    
    print(f"\n总计: {len(etl_dags)}/30 个ETL DAG")
    print("\n访问Airflow: http://localhost:8080")
    print("用户名: airflow")
    print("密码: airflow")
    
except Exception as e:
    print(f"错误: {e}")
    print("请确保Airflow服务正在运行")
