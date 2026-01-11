#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate Airflow DAGs from SQL files

This script generates Airflow DAG files based on SQL files in datawarehouse/sql
and workflow definitions similar to DolphinScheduler workflows.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Paths
# Script is in: datawarehouse/scheduler/airflow/generate_dags.py
# SQL files are in: datawarehouse/sql/
# DAGs should be in: datawarehouse/scheduler/airflow/dags/
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent.parent.parent
SQL_DIR = BASE_DIR / "datawarehouse" / "sql"
DAGS_DIR = SCRIPT_DIR / "dags"

# Ensure DAGs directory exists
DAGS_DIR.mkdir(parents=True, exist_ok=True)

# Workflow definitions (similar to DolphinScheduler workflows)
WORKFLOWS = [
    # ODS layer ETL tasks (1-10) - Daily at 2:00 AM
    {"name": "ods_01_order_master_etl", "description": "ODS层-订单主表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_02_order_detail_etl", "description": "ODS层-订单明细表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_03_customer_etl", "description": "ODS层-客户主表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_04_product_etl", "description": "ODS层-产品主表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_05_production_plan_etl", "description": "ODS层-生产计划表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_06_production_order_etl", "description": "ODS层-生产工单表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_07_bom_etl", "description": "ODS层-物料清单表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_08_material_etl", "description": "ODS层-物料主表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_09_inventory_etl", "description": "ODS层-库存表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    {"name": "ods_10_purchase_etl", "description": "ODS层-采购订单表ETL", "sql_file": "02_ods_tables.sql", "schedule": "0 2 * * *"},
    
    # DWD layer ETL tasks (11-17) - Daily at 3:00 AM
    {"name": "dwd_01_order_fact_etl", "description": "DWD层-订单事实表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 3 * * *"},
    {"name": "dwd_02_production_fact_etl", "description": "DWD层-生产事实表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 3 * * *"},
    {"name": "dwd_03_inventory_fact_etl", "description": "DWD层-库存事实表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 3 * * *"},
    {"name": "dwd_04_purchase_fact_etl", "description": "DWD层-采购事实表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 3 * * *"},
    {"name": "dwd_05_quality_fact_etl", "description": "DWD层-质量事实表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 3 * * *"},
    {"name": "dwd_06_equipment_runtime_etl", "description": "DWD层-设备运行事实表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 3 * * *"},
    {"name": "dwd_07_cost_fact_etl", "description": "DWD层-成本事实表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 3 * * *"},
    
    # DWS layer ETL tasks (18-24) - Daily at 4:00 AM
    {"name": "dws_01_order_daily_etl", "description": "DWS层-订单日汇总ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 4 * * *"},
    {"name": "dws_02_production_daily_etl", "description": "DWS层-生产日汇总ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 4 * * *"},
    {"name": "dws_03_inventory_daily_etl", "description": "DWS层-库存日汇总ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 4 * * *"},
    {"name": "dws_04_purchase_daily_etl", "description": "DWS层-采购日汇总ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 4 * * *"},
    {"name": "dws_05_quality_daily_etl", "description": "DWS层-质量日汇总ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 4 * * *"},
    {"name": "dws_06_equipment_runtime_daily_etl", "description": "DWS层-设备运行日汇总ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 4 * * *"},
    {"name": "dws_07_cost_daily_etl", "description": "DWS层-成本日汇总ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 4 * * *"},
    
    # ADS layer ETL tasks (25-30) - Daily at 5:00 AM
    {"name": "ads_01_sales_analysis_etl", "description": "ADS层-销售分析报表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 5 * * *"},
    {"name": "ads_02_production_analysis_etl", "description": "ADS层-生产分析报表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 5 * * *"},
    {"name": "ads_03_inventory_analysis_etl", "description": "ADS层-库存分析报表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 5 * * *"},
    {"name": "ads_04_purchase_analysis_etl", "description": "ADS层-采购分析报表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 5 * * *"},
    {"name": "ads_05_quality_analysis_etl", "description": "ADS层-质量分析报表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 5 * * *"},
    {"name": "ads_06_business_overview_etl", "description": "ADS层-综合经营分析报表ETL", "sql_file": "07_complex_etl.sql", "schedule": "0 5 * * *"},
]

def read_sql_file(sql_file_name):
    """Read SQL file content"""
    sql_path = SQL_DIR / sql_file_name
    if not sql_path.exists():
        print(f"Warning: SQL file not found: {sql_path}")
        return f"-- SQL file: {sql_file_name}\nSELECT 1;"
    
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Warning: Failed to read SQL file {sql_file_name}: {e}")
        return f"-- SQL file: {sql_file_name}\nSELECT 1;"

def generate_dag_file(workflow):
    """Generate Airflow DAG file for a workflow"""
    dag_id = workflow['name']
    description = workflow['description']
    sql_file = workflow['sql_file']
    schedule = workflow['schedule']
    
    # Get layer prefix for tags
    layer = dag_id.split('_')[0] if '_' in dag_id else 'unknown'
    
    # Use SQL file path instead of embedding content
    # SQL file path relative to Airflow container
    sql_file_path = f"/opt/airflow/dags/sql/{sql_file}"
    
    dag_content = f'''"""
Airflow DAG: {dag_id}

{description}
Generated from SQL file: {sql_file}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from pathlib import Path

# Default arguments
default_args = {{
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}}

# DAG definition
dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='{description}',
    schedule='{schedule}',  # Use 'schedule' instead of deprecated 'schedule_interval'
    catchup=False,
    tags=['etl', 'datawarehouse', '{layer}'],
)

# Read SQL file content
def read_sql_file():
    """Read SQL file content"""
    # Try multiple possible paths
    sql_paths = [
        Path(__file__).parent / 'sql' / '{sql_file}',
        Path('/opt/airflow/dags/sql') / '{sql_file}',
        Path('/workspace/datawarehouse/sql') / '{sql_file}',
    ]
    
    for sql_path in sql_paths:
        if sql_path.exists():
            with open(sql_path, 'r', encoding='utf-8') as f:
                return f.read()
    
    # Fallback: return a simple SQL statement
    return "SELECT 1;"

# SQL task
sql_task = MySqlOperator(
    task_id='execute_sql',
    mysql_conn_id='mysql_default',
    sql=read_sql_file(),
    dag=dag,
)

# Task dependencies
sql_task
'''
    
    # Write DAG file
    dag_file = DAGS_DIR / f"{dag_id}.py"
    with open(dag_file, 'w', encoding='utf-8') as f:
        f.write(dag_content)
    
    return dag_file

def main():
    """Generate all DAG files"""
    print("=" * 60)
    print("Generating Airflow DAGs from SQL files")
    print("=" * 60)
    print(f"\nSQL Directory: {SQL_DIR}")
    print(f"DAGs Directory: {DAGS_DIR}")
    print(f"\nGenerating {len(WORKFLOWS)} DAG files...\n")
    
    generated_count = 0
    failed_count = 0
    
    for i, workflow in enumerate(WORKFLOWS, 1):
        try:
            dag_file = generate_dag_file(workflow)
            print(f"✓ [{i:2d}/{len(WORKFLOWS)}] {workflow['name']} -> {dag_file.name}")
            generated_count += 1
        except Exception as e:
            print(f"❌ [{i:2d}/{len(WORKFLOWS)}] {workflow['name']} - {e}")
            failed_count += 1
    
    print("\n" + "=" * 60)
    print(f"Complete! Generated: {generated_count}, Failed: {failed_count}, Total: {len(WORKFLOWS)}")
    print("=" * 60)
    print(f"\nDAG files location: {DAGS_DIR}")
    print("\nNote: Make sure to configure MySQL connection in Airflow:")
    print("  - Connection ID: mysql_default")
    print("  - Connection Type: MySQL")
    print("  - Host: mysql-db (or your MySQL host)")
    print("  - Schema: sqlExpert")
    print("  - Login: sqluser")
    print("  - Password: sqlpass123")

if __name__ == "__main__":
    main()
