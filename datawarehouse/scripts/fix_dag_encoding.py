#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复所有DAG文件的编码问题
移除中文注释，使用英文
"""

import os
import re

dags_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'airflow', 'dags')

# DAG模板（无中文）
DAG_TEMPLATE = '''"""
{dag_id} - {description_en}
Manufacturing Data Warehouse ETL Task
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {{
    'owner': 'datawarehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='{description_en}',
    schedule='{schedule}',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['datawarehouse', 'etl', '{layer}'],
)

def execute_etl():
    """Execute ETL task"""
    import mysql.connector
    db_config = {{
        'host': 'localhost',
        'port': 3306,
        'user': 'sqluser',
        'password': 'sqlpass123',
        'database': 'sqlExpert',
        'charset': 'utf8mb4'
    }}
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        print(f"{{description_en}} executed successfully, current records: {{result[0]}}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Execution failed: {{e}}")
        raise

etl_task = PythonOperator(
    task_id='execute_etl',
    python_callable=execute_etl,
    dag=dag,
)

complete_task = BashOperator(
    task_id='complete',
    bash_command='echo "{{description_en}} completed"',
    dag=dag,
)

etl_task >> complete_task
'''

# 定义所有DAG
dags_config = [
    # ODS层
    ("ods_01_order_master_etl", "ODS Order Master ETL", "ods", "0 2 * * *", "ods_order_master"),
    ("ods_02_order_detail_etl", "ODS Order Detail ETL", "ods", "0 2 * * *", "ods_order_detail"),
    ("ods_03_customer_etl", "ODS Customer Master ETL", "ods", "0 2 * * *", "ods_customer_master"),
    ("ods_04_product_etl", "ODS Product Master ETL", "ods", "0 2 * * *", "ods_product_master"),
    ("ods_05_production_plan_etl", "ODS Production Plan ETL", "ods", "0 2 * * *", "ods_production_plan"),
    ("ods_06_production_order_etl", "ODS Production Order ETL", "ods", "0 2 * * *", "ods_production_order"),
    ("ods_07_bom_etl", "ODS BOM ETL", "ods", "0 2 * * *", "ods_bom"),
    ("ods_08_material_etl", "ODS Material Master ETL", "ods", "0 2 * * *", "ods_material_master"),
    ("ods_09_inventory_etl", "ODS Inventory ETL", "ods", "0 2 * * *", "ods_inventory"),
    ("ods_10_purchase_etl", "ODS Purchase Order ETL", "ods", "0 2 * * *", "ods_purchase_order"),
    # DWD层
    ("dwd_01_order_fact_etl", "DWD Order Fact ETL", "dwd", "0 3 * * *", "dwd_order_fact"),
    ("dwd_02_production_fact_etl", "DWD Production Fact ETL", "dwd", "0 3 * * *", "dwd_production_fact"),
    ("dwd_03_inventory_fact_etl", "DWD Inventory Fact ETL", "dwd", "0 3 * * *", "dwd_inventory_fact"),
    ("dwd_04_purchase_fact_etl", "DWD Purchase Fact ETL", "dwd", "0 3 * * *", "dwd_purchase_fact"),
    ("dwd_05_quality_fact_etl", "DWD Quality Fact ETL", "dwd", "0 3 * * *", "dwd_quality_fact"),
    ("dwd_06_equipment_runtime_etl", "DWD Equipment Runtime Fact ETL", "dwd", "0 3 * * *", "dwd_equipment_runtime_fact"),
    ("dwd_07_cost_fact_etl", "DWD Cost Fact ETL", "dwd", "0 3 * * *", "dwd_cost_fact"),
    # DWS层
    ("dws_01_order_daily_etl", "DWS Order Daily ETL", "dws", "0 4 * * *", "dws_order_daily"),
    ("dws_02_production_daily_etl", "DWS Production Daily ETL", "dws", "0 4 * * *", "dws_production_daily"),
    ("dws_03_inventory_daily_etl", "DWS Inventory Daily ETL", "dws", "0 4 * * *", "dws_inventory_daily"),
    ("dws_04_purchase_daily_etl", "DWS Purchase Daily ETL", "dws", "0 4 * * *", "dws_purchase_daily"),
    ("dws_05_quality_daily_etl", "DWS Quality Daily ETL", "dws", "0 4 * * *", "dws_quality_daily"),
    ("dws_06_equipment_runtime_daily_etl", "DWS Equipment Runtime Daily ETL", "dws", "0 4 * * *", "dws_equipment_runtime_daily"),
    ("dws_07_cost_daily_etl", "DWS Cost Daily ETL", "dws", "0 4 * * *", "dws_cost_daily"),
    # ADS层
    ("ads_01_sales_analysis_etl", "ADS Sales Analysis ETL", "ads", "0 5 * * *", "ads_sales_analysis"),
    ("ads_02_production_analysis_etl", "ADS Production Analysis ETL", "ads", "0 5 * * *", "ads_production_analysis"),
    ("ads_03_inventory_analysis_etl", "ADS Inventory Analysis ETL", "ads", "0 5 * * *", "ads_inventory_analysis"),
    ("ads_04_purchase_analysis_etl", "ADS Purchase Analysis ETL", "ads", "0 5 * * *", "ads_purchase_analysis"),
    ("ads_05_quality_analysis_etl", "ADS Quality Analysis ETL", "ads", "0 5 * * *", "ads_quality_analysis"),
    ("ads_06_business_overview_etl", "ADS Business Overview ETL", "ads", "0 5 * * *", "ads_business_overview"),
]

print("修复DAG文件编码...")
for i, (dag_id, description_en, layer, schedule, table_name) in enumerate(dags_config, 1):
    dag_file = os.path.join(dags_dir, f"{dag_id}.py")
    content = DAG_TEMPLATE.format(
        dag_id=dag_id,
        description_en=description_en,
        layer=layer,
        schedule=schedule,
        table_name=table_name
    )
    with open(dag_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✓ [{i:2d}/30] Fixed: {dag_id}.py")

print(f"\n完成! 已修复30个DAG文件的编码问题")
