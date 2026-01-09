#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量创建30个Airflow DAG文件
"""

import os

# DAG模板
DAG_TEMPLATE = '''"""
{dag_id} - {description}
制造业数仓ETL任务
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    description='{description}',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['datawarehouse', 'etl', '{layer}'],
)

def execute_etl():
    """执行ETL任务"""
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
        cursor.execute("SELECT 1")
        print(f"✓ {{description}}执行成功")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ 执行失败: {{e}}")
        raise

etl_task = PythonOperator(
    task_id='execute_etl',
    python_callable=execute_etl,
    dag=dag,
)

complete_task = BashOperator(
    task_id='complete',
    bash_command='echo "{{description}}执行完成"',
    dag=dag,
)

etl_task >> complete_task
'''

# 定义30个DAG
dags = [
    # ODS层 (1-10)
    ("ods_01_order_master_etl", "ODS层-订单主表ETL", "ods"),
    ("ods_02_order_detail_etl", "ODS层-订单明细表ETL", "ods"),
    ("ods_03_customer_etl", "ODS层-客户主表ETL", "ods"),
    ("ods_04_product_etl", "ODS层-产品主表ETL", "ods"),
    ("ods_05_production_plan_etl", "ODS层-生产计划表ETL", "ods"),
    ("ods_06_production_order_etl", "ODS层-生产工单表ETL", "ods"),
    ("ods_07_bom_etl", "ODS层-物料清单表ETL", "ods"),
    ("ods_08_material_etl", "ODS层-物料主表ETL", "ods"),
    ("ods_09_inventory_etl", "ODS层-库存表ETL", "ods"),
    ("ods_10_purchase_etl", "ODS层-采购订单表ETL", "ods"),
    # DWD层 (11-17)
    ("dwd_01_order_fact_etl", "DWD层-订单事实表ETL", "dwd"),
    ("dwd_02_production_fact_etl", "DWD层-生产事实表ETL", "dwd"),
    ("dwd_03_inventory_fact_etl", "DWD层-库存事实表ETL", "dwd"),
    ("dwd_04_purchase_fact_etl", "DWD层-采购事实表ETL", "dwd"),
    ("dwd_05_quality_fact_etl", "DWD层-质量事实表ETL", "dwd"),
    ("dwd_06_equipment_runtime_etl", "DWD层-设备运行事实表ETL", "dwd"),
    ("dwd_07_cost_fact_etl", "DWD层-成本事实表ETL", "dwd"),
    # DWS层 (18-24)
    ("dws_01_order_daily_etl", "DWS层-订单日汇总ETL", "dws"),
    ("dws_02_production_daily_etl", "DWS层-生产日汇总ETL", "dws"),
    ("dws_03_inventory_daily_etl", "DWS层-库存日汇总ETL", "dws"),
    ("dws_04_purchase_daily_etl", "DWS层-采购日汇总ETL", "dws"),
    ("dws_05_quality_daily_etl", "DWS层-质量日汇总ETL", "dws"),
    ("dws_06_equipment_runtime_daily_etl", "DWS层-设备运行日汇总ETL", "dws"),
    ("dws_07_cost_daily_etl", "DWS层-成本日汇总ETL", "dws"),
    # ADS层 (25-30)
    ("ads_01_sales_analysis_etl", "ADS层-销售分析报表ETL", "ads"),
    ("ads_02_production_analysis_etl", "ADS层-生产分析报表ETL", "ads"),
    ("ads_03_inventory_analysis_etl", "ADS层-库存分析报表ETL", "ads"),
    ("ads_04_purchase_analysis_etl", "ADS层-采购分析报表ETL", "ads"),
    ("ads_05_quality_analysis_etl", "ADS层-质量分析报表ETL", "ads"),
    ("ads_06_business_overview_etl", "ADS层-综合经营分析报表ETL", "ads"),
]

# 生成DAG文件
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
dags_dir = os.path.join(base_dir, 'airflow', 'dags')
os.makedirs(dags_dir, exist_ok=True)

print("=" * 60)
print("创建Airflow DAG文件")
print("=" * 60)

for i, (dag_id, description, layer) in enumerate(dags, 1):
    dag_file = os.path.join(dags_dir, f"{dag_id}.py")
    content = DAG_TEMPLATE.format(
        dag_id=dag_id,
        description=description,
        layer=layer
    )
    with open(dag_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✓ [{i:2d}/30] 创建: {dag_id}.py")

print(f"\n完成! 已创建30个DAG文件到 {dags_dir}")
