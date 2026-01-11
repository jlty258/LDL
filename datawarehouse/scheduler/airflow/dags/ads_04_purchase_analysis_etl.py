"""
Airflow DAG: ads_04_purchase_analysis_etl

ADS层-采购分析报表ETL
Generated from SQL file: 07_complex_etl.sql
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from pathlib import Path

# Default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# DAG definition
dag = DAG(
    'ads_04_purchase_analysis_etl',
    default_args=default_args,
    description='ADS层-采购分析报表ETL',
    schedule='0 5 * * *',  # Use 'schedule' instead of deprecated 'schedule_interval'
    catchup=False,
    tags=['etl', 'datawarehouse', 'ads'],
)

# Read SQL file content
def read_sql_file():
    """Read SQL file content"""
    # Try multiple possible paths
    sql_paths = [
        Path(__file__).parent / 'sql' / '07_complex_etl.sql',
        Path('/opt/airflow/dags/sql') / '07_complex_etl.sql',
        Path('/workspace/datawarehouse/sql') / '07_complex_etl.sql',
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
