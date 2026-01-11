#!/bin/bash
# 制造业数仓初始化脚本

echo "=========================================="
echo "制造业数仓项目初始化"
echo "=========================================="

# 1. 初始化数据库
echo "1. 初始化数据库..."
mysql -h localhost -P 3306 -u sqluser -psqlpass123 < datawarehouse/sql/01_init_database.sql
mysql -h localhost -P 3306 -u sqluser -psqlpass123 sqlExpert < datawarehouse/sql/02_ods_tables.sql
mysql -h localhost -P 3306 -u sqluser -psqlpass123 sqlExpert < datawarehouse/sql/03_dwd_tables.sql
mysql -h localhost -P 3306 -u sqluser -psqlpass123 sqlExpert < datawarehouse/sql/04_dws_tables.sql
mysql -h localhost -P 3306 -u sqluser -psqlpass123 sqlExpert < datawarehouse/sql/05_ads_tables.sql

echo "✓ 数据库表结构创建完成"

# 2. 生成测试数据
echo "2. 生成测试数据..."
python3 datawarehouse/sql/06_generate_test_data.py

echo "✓ 测试数据生成完成"

# 3. 创建调度任务
echo "3. 创建调度任务..."
echo "  - DolphinScheduler任务: 运行 python datawarehouse/schedulers/create_dolphinscheduler_tasks.py"
echo "  - Airflow DAG任务: 已创建在 airflow/dags/ 目录"

echo ""
echo "=========================================="
echo "初始化完成！"
echo "=========================================="
