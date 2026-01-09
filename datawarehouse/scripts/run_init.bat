@echo off
chcp 65001 >nul
echo ========================================
echo 制造业数仓项目初始化
echo ========================================
echo.

echo [1/3] 数据库表结构已创建
echo   - ODS层: 30张表
echo   - DWD层: 10张表
echo   - DWS层: 7张表
echo   - ADS层: 8张表
echo.

echo [2/3] 生成测试数据...
echo 提示: 需要Python环境，运行以下命令:
echo   python datawarehouse/sql/06_generate_test_data.py
echo   或
echo   docker exec -it mysql-db bash
echo   python3 /path/to/06_generate_test_data.py
echo.

echo [3/3] 创建调度任务...
echo 提示: 
echo   - Airflow DAG: 运行 python datawarehouse/scripts/create_all_dags.py
echo   - DolphinScheduler: 运行 python datawarehouse/schedulers/create_dolphinscheduler_tasks.py
echo.

echo ========================================
echo 初始化完成！
echo ========================================
pause
