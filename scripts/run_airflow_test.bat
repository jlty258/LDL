@echo off
chcp 65001 >nul
echo ============================================================
echo 测试 Airflow REST API
echo ============================================================
echo.

REM 检查 Python 是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到 Python，请先安装 Python
    pause
    exit /b 1
)

REM 安装依赖
echo 安装依赖包...
python -m pip install requests --quiet

REM 运行测试
echo.
echo 运行 Airflow REST API 测试...
python scripts\airflow_rest_api.py

pause



