@echo off
chcp 65001 >nul
echo ================================================================================
echo 数据库库表统计报告
echo ================================================================================

echo.
echo ================================================================================
echo MySQL 数据库统计
echo ================================================================================
echo.

echo 数据库列表:
docker exec mysql-db mysql -usqluser -psqlpass123 -e "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys') ORDER BY SCHEMA_NAME;" 2>nul

echo.
echo 数据库: sqlExpert
echo --------------------------------------------------------------------------------
echo 表列表:
docker exec mysql-db mysql -usqluser -psqlpass123 sqlExpert -e "SHOW TABLES;" 2>nul

echo.
echo 表统计信息:
docker exec mysql-db mysql -usqluser -psqlpass123 sqlExpert -e "SELECT TABLE_NAME as '表名', TABLE_ROWS as '行数', (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='sqlExpert' AND TABLE_NAME=t.TABLE_NAME) as '字段数' FROM information_schema.TABLES t WHERE TABLE_SCHEMA = 'sqlExpert' ORDER BY TABLE_NAME;" 2>nul

echo.
echo ================================================================================
echo PostgreSQL 数据库统计
echo ================================================================================
echo.

echo 数据库列表:
docker exec postgres-db psql -U postgres -c "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1') ORDER BY datname;" 2>nul

echo.
echo 数据库: sqlExpert
echo --------------------------------------------------------------------------------
echo 表列表:
docker exec postgres-db psql -U postgres -d sqlExpert -c "\dt" 2>nul

echo.
echo ================================================================================
pause
