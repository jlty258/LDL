# SQLMesh 功能测试脚本 (PowerShell)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "开始测试 SQLMesh 镜像功能" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# 检查容器是否运行
Write-Host ""
Write-Host "1. 检查 SQLMesh 容器状态..." -ForegroundColor Yellow
$container = docker ps --filter "name=sqlmesh-test" --format "{{.Names}}"
if (-not $container) {
    Write-Host "错误: SQLMesh 容器未运行，请先启动容器" -ForegroundColor Red
    Write-Host "运行: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}
Write-Host "容器状态: $container" -ForegroundColor Green

Write-Host ""
Write-Host "2. 检查 SQLMesh 版本..." -ForegroundColor Yellow
docker exec sqlmesh-test sqlmesh --version

Write-Host ""
Write-Host "3. 验证 SQLMesh 配置..." -ForegroundColor Yellow
docker exec sqlmesh-test sqlmesh info

Write-Host ""
Write-Host "4. 运行 SQLMesh 计划..." -ForegroundColor Yellow
docker exec sqlmesh-test sqlmesh plan --no-prompts

Write-Host ""
Write-Host "5. 应用 SQLMesh 计划..." -ForegroundColor Yellow
docker exec sqlmesh-test sqlmesh apply --no-prompts

Write-Host ""
Write-Host "6. 验证数据..." -ForegroundColor Yellow
docker exec postgres-db psql -U postgres -d sqlExpert -c 'SET search_path TO "default"; SELECT * FROM test_model LIMIT 5;'

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "SQLMesh 功能测试完成！" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
