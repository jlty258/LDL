# dbt 功能测试脚本 (PowerShell)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "开始测试 dbt 镜像功能" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# 检查容器是否运行
Write-Host ""
Write-Host "1. 检查 dbt 容器状态..." -ForegroundColor Yellow
$container = docker ps --filter "name=dbt-test" --format "{{.Names}}"
if (-not $container) {
    Write-Host "错误: dbt 容器未运行，请先启动容器" -ForegroundColor Red
    Write-Host "运行: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}
Write-Host "容器状态: $container" -ForegroundColor Green

Write-Host ""
Write-Host "2. 检查 dbt 版本..." -ForegroundColor Yellow
docker exec dbt-test dbt --version

Write-Host ""
Write-Host "3. 验证 dbt 配置..." -ForegroundColor Yellow
docker exec dbt-test dbt debug --profiles-dir /root/.dbt

Write-Host ""
Write-Host "4. 运行 dbt 编译..." -ForegroundColor Yellow
docker exec dbt-test dbt compile --profiles-dir /root/.dbt

Write-Host ""
Write-Host "5. 运行 dbt 模型..." -ForegroundColor Yellow
docker exec dbt-test dbt run --profiles-dir /root/.dbt

Write-Host ""
Write-Host "6. 运行 dbt 测试..." -ForegroundColor Yellow
docker exec dbt-test dbt test --profiles-dir /root/.dbt

Write-Host ""
Write-Host "7. 生成 dbt 文档..." -ForegroundColor Yellow
docker exec dbt-test dbt docs generate --profiles-dir /root/.dbt

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "dbt 功能测试完成！" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
