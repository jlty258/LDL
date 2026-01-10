#!/bin/bash

# dbt 功能测试脚本

echo "========================================="
echo "开始测试 dbt 镜像功能"
echo "========================================="

# 检查容器是否运行
echo ""
echo "1. 检查 dbt 容器状态..."
docker ps | grep dbt-test

if [ $? -ne 0 ]; then
    echo "错误: dbt 容器未运行，请先启动容器"
    echo "运行: docker-compose up -d"
    exit 1
fi

echo ""
echo "2. 检查 dbt 版本..."
docker exec dbt-test dbt --version

echo ""
echo "3. 验证 dbt 配置..."
docker exec dbt-test dbt debug --profiles-dir /root/.dbt

echo ""
echo "4. 运行 dbt 编译..."
docker exec dbt-test dbt compile --profiles-dir /root/.dbt

echo ""
echo "5. 运行 dbt 模型..."
docker exec dbt-test dbt run --profiles-dir /root/.dbt

echo ""
echo "6. 运行 dbt 测试..."
docker exec dbt-test dbt test --profiles-dir /root/.dbt

echo ""
echo "7. 生成 dbt 文档..."
docker exec dbt-test dbt docs generate --profiles-dir /root/.dbt

echo ""
echo "========================================="
echo "dbt 功能测试完成！"
echo "========================================="
