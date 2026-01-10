#!/bin/bash

# SQLMesh 功能测试脚本

echo "========================================="
echo "开始测试 SQLMesh 镜像功能"
echo "========================================="

# 检查容器是否运行
echo ""
echo "1. 检查 SQLMesh 容器状态..."
docker ps | grep sqlmesh-test

if [ $? -ne 0 ]; then
    echo "错误: SQLMesh 容器未运行，请先启动容器"
    echo "运行: docker-compose up -d"
    exit 1
fi

echo ""
echo "2. 检查 SQLMesh 版本..."
docker exec sqlmesh-test sqlmesh --version

echo ""
echo "3. 验证 SQLMesh 配置..."
docker exec sqlmesh-test sqlmesh info

echo ""
echo "4. 运行 SQLMesh 计划..."
docker exec sqlmesh-test sqlmesh plan --no-prompts

echo ""
echo "5. 应用 SQLMesh 计划..."
docker exec sqlmesh-test sqlmesh apply --no-prompts

echo ""
echo "6. 验证模型..."
docker exec sqlmesh-test sqlmesh table info sqlmesh_test.test_model

echo ""
echo "========================================="
echo "SQLMesh 功能测试完成！"
echo "========================================="
