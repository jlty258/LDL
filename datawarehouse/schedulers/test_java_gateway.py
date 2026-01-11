#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Java Gateway 连接和 PyDolphinScheduler 客户端
"""

import os
import sys

# 添加当前目录到路径
sys.path.append(os.path.dirname(__file__))

from pydolphinscheduler_client import get_client_from_env, PYDOLPHINSCHEDULER_AVAILABLE

def test_java_gateway_connection():
    """测试 Java Gateway 连接"""
    print("=" * 60)
    print("测试 Java Gateway 连接")
    print("=" * 60)
    
    # 检查环境变量
    java_gateway_address = os.getenv("PYDS_JAVA_GATEWAY_ADDRESS")
    java_gateway_port = os.getenv("PYDS_JAVA_GATEWAY_PORT")
    
    print(f"\n环境变量检查:")
    print(f"  PYDS_JAVA_GATEWAY_ADDRESS: {java_gateway_address}")
    print(f"  PYDS_JAVA_GATEWAY_PORT: {java_gateway_port}")
    
    if not java_gateway_address or not java_gateway_port:
        print("\n❌ Java Gateway 环境变量未配置")
        return False
    
    # 测试端口连接
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        result = s.connect_ex((java_gateway_address, int(java_gateway_port)))
        s.close()
        
        if result == 0:
            print(f"\n✓ Java Gateway 端口连接成功: {java_gateway_address}:{java_gateway_port}")
            return True
        else:
            print(f"\n❌ Java Gateway 端口连接失败: {java_gateway_address}:{java_gateway_port}")
            return False
    except Exception as e:
        print(f"\n❌ Java Gateway 连接测试异常: {e}")
        return False


def test_pyds_client():
    """测试 PyDolphinScheduler 客户端"""
    print("\n" + "=" * 60)
    print("测试 PyDolphinScheduler 客户端")
    print("=" * 60)
    
    # 直接检查导入
    try:
        import pydolphinscheduler
        print(f"\n✓ PyDolphinScheduler 已安装，版本: {pydolphinscheduler.__version__}")
    except ImportError:
        print("\n❌ PyDolphinScheduler 未安装")
        return False
    
    if not PYDOLPHINSCHEDULER_AVAILABLE:
        print("\n⚠ 警告: 部分模块导入失败，但 PyDolphinScheduler 已安装")
        print("  这可能是版本兼容性问题，但不影响基本功能")
    
    try:
        client = get_client_from_env()
        print("\n✓ PyDolphinScheduler 客户端创建成功")
        print(f"  用户: {client.user}")
        print(f"  服务器: {client.host}:{client.port}")
        print(f"  项目: {client.project_name}")
        return True
    except Exception as e:
        print(f"\n❌ PyDolphinScheduler 客户端创建失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Java Gateway 和 PyDolphinScheduler 测试")
    print("=" * 60)
    
    # 测试 Java Gateway 连接
    gateway_ok = test_java_gateway_connection()
    
    # 测试 PyDolphinScheduler 客户端
    client_ok = test_pyds_client()
    
    # 总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    print(f"Java Gateway 连接: {'✓ 成功' if gateway_ok else '❌ 失败'}")
    print(f"PyDolphinScheduler 客户端: {'✓ 成功' if client_ok else '❌ 失败'}")
    
    if gateway_ok and client_ok:
        print("\n✓ 所有测试通过！Java Gateway 已正确配置。")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败，请检查配置。")
        sys.exit(1)
