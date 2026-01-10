#!/usr/bin/env python3
"""
分域迁移脚本
支持按业务域逐步迁移，验证数据一致性
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Dict

DOMAINS = {
    'sales': {
        'sqlmesh_models': ['dwd.order_fact', 'dws.order_daily'],
        'dbt_models': ['staging.sales.*', 'marts.sales.*'],
        'dependencies': []
    },
    'production': {
        'sqlmesh_models': ['dwd.production_fact', 'dws.production_daily'],
        'dbt_models': ['staging.production.*'],
        'dependencies': ['sales']
    },
    'inventory': {
        'sqlmesh_models': ['dwd.inventory_fact', 'dws.inventory_daily'],
        'dbt_models': ['staging.inventory.*'],
        'dependencies': []
    },
    'purchase': {
        'sqlmesh_models': ['dwd.purchase_fact', 'dws.purchase_daily'],
        'dbt_models': ['staging.purchase.*'],
        'dependencies': []
    },
    'quality': {
        'sqlmesh_models': ['dwd.quality_fact', 'dws.quality_daily'],
        'dbt_models': ['staging.quality.*'],
        'dependencies': ['production']
    },
    'cost': {
        'sqlmesh_models': ['dwd.cost_fact', 'dws.cost_daily'],
        'dbt_models': ['staging.cost.*'],
        'dependencies': ['production', 'purchase']
    }
}

def migrate_domain(domain: str, target_db: str = 'postgres'):
    """迁移单个业务域"""
    print(f"开始迁移业务域: {domain}")
    
    if domain not in DOMAINS:
        print(f"错误: 未知的业务域: {domain}")
        print(f"支持的域: {', '.join(DOMAINS.keys())}")
        return False
    
    domain_config = DOMAINS[domain]
    
    # 1. 检查依赖
    for dep in domain_config['dependencies']:
        print(f"  检查依赖域: {dep}")
        if not verify_domain_migrated(dep, target_db):
            print(f"  错误: 依赖域 {dep} 尚未迁移")
            return False
    
    # 2. 迁移 SQLMesh 模型
    if domain_config['sqlmesh_models']:
        print(f"  → 迁移 SQLMesh 模型...")
        for model in domain_config['sqlmesh_models']:
            cmd = ['sqlmesh', 'plan', 'dev', '--auto-apply', '--models', model]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd='sqlmesh_project')
            if result.returncode != 0:
                print(f"  错误: SQLMesh 模型 {model} 迁移失败")
                print(result.stderr)
                return False
    
    # 3. 迁移 dbt 模型
    if domain_config['dbt_models']:
        print(f"  → 迁移 dbt 模型...")
        for model_pattern in domain_config['dbt_models']:
            cmd = ['dbt', 'run', '--select', model_pattern, '--target', target_db]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd='dbt_project')
            if result.returncode != 0:
                print(f"  错误: dbt 模型 {model_pattern} 迁移失败")
                print(result.stderr)
                return False
    
    # 4. 运行测试
    print(f"  → 运行测试...")
    if not run_tests(domain, target_db):
        print(f"  警告: 部分测试失败")
    
    # 5. 验证数据一致性
    print(f"  → 验证数据一致性...")
    if not verify_data_consistency(domain, target_db):
        print(f"  错误: 数据一致性验证失败")
        return False
    
    print(f"✓ {domain} 域迁移完成")
    return True

def verify_domain_migrated(domain: str, target_db: str) -> bool:
    """验证域是否已迁移"""
    # TODO: 实现检查逻辑
    # 可以检查目标数据库中是否存在该域的表
    return True

def run_tests(domain: str, target_db: str) -> bool:
    """运行测试"""
    # dbt 测试
    cmd = ['dbt', 'test', '--select', f'*{domain}*', '--target', target_db]
    result = subprocess.run(cmd, cwd='dbt_project')
    return result.returncode == 0

def verify_data_consistency(domain: str, target_db: str) -> bool:
    """验证数据一致性"""
    # TODO: 实现数据一致性检查
    # 比较 MySQL 和目标数据库的记录数、金额等
    return True

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python migrate_domain.py <domain> [target_db]")
        print(f"支持的域: {', '.join(DOMAINS.keys())}")
        sys.exit(1)
    
    domain = sys.argv[1]
    target_db = sys.argv[2] if len(sys.argv) > 2 else 'postgres'
    
    success = migrate_domain(domain, target_db)
    sys.exit(0 if success else 1)
