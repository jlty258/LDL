#!/usr/bin/env python3
"""
SQL 分析工具
分析现有 SQL 文件，提取：
1. 表依赖关系
2. 增量更新模式
3. 时间范围
4. 复杂度评分
5. 数据库特定语法
"""

import re
import json
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

class SQLAnalyzer:
    def __init__(self, sql_dir: str):
        self.sql_dir = Path(sql_dir)
        self.dependencies = defaultdict(set)
        self.tables = set()
        self.analysis_results = []
    
    def analyze_all(self) -> Dict[str, Any]:
        """分析所有 SQL 文件"""
        for sql_file in self.sql_dir.glob('*.sql'):
            result = self.analyze_file(sql_file)
            self.analysis_results.append(result)
        
        return {
            'files': self.analysis_results,
            'summary': self._generate_summary()
        }
    
    def analyze_file(self, file_path: Path) -> Dict[str, Any]:
        """分析单个 SQL 文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return {
            'file': str(file_path),
            'layer': self._identify_layer(file_path.name),
            'domain': self._identify_domain(content),
            'tables': self._extract_tables(content),
            'dependencies': self._extract_dependencies(content),
            'is_incremental': self._detect_incremental(content),
            'time_range': self._extract_time_range(content),
            'complexity': self._calculate_complexity(content),
            'db_specific': self._detect_db_specific(content),
            'migration_recommendation': self._recommend_migration_tool(content)
        }
    
    def _identify_layer(self, filename: str) -> str:
        """识别数据层"""
        filename_lower = filename.lower()
        if 'ods' in filename_lower:
            return 'ods'
        elif 'dwd' in filename_lower:
            return 'dwd'
        elif 'dws' in filename_lower:
            return 'dws'
        elif 'ads' in filename_lower:
            return 'ads'
        return 'unknown'
    
    def _identify_domain(self, content: str) -> str:
        """识别业务域"""
        domains = {
            'sales': ['order', 'customer', 'sales'],
            'production': ['production', 'work_order', 'bom'],
            'inventory': ['inventory', 'warehouse', 'material'],
            'purchase': ['purchase', 'supplier'],
            'quality': ['quality', 'defect', 'inspection'],
            'cost': ['cost', 'cost_center']
        }
        
        content_lower = content.lower()
        for domain, keywords in domains.items():
            if any(kw in content_lower for kw in keywords):
                return domain
        return 'common'
    
    def _extract_tables(self, content: str) -> List[str]:
        """提取表名"""
        patterns = [
            r'FROM\s+([a-z_]+)',
            r'JOIN\s+([a-z_]+)',
            r'INTO\s+([a-z_]+)',
            r'UPDATE\s+([a-z_]+)'
        ]
        
        tables = set()
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            tables.update(matches)
        
        return sorted(list(tables))
    
    def _extract_dependencies(self, content: str) -> Dict[str, List[str]]:
        """提取依赖关系"""
        insert_pattern = r'INSERT\s+INTO\s+([a-z_]+)'
        target_tables = re.findall(insert_pattern, content, re.IGNORECASE)
        
        source_tables = self._extract_tables(content)
        
        dependencies = {}
        for target in target_tables:
            dependencies[target] = [t for t in source_tables if t != target]
        
        return dependencies
    
    def _detect_incremental(self, content: str) -> Dict[str, Any]:
        """检测增量更新模式"""
        patterns = {
            'mysql_upsert': r'ON\s+DUPLICATE\s+KEY\s+UPDATE',
            'postgres_upsert': r'ON\s+CONFLICT',
            'merge': r'MERGE\s+INTO',
            'time_filter': r'WHERE.*>=.*DATE|@start_ds|@end_ds'
        }
        
        for pattern_name, pattern in patterns.items():
            if re.search(pattern, content, re.IGNORECASE):
                return {
                    'type': pattern_name,
                    'detected': True,
                    'recommendation': '使用 SQLMesh INCREMENTAL_BY_TIME_RANGE'
                }
        
        return {'detected': False}
    
    def _extract_time_range(self, content: str) -> Dict[str, Any]:
        """提取时间范围"""
        mysql_pattern = r'DATE_SUB\(CURDATE\(\),\s+INTERVAL\s+(\d+)\s+(YEAR|MONTH|DAY)\)'
        mysql_match = re.search(mysql_pattern, content, re.IGNORECASE)
        
        if mysql_match:
            interval = int(mysql_match.group(1))
            unit = mysql_match.group(2).lower()
            return {
                'interval': interval,
                'unit': unit,
                'recommendation': f'SQLMesh start: 计算 {interval} {unit} 前的日期'
            }
        
        return None
    
    def _calculate_complexity(self, content: str) -> Dict[str, Any]:
        """计算 SQL 复杂度"""
        metrics = {
            'subqueries': len(re.findall(r'\(SELECT', content, re.IGNORECASE)),
            'joins': len(re.findall(r'\b(INNER|LEFT|RIGHT|FULL)\s+JOIN\b', content, re.IGNORECASE)),
            'aggregations': len(re.findall(r'\b(SUM|COUNT|AVG|MAX|MIN)\s*\(', content, re.IGNORECASE)),
            'case_statements': len(re.findall(r'\bCASE\b', content, re.IGNORECASE)),
            'lines': len(content.split('\n'))
        }
        
        score = (
            metrics['subqueries'] * 3 +
            metrics['joins'] * 2 +
            metrics['aggregations'] * 1 +
            metrics['case_statements'] * 1
        )
        
        metrics['score'] = score
        metrics['level'] = 'high' if score > 20 else 'medium' if score > 10 else 'low'
        
        return metrics
    
    def _detect_db_specific(self, content: str) -> Dict[str, List[str]]:
        """检测数据库特定语法"""
        db_specific = {
            'mysql': [],
            'postgres': [],
            'generic': True
        }
        
        mysql_patterns = [
            (r'ON\s+DUPLICATE\s+KEY\s+UPDATE', 'ON DUPLICATE KEY UPDATE'),
            (r'CURDATE\(\)', 'CURDATE()'),
            (r'DATE_SUB\(', 'DATE_SUB()'),
            (r'QUARTER\(', 'QUARTER()')
        ]
        
        postgres_patterns = [
            (r'ON\s+CONFLICT', 'ON CONFLICT'),
            (r'CURRENT_DATE', 'CURRENT_DATE'),
            (r'DATE_TRUNC\(', 'DATE_TRUNC()'),
            (r'EXTRACT\(', 'EXTRACT()')
        ]
        
        for pattern, name in mysql_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                db_specific['mysql'].append(name)
                db_specific['generic'] = False
        
        for pattern, name in postgres_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                db_specific['postgres'].append(name)
                db_specific['generic'] = False
        
        return db_specific
    
    def _recommend_migration_tool(self, content: str) -> str:
        """推荐迁移工具"""
        is_incremental = self._detect_incremental(content)['detected']
        complexity = self._calculate_complexity(content)
        db_specific = self._detect_db_specific(content)
        
        if is_incremental and complexity['level'] == 'high':
            return 'SQLMesh (复杂增量更新)'
        elif is_incremental:
            return 'SQLMesh (增量更新)'
        elif complexity['level'] == 'high':
            return 'SQLMesh (高复杂度)'
        elif not db_specific['generic']:
            return 'SQLMesh (数据库特定语法)'
        else:
            return 'dbt (简单转换)'
    
    def _generate_summary(self) -> Dict[str, Any]:
        """生成分析摘要"""
        total_files = len(self.analysis_results)
        sqlmesh_count = sum(1 for r in self.analysis_results 
                           if 'SQLMesh' in r['migration_recommendation'])
        dbt_count = total_files - sqlmesh_count
        
        return {
            'total_files': total_files,
            'sqlmesh_recommended': sqlmesh_count,
            'dbt_recommended': dbt_count,
            'domains': self._count_by_domain(),
            'layers': self._count_by_layer()
        }
    
    def _count_by_domain(self) -> Dict[str, int]:
        """按业务域统计"""
        domains = defaultdict(int)
        for result in self.analysis_results:
            domains[result['domain']] += 1
        return dict(domains)
    
    def _count_by_layer(self) -> Dict[str, int]:
        """按数据层统计"""
        layers = defaultdict(int)
        for result in self.analysis_results:
            layers[result['layer']] += 1
        return dict(layers)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python analyze_sql.py <sql_directory> [output_file]")
        sys.exit(1)
    
    sql_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'analysis_results.json'
    
    analyzer = SQLAnalyzer(sql_dir)
    results = analyzer.analyze_all()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"分析完成！共分析 {results['summary']['total_files']} 个文件")
    print(f"推荐使用 SQLMesh: {results['summary']['sqlmesh_recommended']} 个")
    print(f"推荐使用 dbt: {results['summary']['dbt_recommended']} 个")
    print(f"结果已保存到: {output_file}")
