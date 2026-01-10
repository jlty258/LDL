# 数仓解耦与多数据库迁移项目

## 项目概述

本项目旨在将现有的 MySQL 数仓系统解耦，支持多数据库迁移（MySQL、PostgreSQL、Greenplum 等），实现业务逻辑与数据库的分离，支持一键迁移和分域迁移。

## 技术栈

- **SQLMesh**: 核心 ETL 层，处理复杂增量更新和历史数据回溯
- **dbt**: 辅助层，处理数据质量测试和文档生成
- **DolphinScheduler**: 调度系统（现有）
- **多数据库支持**: MySQL、PostgreSQL、Greenplum、ClickHouse

## 项目结构

```
logical_dl/
├── docs/                    # 文档目录
│   ├── 架构文档.md          # 架构设计文档
│   └── 详细设计文档.md      # 详细设计文档
│
├── sqlmesh_project/         # SQLMesh 项目
│   ├── config.yaml         # SQLMesh 配置
│   └── models/             # SQLMesh 模型
│
├── dbt_project/            # dbt 项目
│   ├── dbt_project.yml     # dbt 项目配置
│   ├── profiles.yml        # 数据库连接配置
│   └── models/             # dbt 模型
│
├── scripts/                # 工具脚本
│   ├── analyze_sql.py      # SQL 分析工具
│   ├── convert_sql.py       # SQL 转换工具
│   └── migrate_domain.py    # 分域迁移脚本
│
└── config/                  # 配置文件
    ├── migration_plan.yaml  # 迁移计划
    └── database_config.yaml # 数据库配置
```

## 快速开始

### 1. 环境准备

```bash
# 安装 SQLMesh
pip install sqlmesh

# 安装 dbt
pip install dbt-core dbt-postgres dbt-mysql

# 配置环境变量
export MYSQL_PASSWORD="sqlpass123"
export POSTGRES_PASSWORD="postgres123"
```

### 2. 初始化项目

```bash
# 初始化 SQLMesh 项目
cd sqlmesh_project
sqlmesh init

# 初始化 dbt 项目
cd ../dbt_project
dbt init
```

### 3. 分析现有 SQL

```bash
# 分析现有 SQL 文件
python scripts/analyze_sql.py ../datawarehouse/sql
```

### 4. 开始迁移

```bash
# 迁移单个业务域
python scripts/migrate_domain.py sales postgres
```

## 文档

- [架构文档](docs/架构文档.md) - 整体架构设计
- [详细设计文档](docs/详细设计文档.md) - 详细实施设计

## 迁移策略

### 分域迁移顺序

1. **Sales 域** (优先级 1) - 试点
2. **Production 域** (优先级 2) - 核心业务
3. **其他域** (优先级 3-6) - 按依赖关系

### 迁移步骤

1. SQL 分析和工具选择
2. SQL 转换（MySQL -> 通用 SQL）
3. 模型迁移（按域逐步迁移）
4. 数据一致性验证
5. 调度系统集成
6. 生产上线

## 工具说明

### SQL 分析工具

分析现有 SQL 文件，识别：
- 表依赖关系
- 增量更新模式
- 时间范围
- 复杂度评分
- 数据库特定语法

### SQL 转换工具

将 MySQL SQL 转换为：
- SQLMesh 模型格式
- dbt 模型格式
- 通用 SQL（适配多数据库）

### 分域迁移脚本

支持按业务域逐步迁移：
- 自动处理依赖关系
- 数据一致性验证
- 回滚支持

## 配置说明

### 数据库配置

支持多数据库配置：
- MySQL
- PostgreSQL
- Greenplum
- ClickHouse

### 迁移计划

配置迁移阶段和任务：
- 准备阶段
- 试点迁移
- 全面迁移
- 上线阶段

## 测试

### 单元测试

```bash
pytest tests/test_sql_converter.py
```

### 集成测试

```bash
pytest tests/test_migration.py
```

### 数据一致性测试

```bash
python scripts/verify_data.py sales mysql postgres
```

## 监控

- 任务执行状态
- 数据质量指标
- 性能指标
- 告警机制

## 参考资源

- [SQLMesh 文档](https://sqlmesh.readthedocs.io/)
- [dbt 文档](https://docs.getdbt.com/)
- [DolphinScheduler 文档](https://dolphinscheduler.apache.org/)

## 许可证

内部项目
