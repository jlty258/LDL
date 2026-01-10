# 项目结构说明

## 目录结构

```
logical_dl/
├── docs/                          # 文档目录
│   ├── 架构文档.md                 # 整体架构设计文档
│   ├── 详细设计文档.md             # 详细实施设计文档
│   └── 迁移指南.md                 # 迁移操作指南
│
├── sqlmesh_project/                # SQLMesh 项目
│   ├── config.yaml                 # SQLMesh 配置文件
│   └── models/                     # SQLMesh 模型目录
│       ├── ods/                    # ODS 层模型
│       ├── dwd/                    # DWD 层模型
│       ├── dws/                    # DWS 层模型
│       └── ads/                    # ADS 层模型
│
├── dbt_project/                    # dbt 项目
│   ├── dbt_project.yml             # dbt 项目配置
│   ├── profiles.yml                # 数据库连接配置
│   ├── models/                     # dbt 模型目录
│   │   ├── staging/                # Staging 层（对应 ODS）
│   │   ├── intermediate/           # Intermediate 层（对应 DWD）
│   │   └── marts/                  # Marts 层（对应 DWS + ADS）
│   ├── macros/                     # 宏定义
│   │   └── database_utils.sql      # 数据库工具宏
│   ├── tests/                      # 测试文件
│   └── seeds/                      # 种子数据
│
├── scripts/                         # 工具脚本
│   ├── analyze_sql.py              # SQL 分析工具
│   ├── migrate_domain.py           # 分域迁移脚本
│   └── convert_sql.py              # SQL 转换工具（待实现）
│
├── config/                          # 配置文件
│   ├── migration_plan.yaml         # 迁移计划配置
│   ├── domain_mapping.yaml          # 业务域映射配置
│   └── database_config.yaml         # 数据库配置
│
├── README.md                        # 项目说明
├── PROJECT_STRUCTURE.md             # 本文件
└── .gitignore                       # Git 忽略文件
```

## 文件说明

### 文档文件

- **docs/架构文档.md**: 整体架构设计，包括技术选型、架构图、核心组件等
- **docs/详细设计文档.md**: 详细实施设计，包括项目结构、模型设计、工具脚本等
- **docs/迁移指南.md**: 迁移操作指南，包括快速开始、迁移步骤、常见问题等

### SQLMesh 项目

- **sqlmesh_project/config.yaml**: SQLMesh 配置文件，包含数据库连接、环境配置等
- **sqlmesh_project/models/**: SQLMesh 模型文件，按数据层组织

### dbt 项目

- **dbt_project/dbt_project.yml**: dbt 项目配置
- **dbt_project/profiles.yml**: 数据库连接配置，支持多数据库
- **dbt_project/macros/database_utils.sql**: 数据库工具宏，处理不同数据库的语法差异

### 工具脚本

- **scripts/analyze_sql.py**: SQL 分析工具，分析现有 SQL 文件的依赖关系、复杂度等
- **scripts/migrate_domain.py**: 分域迁移脚本，支持按业务域逐步迁移

### 配置文件

- **config/migration_plan.yaml**: 迁移计划配置，定义迁移阶段和任务
- **config/domain_mapping.yaml**: 业务域映射配置，定义各业务域的表和依赖关系
- **config/database_config.yaml**: 数据库配置，支持 MySQL、PostgreSQL、Greenplum 等

## 使用流程

1. **分析现有 SQL**: 运行 `python scripts/analyze_sql.py ../datawarehouse/sql`
2. **查看分析结果**: 查看 `analysis_results.json` 了解迁移建议
3. **配置数据库**: 编辑 `dbt_project/profiles.yml` 和 `sqlmesh_project/config.yaml`
4. **开始迁移**: 运行 `python scripts/migrate_domain.py sales postgres`
5. **验证数据**: 验证迁移后的数据一致性

## 下一步

1. 根据分析结果，开始转换 SQL 文件
2. 创建 SQLMesh 和 dbt 模型
3. 配置调度系统集成
4. 进行测试和验证
