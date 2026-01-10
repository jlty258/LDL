# dbt 镜像验证

本目录包含用于验证 dbt Docker 镜像功能的测试项目。

## 目录结构

```
dbt/
├── docker-compose.yml      # dbt 容器配置
├── dbt_project/            # dbt 项目目录
│   ├── dbt_project.yml     # dbt 项目配置
│   └── models/             # dbt 模型
│       ├── schema.yml      # 模型定义和测试
│       ├── test_model.sql  # 测试模型
│       └── example_model.sql # 示例模型
├── profiles/               # dbt profiles 配置
│   └── profiles.yml        # 数据库连接配置
├── test_dbt.sh            # Linux/Mac 测试脚本
├── test_dbt.ps1           # Windows PowerShell 测试脚本
└── README.md              # 本文件
```

## 快速开始

### 1. 确保 PostgreSQL 数据库运行

dbt 需要连接到 PostgreSQL 数据库。确保 `postgres-db` 容器正在运行：

```bash
# 在 physical_dl 目录下启动数据库
cd ../physical_dl
docker-compose up -d postgres
```

### 2. 启动 dbt 容器

```bash
cd dbt
docker-compose up -d
```

### 3. 运行测试

**Windows (PowerShell):**
```powershell
.\test_dbt.ps1
```

**Linux/Mac:**
```bash
chmod +x test_dbt.sh
./test_dbt.sh
```

## 手动测试命令

你也可以手动执行 dbt 命令来验证功能：

```bash
# 检查 dbt 版本
docker exec dbt-test dbt --version

# 验证配置
docker exec dbt-test dbt debug --profiles-dir /root/.dbt

# 编译项目
docker exec dbt-test dbt compile --profiles-dir /root/.dbt

# 运行模型
docker exec dbt-test dbt run --profiles-dir /root/.dbt

# 运行测试
docker exec dbt-test dbt test --profiles-dir /root/.dbt

# 生成文档
docker exec dbt-test dbt docs generate --profiles-dir /root/.dbt

# 查看帮助
docker exec dbt-test dbt --help
```

## 测试内容

测试脚本会验证以下功能：

1. ✅ 容器运行状态
2. ✅ dbt 版本信息
3. ✅ 数据库连接配置
4. ✅ 项目编译
5. ✅ 模型运行
6. ✅ 数据测试
7. ✅ 文档生成

## 数据库连接

dbt 连接到 PostgreSQL 数据库：
- **主机**: postgres-db (容器名称)
- **端口**: 5432
- **数据库**: sqlExpert
- **用户**: postgres
- **密码**: postgres123
- **Schema**: dbt_test

## 注意事项

- 确保 PostgreSQL 容器已启动并运行
- 首次运行前，PostgreSQL 会自动创建 `dbt_test` schema
- 如果遇到连接问题，检查网络配置和容器状态
