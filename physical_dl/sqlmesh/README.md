# SQLMesh 镜像验证

本目录包含用于验证 SQLMesh Docker 镜像功能的测试项目。

## 目录结构

```
sqlmesh/
├── docker-compose.yml          # SQLMesh 容器配置
├── sqlmesh_project/            # SQLMesh 项目目录
│   ├── config.yaml             # SQLMesh 配置文件
│   └── models/                  # SQLMesh 模型
│       ├── example_model.sql    # 示例模型
│       └── test_model.sql       # 测试模型
├── test_sqlmesh.sh            # Linux/Mac 测试脚本
├── test_sqlmesh.ps1            # Windows PowerShell 测试脚本
└── README.md                   # 本文件
```

## 快速开始

### 1. 确保 PostgreSQL 数据库运行

SQLMesh 需要连接到 PostgreSQL 数据库。确保 `postgres-db` 容器正在运行：

```bash
# 在 physical_dl 目录下启动数据库
cd ../physical_dl
docker-compose up -d postgres
```

### 2. 启动 SQLMesh 容器

```bash
cd sqlmesh
docker-compose up -d
```

### 3. 运行测试

**Windows (PowerShell):**
```powershell
.\test_sqlmesh.ps1
```

**Linux/Mac:**
```bash
chmod +x test_sqlmesh.sh
./test_sqlmesh.sh
```

## 手动测试命令

你也可以手动执行 SQLMesh 命令来验证功能：

```bash
# 检查 SQLMesh 版本
docker exec sqlmesh-test sqlmesh --version

# 查看项目信息
docker exec sqlmesh-test sqlmesh info

# 创建计划
docker exec sqlmesh-test sqlmesh plan --no-prompts

# 应用计划
docker exec sqlmesh-test sqlmesh apply --no-prompts

# 查看表信息
docker exec sqlmesh-test sqlmesh table info sqlmesh_test.test_model

# 查看帮助
docker exec sqlmesh-test sqlmesh --help
```

## 测试内容

测试脚本会验证以下功能：

1. ✅ 容器运行状态
2. ✅ SQLMesh 版本信息
3. ✅ 项目配置信息
4. ✅ 计划创建
5. ✅ 计划应用
6. ✅ 模型验证

## 数据库连接

SQLMesh 连接到 PostgreSQL 数据库：
- **主机**: postgres-db (容器名称)
- **端口**: 5432
- **数据库**: sqlExpert
- **用户**: postgres
- **密码**: postgres123
- **Schema**: sqlmesh_test

## 注意事项

- 确保 PostgreSQL 容器已启动并运行
- 首次运行前，SQLMesh 会自动创建 schema
- 如果遇到连接问题，检查网络配置和容器状态
- SQLMesh 使用 `--no-prompts` 参数以避免交互式提示

## SQLMesh 特性

SQLMesh 是一个数据转换框架，主要特性包括：

- **版本控制**: 自动跟踪模型变更
- **增量处理**: 支持增量数据加载
- **测试**: 内置数据质量测试
- **计划**: 创建和执行变更计划
- **回滚**: 支持计划回滚
