# Airflow 2.9 Standalone 项目

## 项目结构

```
airflow/
├── docker-compose.yml    # Docker Compose 配置文件
├── dags/                 # DAG 文件目录
├── logs/                 # 日志文件目录
├── plugins/              # 插件目录
└── config/               # 配置文件目录
```

## 快速开始

### 1. 启动 Airflow

```bash
cd airflow
docker-compose up -d
```

standalone 模式会自动初始化数据库和创建默认用户。

### 2. 访问 Web UI

- **地址**: http://100.126.111.70:8080 (使用 Tailscale IP)
- **用户名**: airflow
- **密码**: airflow

### 3. 常用命令

```bash
# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f

# 查看特定服务日志
docker-compose logs -f airflow-standalone

# 停止服务
docker-compose down

# 停止并删除数据卷
docker-compose down -v

# 重启服务
docker-compose restart
```

## 配置说明

### Standalone 模式

Airflow Standalone 模式在一个容器中运行所有组件：
- Web Server (端口 8080)
- Scheduler
- Worker (SequentialExecutor - 与 SQLite 兼容)

### 数据库

Airflow 使用 **SQLite** 作为默认数据库（内置，无需额外配置）。

> **注意**: SQLite 适合开发和测试环境。生产环境建议使用 PostgreSQL 或 MySQL。

### 环境变量

可以在 `docker-compose.yml` 或通过环境变量修改以下配置：

- `_AIRFLOW_WWW_USER_USERNAME`: Web UI 用户名（默认: airflow）
- `_AIRFLOW_WWW_USER_PASSWORD`: Web UI 密码（默认: airflow）

## 添加 DAG

将你的 DAG Python 文件放在 `dags/` 目录下，Airflow 会自动检测并加载。

## 注意事项

1. 首次启动需要等待初始化完成（约 60 秒），standalone 模式会自动：
   - 初始化 SQLite 数据库
   - 创建默认用户（airflow/airflow）
2. 确保端口 8080 未被占用
3. 使用 SQLite 数据库，数据存储在容器内的 `/opt/airflow/airflow.db`
4. Standalone 模式适合开发和测试环境，生产环境建议使用 PostgreSQL/MySQL + 分布式部署
