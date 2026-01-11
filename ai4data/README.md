# Jupyter Notebook 数据分析环境

用于数据查看、SQL 执行和数据分析的 Jupyter Notebook 环境。

## 架构优化说明

本配置采用**构建时安装依赖**的方式，相比运行时安装有以下优势：

✅ **性能优化**：
- 依赖在镜像构建时安装，容器启动速度更快
- 避免每次启动容器都重新安装依赖
- 减少容器启动时间从几分钟降至几秒

✅ **持续集成友好**：
- 依赖变更通过 Docker 镜像版本管理
- 支持 CI/CD 流程中的镜像构建和缓存
- 环境一致性更好，避免运行时依赖安装失败

✅ **资源优化**：
- 使用 Docker 层缓存，只重新构建变更的部分
- 减少网络请求和磁盘 I/O
- 镜像可复用，多环境部署更高效

## 快速开始

### 构建并启动服务

```bash
cd ai4data
# 首次构建镜像（会安装所有依赖）
docker-compose build

# 启动服务
docker-compose up -d
```

**注意**：
- 首次构建镜像时会安装 `requirements.txt` 中的所有依赖包，可能需要几分钟时间
- 之后启动容器会非常快，因为依赖已经打包在镜像中
- 如果更新了 `requirements.txt`，需要重新构建镜像：`docker-compose build --no-cache`

### 访问 Jupyter Notebook

- **Web UI**: http://localhost:8890
- **Token**: `ldl-jupyter-2024`

在浏览器中打开上述地址，使用 token 登录即可。

### 查看日志

```bash
docker-compose logs -f jupyter
```

### 常用命令

```bash
# 重新构建镜像（更新依赖后）
docker-compose build

# 强制重新构建（不使用缓存）
docker-compose build --no-cache

# 构建并启动
docker-compose up -d --build

# 停止服务
docker-compose down

# 查看镜像
docker images | grep jupyter-ldl

# 进入容器
docker exec -it jupyter-notebook bash
```

### 从旧版本迁移

如果你之前使用的是运行时安装依赖的版本，需要迁移到新版本：

```bash
# 1. 停止旧容器
docker-compose down

# 2. 删除旧镜像（可选）
docker rmi jupyter/scipy-notebook:latest

# 3. 构建新镜像
docker-compose build

# 4. 启动新容器
docker-compose up -d
```

## 数据库连接

Jupyter Notebook 已配置连接到项目中的数据库：

### MySQL 连接示例

```python
import mysql.connector
import os

conn = mysql.connector.connect(
    host=os.getenv('MYSQL_HOST', 'mysql-db'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    database=os.getenv('MYSQL_DATABASE', 'sqlExpert'),
    user=os.getenv('MYSQL_USERNAME', 'sqluser'),
    password=os.getenv('MYSQL_PASSWORD', 'sqlpass123')
)

cursor = conn.cursor()
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print(tables)
```

### PostgreSQL 连接示例

```python
import psycopg2
import os

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'postgres-db'),
    port=int(os.getenv('POSTGRES_PORT', 5432)),
    database=os.getenv('POSTGRES_DATABASE', 'sqlExpert'),
    user=os.getenv('POSTGRES_USERNAME', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', 'postgres123')
)

cursor = conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = cursor.fetchall()
print(tables)
```

### 使用 SQLAlchemy + Pandas

```python
from sqlalchemy import create_engine
import pandas as pd
import os

# MySQL 连接
mysql_engine = create_engine(
    f"mysql+pymysql://{os.getenv('MYSQL_USERNAME')}:{os.getenv('MYSQL_PASSWORD')}"
    f"@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
)

# 执行 SQL 查询
df = pd.read_sql("SELECT * FROM your_table LIMIT 100", mysql_engine)
df.head()
```

### 使用 ipython-sql 扩展

```python
%load_ext sql

# 连接 MySQL
%sql mysql+pymysql://sqluser:sqlpass123@mysql-db:3306/sqlExpert

# 执行 SQL
%sql SELECT * FROM your_table LIMIT 10
```

## 目录结构

```
ai4data/
├── docker-compose.yml    # Docker Compose 配置
├── requirements.txt      # Python 依赖包
├── README.md            # 本文件
└── notebooks/           # Notebook 文件目录（自动创建）
```

## 安装额外的 Python 包

如果需要安装额外的 Python 包，推荐方式：

1. **更新 requirements.txt 并重新构建镜像（推荐）**：
```bash
# 编辑 requirements.txt，添加新包
# 然后重新构建镜像
docker-compose build --no-cache
docker-compose up -d
```

2. **在 Notebook 中临时安装**（重启容器后会丢失）：
```python
!pip install package-name
```

3. **在容器中安装**（重启容器后会丢失）：
```bash
docker exec -it jupyter-notebook pip install package-name
```

**最佳实践**：所有依赖都应该在 `requirements.txt` 中声明，并通过构建镜像的方式安装，这样可以确保环境一致性。

## 停止服务

```bash
docker-compose down
```

## 注意事项

- Notebook 文件保存在 `./notebooks` 目录中
- 可以通过挂载的卷访问 `data_volume` 和 `datawarehouse` 目录
- 数据库连接使用容器网络中的服务名（如 `mysql-db`、`postgres-db`）
- 如需从宿主机访问，请使用 Tailscale IP: `100.126.111.70`
