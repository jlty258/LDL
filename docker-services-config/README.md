# Docker 服务配置说明

本目录包含所有 Docker 服务的统一配置文件，方便在其他项目中使用这些服务。

## 服务列表

### 1. MySQL 数据库
- **容器名称**: `mysql-db`
- **端口**: `3306`
- **数据库名**: `sqlExpert`
- **用户名**: `sqluser`
- **密码**: `sqlpass123`
- **Root 密码**: `root123`
- **连接字符串**: `mysql://sqluser:sqlpass123@localhost:3306/sqlExpert`
- **JDBC URL**: `jdbc:mysql://localhost:3306/sqlExpert?useSSL=false&serverTimezone=Asia/Shanghai`

### 2. PostgreSQL 数据库
- **容器名称**: `postgres-db`
- **端口**: `5432`
- **数据库名**: `sqlExpert`
- **用户名**: `postgres`
- **密码**: `postgres123`
- **连接字符串**: `postgresql://postgres:postgres123@localhost:5432/sqlExpert`
- **JDBC URL**: `jdbc:postgresql://localhost:5432/sqlExpert`

### 3. Apache Airflow
- **容器名称**: `airflow-standalone`
- **Web UI**: http://localhost:8080
- **用户名**: `airflow`
- **密码**: `airflow`
- **API URL**: http://localhost:8080/api/v1
- **版本**: 2.9.0

### 4. Apache DolphinScheduler
- **容器名称**: `dolphinscheduler`
- **Web UI**: http://localhost:12345
- **用户名**: `admin`
- **密码**: `dolphinscheduler123`
- **API URL**: http://localhost:12345/dolphinscheduler
- **版本**: latest

## 快速开始

### 启动所有服务

```bash
# 使用 docker-compose 启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看服务日志
docker-compose logs -f
```

### 停止所有服务

```bash
# 停止所有服务（保留数据）
docker-compose stop

# 停止并删除容器（保留数据卷）
docker-compose down

# 停止并删除容器和数据卷（谨慎使用）
docker-compose down -v
```

### 单独管理服务

```bash
# 启动单个服务
docker-compose up -d mysql
docker-compose up -d postgres
docker-compose up -d airflow-standalone
docker-compose up -d dolphinscheduler

# 停止单个服务
docker-compose stop mysql
docker-compose stop postgres

# 查看单个服务日志
docker-compose logs -f mysql
docker-compose logs -f airflow-standalone
```

## 在其他项目中使用

### Python 项目

```python
# MySQL 连接示例
import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    port=3306,
    user='sqluser',
    password='sqlpass123',
    database='sqlExpert'
)

# PostgreSQL 连接示例
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='postgres',
    password='postgres123',
    database='sqlExpert'
)

# Airflow API 调用示例
import requests
from requests.auth import HTTPBasicAuth

response = requests.get(
    'http://localhost:8080/api/v1/dags',
    auth=HTTPBasicAuth('airflow', 'airflow')
)
```

### Java 项目

```java
// MySQL JDBC 连接
String url = "jdbc:mysql://localhost:3306/sqlExpert?useSSL=false&serverTimezone=Asia/Shanghai";
String user = "sqluser";
String password = "sqlpass123";

// PostgreSQL JDBC 连接
String url = "jdbc:postgresql://localhost:5432/sqlExpert";
String user = "postgres";
String password = "postgres123";
```

### Node.js 项目

```javascript
// MySQL 连接
const mysql = require('mysql2');
const connection = mysql.createConnection({
  host: 'localhost',
  port: 3306,
  user: 'sqluser',
  password: 'sqlpass123',
  database: 'sqlExpert'
});

// PostgreSQL 连接
const { Client } = require('pg');
const client = new Client({
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: 'postgres123',
  database: 'sqlExpert'
});
```

## 配置文件说明

- **docker-compose.yml**: 完整的 Docker Compose 配置文件，包含所有服务定义
- **services-config.json**: JSON 格式的服务配置信息，包含所有连接参数
- **.env.example**: 环境变量示例文件，可以复制为 `.env` 并在项目中使用

## 数据持久化

所有数据都存储在 Docker 卷中，即使容器被删除，数据也会保留：

- `mysql-data`: MySQL 数据存储
- `postgres-data`: PostgreSQL 数据存储
- `airflow-dags`: Airflow DAG 文件
- `airflow-logs`: Airflow 日志文件
- `airflow-plugins`: Airflow 插件
- `airflow-config`: Airflow 配置文件
- `dolphinscheduler-data`: DolphinScheduler 数据存储

## 网络配置

所有服务都在同一个 Docker 网络 `services-network` 中，可以通过容器名称相互访问：

- 从其他容器访问 MySQL: `mysql:3306`
- 从其他容器访问 PostgreSQL: `postgres:5432`

## 健康检查

MySQL 和 PostgreSQL 服务都配置了健康检查，可以通过以下命令查看：

```bash
docker-compose ps
```

## 注意事项

1. 确保端口 3306、5432、8080、12345 没有被其他服务占用
2. 首次启动 Airflow 需要一些时间来初始化数据库
3. DolphinScheduler 的默认密码是 `dolphinscheduler123`，首次登录后建议修改
4. 所有密码都是示例密码，生产环境请务必修改

## 故障排查

### 查看服务日志
```bash
docker-compose logs [service-name]
```

### 检查服务状态
```bash
docker-compose ps
docker ps
```

### 重启服务
```bash
docker-compose restart [service-name]
```

### 进入容器
```bash
docker exec -it mysql-db bash
docker exec -it postgres-db bash
docker exec -it airflow-standalone bash
```
