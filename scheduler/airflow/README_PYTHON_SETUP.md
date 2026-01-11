# Airflow Python 配置脚本使用说明

## 概述

`setup_airflow.py` 是一个 Python 脚本，用于通过 Airflow REST API 自动配置和管理 Airflow。

## 功能

✅ **自动配置 MySQL 连接**
- 创建或更新 `mysql_default` 连接
- 无需手动在 Web UI 中操作

✅ **检查 DAG 状态**
- 列出所有已加载的 DAG
- 按层（ODS/DWD/DWS/ADS）统计 DAG 数量
- 显示 DAG 启用/暂停状态
- 检查 DAG 导入错误

## 前置要求

1. **Python 3.7+**
2. **安装 requests 库**:
   ```bash
   pip install requests
   ```
   或使用项目 requirements.txt:
   ```bash
   pip install -r requirements.txt
   ```

3. **Airflow 服务正在运行**:
   ```bash
   cd scheduler/airflow
   docker-compose up -d
   ```

## 使用方法

### 基本使用

```bash
cd scheduler/airflow
python setup_airflow.py
```

### 输出示例

```
============================================================
Airflow 配置脚本
============================================================
✓ Airflow 服务连接正常

============================================================
配置 MySQL 连接
============================================================
✓ 连接 'mysql_default' 已创建

============================================================
检查 DAG 状态
============================================================

找到 30 个 DAG:

按层统计:
  ODS 层: 10 个
  DWD 层: 7 个
  DWS 层: 7 个
  ADS 层: 6 个

状态统计:
  已暂停: 30 个
  已启用: 0 个

✓ 未发现 DAG 导入错误

============================================================
配置完成！
============================================================
```

## 配置说明

### 修改连接配置

编辑 `setup_airflow.py` 中的 `MYSQL_CONNECTION` 字典：

```python
MYSQL_CONNECTION = {
    "connection_id": "mysql_default",
    "conn_type": "mysql",
    "host": "mysql-db",  # 容器网络内使用容器名
    "schema": "sqlExpert",
    "login": "sqluser",
    "password": "sqlpass123",
    "port": 3306,
    "description": "MySQL connection for datawarehouse ETL"
}
```

### 修改 Airflow 连接信息

如果 Airflow 运行在不同的地址或使用不同的凭据：

```python
AIRFLOW_BASE_URL = "http://100.126.111.70:8080"  # 使用 Tailscale IP
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"
```

## 扩展功能

脚本中的 `AirflowClient` 类提供了更多功能，可以扩展使用：

### 启用/禁用 DAG

```python
client = AirflowClient(AIRFLOW_API_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
client.update_dag("ods_01_order_master_etl", is_paused=False)  # 启用
client.update_dag("ods_01_order_master_etl", is_paused=True)   # 禁用
```

### 获取 DAG 详情

```python
dag_info = client.get_dag("ods_01_order_master_etl")
print(dag_info)
```

### 批量启用 DAG

可以扩展脚本添加批量操作功能：

```python
# 启用所有 ODS 层 DAG
dags = client.list_dags()
for dag in dags:
    if dag["dag_id"].startswith("ods_"):
        client.update_dag(dag["dag_id"], is_paused=False)
```

## 故障排除

### 连接失败

如果提示 "无法连接到 Airflow 服务"：
1. 检查 Airflow 是否运行: `docker ps | grep airflow`
2. 检查端口是否正确: `http://100.126.111.70:8080` (使用 Tailscale IP)
3. 等待服务完全启动（首次启动需要 30-60 秒）

### 认证失败

如果提示认证错误：
1. 检查用户名和密码是否正确
2. 确认 Airflow 环境变量配置

### DAG 未加载

如果脚本显示 "未找到任何 DAG"：
1. 检查 DAG 文件是否正确挂载
2. 查看 Airflow 日志: `docker logs airflow-standalone`
3. 确认 DAG 文件语法正确

## API 参考

脚本使用 Airflow REST API v1，主要端点：

- `GET /api/v1/health` - 健康检查
- `GET /api/v1/connections/{connection_id}` - 获取连接
- `POST /api/v1/connections` - 创建连接
- `PATCH /api/v1/connections/{connection_id}` - 更新连接
- `GET /api/v1/dags` - 列出所有 DAG
- `GET /api/v1/dags/{dag_id}` - 获取 DAG 详情
- `PATCH /api/v1/dags/{dag_id}` - 更新 DAG

更多 API 文档: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
