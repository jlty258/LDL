# Airflow DAG 配置说明

## DAG 文件位置

datawarehouse 中的 DAG 文件已经直接挂载到 Airflow 容器中：
- **源目录**: `datawarehouse/scheduler/airflow/dags/`
- **容器内路径**: `/opt/airflow/dags/`

## 已修复的问题

1. ✅ **schedule_interval → schedule**: 所有 DAG 文件已更新为使用 `schedule` 参数（Airflow 2.4+ 推荐）
2. ✅ **DAG 目录挂载**: 已配置直接挂载 datawarehouse 的 DAG 目录
3. ✅ **SQL 文件挂载**: SQL 文件已挂载到 `/opt/airflow/dags/sql/`

## 需要配置的连接

### MySQL 连接配置

DAG 文件使用 `mysql_default` 连接 ID，需要在 Airflow Web UI 中配置：

1. 访问 Airflow Web UI: http://localhost:8080
2. 登录（用户名/密码: airflow/airflow）
3. 进入 **Admin** → **Connections**
4. 添加新连接或编辑 `mysql_default`：
   - **Connection Id**: `mysql_default`
   - **Connection Type**: `MySQL`
   - **Host**: `mysql-db` (容器网络内) 或 `localhost` (从宿主机)
   - **Schema**: `sqlExpert`
   - **Login**: `sqluser`
   - **Password**: `sqlpass123`
   - **Port**: `3306`

### 使用 Python 脚本配置连接（推荐）

使用 Python 脚本自动配置连接并检查 DAG 状态：

```bash
# 安装依赖（如果还没有）
pip install requests

# 运行配置脚本
cd scheduler/airflow
python setup_airflow.py
```

脚本功能：
- ✅ 自动配置 MySQL 连接
- ✅ 检查 DAG 加载状态
- ✅ 统计各层 DAG 数量
- ✅ 检查 DAG 导入错误

### 使用 Airflow CLI 配置连接

```bash
docker exec -it airflow-standalone airflow connections add mysql_default \
  --conn-type mysql \
  --conn-host mysql-db \
  --conn-schema sqlExpert \
  --conn-login sqluser \
  --conn-password sqlpass123 \
  --conn-port 3306
```

## 安装 MySQL Provider（如果需要）

Airflow 2.9.0 镜像可能已包含 MySQL provider，如果 DAG 报错找不到 `airflow.providers.mysql`，需要安装：

```bash
docker exec -it airflow-standalone pip install apache-airflow-providers-mysql
docker-compose restart
```

## 验证 DAG 加载

1. 重启 Airflow 服务：
   ```bash
   cd scheduler/airflow
   docker-compose restart
   ```

2. 等待 30-60 秒后访问 Web UI，应该能看到 30 个 DAG：
   - ODS 层: 10 个 DAG (ods_01 ~ ods_10)
   - DWD 层: 7 个 DAG (dwd_01 ~ dwd_07)
   - DWS 层: 7 个 DAG (dws_01 ~ dws_07)
   - ADS 层: 6 个 DAG (ads_01 ~ ads_06)

3. 检查 DAG 是否有错误：
   - 在 Web UI 中查看 DAG 列表
   - 红色标记表示有错误，点击查看详情

## DAG 调度时间

- **ODS 层**: 每天 02:00 (0 2 * * *)
- **DWD 层**: 每天 03:00 (0 3 * * *)
- **DWS 层**: 每天 04:00 (0 4 * * *)
- **ADS 层**: 每天 05:00 (0 5 * * *)

## 注意事项

1. 所有 DAG 在创建时默认是**暂停状态**（`DAGS_ARE_PAUSED_AT_CREATION: 'true'`）
2. 需要在 Web UI 中手动启用需要运行的 DAG
3. SQL 文件路径在容器内为 `/opt/airflow/dags/sql/`
4. 确保 MySQL 服务（mysql-db）正在运行且在同一网络（ldl-net）中
