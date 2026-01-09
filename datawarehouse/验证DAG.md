# 验证Airflow DAG加载

## 当前状态

Airflow容器已重启，正在扫描DAG文件。通常需要1-2分钟才能完成扫描。

## 验证步骤

### 1. 检查DAG文件数量
```bash
docker exec airflow-standalone ls /opt/airflow/dags/*etl.py | wc -l
```
应该显示30个文件

### 2. 等待Airflow扫描（约1-2分钟）
Airflow默认每30秒扫描一次DAG目录。

### 3. 检查DAG列表
```bash
docker exec airflow-standalone airflow dags list | grep etl
```

### 4. 访问Web界面
- URL: http://localhost:8080
- 用户名: airflow
- 密码: airflow

在DAG列表页面应该能看到30个ETL相关的DAG。

## 如果DAG仍未出现

1. **检查DAG语法错误**:
   ```bash
   docker exec airflow-standalone airflow dags list-import-errors
   ```

2. **查看Airflow日志**:
   ```bash
   docker logs airflow-standalone --tail 100
   ```

3. **手动触发扫描**:
   ```bash
   docker restart airflow-standalone
   ```

4. **检查文件权限**:
   ```bash
   docker exec airflow-standalone ls -la /opt/airflow/dags/
   ```

## 已创建的30个DAG

- ODS层: ods_01 到 ods_10 (10个)
- DWD层: dwd_01 到 dwd_07 (7个)
- DWS层: dws_01 到 dws_07 (7个)
- ADS层: ads_01 到 ads_06 (6个)

所有DAG文件已创建在 `airflow/dags/` 目录，Airflow会自动扫描并加载。
