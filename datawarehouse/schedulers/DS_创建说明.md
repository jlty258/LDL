# DolphinScheduler 项目和调度创建说明

## 当前状态

✅ **项目已创建成功**: "制造业数仓" (代码: 162489188384192)
⚠️ **工作流创建**: 由于API限制，需要通过Web界面手动创建

## 访问信息

- **Web UI**: http://localhost:12345
- **用户名**: `admin`
- **密码**: `dolphinscheduler123`
- **项目名称**: 制造业数仓

## 手动创建工作流步骤

### 1. 登录DolphinScheduler

访问 http://localhost:12345，使用以下凭证登录：
- 用户名: `admin`
- 密码: `dolphinscheduler123`

### 2. 进入项目

1. 点击左侧菜单 **"项目管理"**
2. 选择 **"制造业数仓"** 项目
3. 点击进入项目

### 3. 配置数据源（重要）

在创建工作流之前，需要先配置MySQL数据源：

1. 点击左侧菜单 **"数据源中心"**
2. 点击 **"创建数据源"**
3. 填写以下信息：
   - **数据源类型**: MySQL
   - **数据源名称**: MySQL-sqlExpert
   - **主机**: `mysql-db` (如果在Docker网络中) 或 `localhost`
   - **端口**: `3306`
   - **数据库名**: `sqlExpert`
   - **用户名**: `sqluser`
   - **密码**: `sqlpass123`
   - **其他参数**: 
     ```
     useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8mb4
     ```
4. 点击 **"测试连接"** 确认连接成功
5. 点击 **"提交"** 保存

### 4. 创建工作流

需要创建以下15个工作流（UTF-8编码）：

#### ODS层工作流（5个，每天凌晨2点执行）

1. **ods_01_order_etl** - ODS层-订单数据ETL
   - Cron: `0 0 2 * * ?`
   - SQL任务: 执行订单相关表的ETL

2. **ods_02_production_etl** - ODS层-生产数据ETL
   - Cron: `0 0 2 * * ?`
   - SQL任务: 执行生产相关表的ETL

3. **ods_03_inventory_etl** - ODS层-库存数据ETL
   - Cron: `0 0 2 * * ?`
   - SQL任务: 执行库存相关表的ETL

4. **ods_04_purchase_etl** - ODS层-采购数据ETL
   - Cron: `0 0 2 * * ?`
   - SQL任务: 执行采购相关表的ETL

5. **ods_05_quality_etl** - ODS层-质量数据ETL
   - Cron: `0 0 2 * * ?`
   - SQL任务: 执行质量相关表的ETL

#### DWD层工作流（4个，每天凌晨3点执行）

6. **dwd_01_order_fact_etl** - DWD层-订单事实表ETL
   - Cron: `0 0 3 * * ?`
   - SQL任务: 执行 `datawarehouse/sql/07_complex_etl.sql` 中的订单事实表ETL

7. **dwd_02_production_fact_etl** - DWD层-生产事实表ETL
   - Cron: `0 0 3 * * ?`
   - SQL任务: 执行生产事实表ETL

8. **dwd_03_inventory_fact_etl** - DWD层-库存事实表ETL
   - Cron: `0 0 3 * * ?`
   - SQL任务: 执行库存事实表ETL

9. **dwd_04_purchase_fact_etl** - DWD层-采购事实表ETL
   - Cron: `0 0 3 * * ?`
   - SQL任务: 执行采购事实表ETL

#### DWS层工作流（3个，每天凌晨4点执行）

10. **dws_01_order_daily_etl** - DWS层-订单日汇总ETL
    - Cron: `0 0 4 * * ?`
    - SQL任务: 执行订单日汇总计算

11. **dws_02_production_daily_etl** - DWS层-生产日汇总ETL
    - Cron: `0 0 4 * * ?`
    - SQL任务: 执行生产日汇总计算

12. **dws_03_inventory_daily_etl** - DWS层-库存日汇总ETL
    - Cron: `0 0 4 * * ?`
    - SQL任务: 执行库存日汇总计算

#### ADS层工作流（3个，每天凌晨5点执行）

13. **ads_01_sales_analysis_etl** - ADS层-销售分析报表ETL
    - Cron: `0 0 5 * * ?`
    - SQL任务: 执行销售分析报表生成

14. **ads_02_production_analysis_etl** - ADS层-生产分析报表ETL
    - Cron: `0 0 5 * * ?`
    - SQL任务: 执行生产分析报表生成

15. **ads_03_business_overview_etl** - ADS层-综合经营分析报表ETL
    - Cron: `0 0 5 * * ?`
    - SQL任务: 执行综合经营分析报表生成

### 5. 创建工作流详细步骤

对于每个工作流：

1. 点击 **"工作流定义"** 菜单
2. 点击 **"创建工作流"** 按钮
3. 填写工作流信息：
   - **工作流名称**: 如 `ods_01_order_etl`
   - **描述**: 如 `ODS层-订单数据ETL`
4. 在画布中添加任务：
   - 拖拽 **"SQL"** 任务到画布
   - 双击任务节点，配置SQL任务：
     - **任务名称**: 如 `订单ETL任务`
     - **数据源**: 选择 `MySQL-sqlExpert`
     - **SQL类型**: 非查询
     - **SQL语句**: 从 `datawarehouse/sql/07_complex_etl.sql` 复制对应的SQL
5. 点击 **"保存"** 保存工作流
6. 点击 **"上线"** 使工作流生效

### 6. 创建调度

1. 在工作流列表中，找到刚创建的工作流
2. 点击 **"定时管理"** 按钮
3. 点击 **"创建定时"**
4. 配置调度信息：
   - **Cron表达式**: 根据层级设置（见上方）
   - **时区**: `Asia/Shanghai`
   - **生效失效时间**: 根据需要设置
5. 点击 **"提交"** 保存调度

## SQL文件位置

所有SQL文件位于 `datawarehouse/sql/` 目录：

- `02_ods_tables.sql` - ODS层表结构
- `03_dwd_tables.sql` - DWD层表结构
- `04_dws_tables.sql` - DWS层表结构
- `05_ads_tables.sql` - ADS层表结构
- `07_complex_etl.sql` - 完整ETL SQL脚本

## 注意事项

1. **UTF-8编码**: 确保所有SQL和配置使用UTF-8编码
2. **数据源配置**: 必须先配置MySQL数据源才能创建SQL任务
3. **SQL内容**: 可以从 `07_complex_etl.sql` 文件中复制对应的SQL语句
4. **调度时间**: 
   - ODS层: 每天凌晨2点
   - DWD层: 每天凌晨3点（依赖ODS层完成）
   - DWS层: 每天凌晨4点（依赖DWD层完成）
   - ADS层: 每天凌晨5点（依赖DWS层完成）

## 快速验证

创建完成后，可以：

1. 在工作流列表中点击 **"运行"** 手动触发执行
2. 查看 **"工作流实例"** 查看执行历史
3. 查看 **"任务实例"** 查看任务执行详情

## 项目代码

项目代码: `162489188384192`

可以在API调用或脚本中使用此代码。
