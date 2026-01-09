# 创建DolphinScheduler调度任务

## 当前状态

- ✅ DolphinScheduler容器正在运行
- ✅ 创建脚本已存在: `datawarehouse/schedulers/create_dolphinscheduler_tasks.py`
- ✅ REST API客户端已存在: `scripts/dolphinscheduler_rest_api.py`

## 执行步骤

### 方法1: 使用Python脚本（推荐）

```bash
# 确保Python已安装
python --version

# 安装依赖
pip install requests

# 执行创建脚本
python datawarehouse/schedulers/create_dolphinscheduler_tasks.py
```

### 方法2: 通过Docker容器执行

```bash
# 进入容器执行
docker exec -it dolphinscheduler bash
# 然后在容器内执行Python脚本
```

### 方法3: 使用REST API直接创建

访问DolphinScheduler Web界面：
- URL: http://localhost:12345
- 用户名: admin
- 密码: dolphinscheduler123

## 配置说明

脚本中的配置：
- DolphinScheduler地址: http://localhost:12345
- 用户名: admin
- 密码: dolphinscheduler123
- 项目名称: 制造业数仓

## 30个调度任务

脚本会创建30个工作流：
- ODS层: 10个 (ods_01 到 ods_10)
- DWD层: 7个 (dwd_01 到 dwd_07)
- DWS层: 7个 (dws_01 到 dws_07)
- ADS层: 6个 (ads_01 到 ads_06)

## 注意事项

1. 确保DolphinScheduler容器正在运行
2. 确保MySQL数据源已在DolphinScheduler中配置
3. 如果数据源未配置，需要先在Web界面手动配置MySQL数据源
