# DolphinScheduler调度任务创建脚本
# 创建30个制造业数仓ETL调度任务

# 设置UTF-8编码
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

$baseUrl = "http://localhost:12345"
$username = "admin"
$password = "dolphinscheduler123"

# 登录获取Session ID
Write-Host "正在登录DolphinScheduler..." -ForegroundColor Yellow
$loginUrl = "$baseUrl/dolphinscheduler/login"
$loginBody = "userName=$username&userPassword=$password"

try {
    $loginResponse = Invoke-RestMethod -Uri $loginUrl -Method Post -Body $loginBody -ContentType "application/x-www-form-urlencoded"
    
        if ($loginResponse.code -eq 0) {
            # 新版本API使用token，旧版本使用sessionId
            $token = $loginResponse.data.token
            $sessionId = $loginResponse.data.sessionId
            $authToken = if ($token) { $token } else { $sessionId }
            
            Write-Host "✓ 登录成功" -ForegroundColor Green
            if ($token) {
                Write-Host "  使用Token认证" -ForegroundColor Cyan
            } else {
                Write-Host "  使用Session ID认证" -ForegroundColor Cyan
            }
            
            # 获取或创建项目
            Write-Host "`n正在获取项目列表..." -ForegroundColor Yellow
            $projectsUrl = "$baseUrl/dolphinscheduler/projects/list"
            $headers = @{}
            if ($token) {
                $headers["token"] = $token
            } else {
                $headers["sessionId"] = $sessionId
            }
        
        # 使用UTF-8编码获取项目列表
        $projectsResponseRaw = Invoke-WebRequest -Uri $projectsUrl -Method Get -Headers $headers -UseBasicParsing
        $projectsResponse = $projectsResponseRaw.Content | ConvertFrom-Json
        
        $projectCode = $null
        $projectName = "制造业数仓"
        
        # 直接使用第一个项目（项目已存在）
        if ($projectsResponse.data.Count -gt 0) {
            $projectCode = $projectsResponse.data[0].code
            $actualProjectName = $projectsResponse.data[0].name
            Write-Host "✓ 使用项目: $actualProjectName (代码: $projectCode)" -ForegroundColor Green
        } elseif ($projectsResponse.data.totalList -and $projectsResponse.data.totalList.Count -gt 0) {
            $projectCode = $projectsResponse.data.totalList[0].code
            $actualProjectName = $projectsResponse.data.totalList[0].name
            Write-Host "✓ 使用项目: $actualProjectName (代码: $projectCode)" -ForegroundColor Green
        } else {
            Write-Host "❌ 未找到项目，请先在Web界面创建项目" -ForegroundColor Red
            Write-Host "   访问: http://localhost:12345" -ForegroundColor Yellow
            exit 1
        }
        
        if (-not $projectCode) {
            Write-Host "❌ 无法获取项目代码" -ForegroundColor Red
            exit 1
        }
        
        # 定义30个调度任务
        $workflows = @(
            # ODS层 (1-10)
            @{name="ods_01_order_master_etl"; desc="ODS层-订单主表ETL"; sql="SELECT COUNT(*) FROM ods_order_master"},
            @{name="ods_02_order_detail_etl"; desc="ODS层-订单明细表ETL"; sql="SELECT COUNT(*) FROM ods_order_detail"},
            @{name="ods_03_customer_etl"; desc="ODS层-客户主表ETL"; sql="SELECT COUNT(*) FROM ods_customer_master"},
            @{name="ods_04_product_etl"; desc="ODS层-产品主表ETL"; sql="SELECT COUNT(*) FROM ods_product_master"},
            @{name="ods_05_production_plan_etl"; desc="ODS层-生产计划表ETL"; sql="SELECT COUNT(*) FROM ods_production_plan"},
            @{name="ods_06_production_order_etl"; desc="ODS层-生产工单表ETL"; sql="SELECT COUNT(*) FROM ods_production_order"},
            @{name="ods_07_bom_etl"; desc="ODS层-物料清单表ETL"; sql="SELECT COUNT(*) FROM ods_bom"},
            @{name="ods_08_material_etl"; desc="ODS层-物料主表ETL"; sql="SELECT COUNT(*) FROM ods_material_master"},
            @{name="ods_09_inventory_etl"; desc="ODS层-库存表ETL"; sql="SELECT COUNT(*) FROM ods_inventory"},
            @{name="ods_10_purchase_etl"; desc="ODS层-采购订单表ETL"; sql="SELECT COUNT(*) FROM ods_purchase_order"},
            
            # DWD层 (11-17)
            @{name="dwd_01_order_fact_etl"; desc="DWD层-订单事实表ETL"; sql="SELECT COUNT(*) FROM dwd_order_fact"},
            @{name="dwd_02_production_fact_etl"; desc="DWD层-生产事实表ETL"; sql="SELECT COUNT(*) FROM dwd_production_fact"},
            @{name="dwd_03_inventory_fact_etl"; desc="DWD层-库存事实表ETL"; sql="SELECT COUNT(*) FROM dwd_inventory_fact"},
            @{name="dwd_04_purchase_fact_etl"; desc="DWD层-采购事实表ETL"; sql="SELECT COUNT(*) FROM dwd_purchase_fact"},
            @{name="dwd_05_quality_fact_etl"; desc="DWD层-质量事实表ETL"; sql="SELECT COUNT(*) FROM dwd_quality_fact"},
            @{name="dwd_06_equipment_runtime_etl"; desc="DWD层-设备运行事实表ETL"; sql="SELECT COUNT(*) FROM dwd_equipment_runtime_fact"},
            @{name="dwd_07_cost_fact_etl"; desc="DWD层-成本事实表ETL"; sql="SELECT COUNT(*) FROM dwd_cost_fact"},
            
            # DWS层 (18-24)
            @{name="dws_01_order_daily_etl"; desc="DWS层-订单日汇总ETL"; sql="SELECT COUNT(*) FROM dws_order_daily"},
            @{name="dws_02_production_daily_etl"; desc="DWS层-生产日汇总ETL"; sql="SELECT COUNT(*) FROM dws_production_daily"},
            @{name="dws_03_inventory_daily_etl"; desc="DWS层-库存日汇总ETL"; sql="SELECT COUNT(*) FROM dws_inventory_daily"},
            @{name="dws_04_purchase_daily_etl"; desc="DWS层-采购日汇总ETL"; sql="SELECT COUNT(*) FROM dws_purchase_daily"},
            @{name="dws_05_quality_daily_etl"; desc="DWS层-质量日汇总ETL"; sql="SELECT COUNT(*) FROM dws_quality_daily"},
            @{name="dws_06_equipment_runtime_daily_etl"; desc="DWS层-设备运行日汇总ETL"; sql="SELECT COUNT(*) FROM dws_equipment_runtime_daily"},
            @{name="dws_07_cost_daily_etl"; desc="DWS层-成本日汇总ETL"; sql="SELECT COUNT(*) FROM dws_cost_daily"},
            
            # ADS层 (25-30)
            @{name="ads_01_sales_analysis_etl"; desc="ADS层-销售分析报表ETL"; sql="SELECT COUNT(*) FROM ads_sales_analysis"},
            @{name="ads_02_production_analysis_etl"; desc="ADS层-生产分析报表ETL"; sql="SELECT COUNT(*) FROM ads_production_analysis"},
            @{name="ads_03_inventory_analysis_etl"; desc="ADS层-库存分析报表ETL"; sql="SELECT COUNT(*) FROM ads_inventory_analysis"},
            @{name="ads_04_purchase_analysis_etl"; desc="ADS层-采购分析报表ETL"; sql="SELECT COUNT(*) FROM ads_purchase_analysis"},
            @{name="ads_05_quality_analysis_etl"; desc="ADS层-质量分析报表ETL"; sql="SELECT COUNT(*) FROM ads_quality_analysis"},
            @{name="ads_06_business_overview_etl"; desc="ADS层-综合经营分析报表ETL"; sql="SELECT COUNT(*) FROM ads_business_overview"}
        )
        
        Write-Host "`n开始创建30个工作流..." -ForegroundColor Yellow
        # 尝试不同的API端点
        $createUrl = "$baseUrl/dolphinscheduler/projects/$projectCode/process-definition"
        $createUrl2 = "$baseUrl/dolphinscheduler/projects/$projectCode/process/definition"
        
        $createdCount = 0
        for ($i = 0; $i -lt $workflows.Count; $i++) {
            $wf = $workflows[$i]
            $taskNum = $i + 1
            
            # 创建工作流定义
            $workflowDef = @{
                name = $wf.name
                description = $wf.desc
                globalParams = @()
                tasks = @(
                    @{
                        name = $wf.name
                        description = $wf.desc
                        taskType = "SHELL"
                        taskParams = @{
                            resourceList = @()
                            localParams = @()
                            rawScript = "mysql -h localhost -P 3306 -u sqluser -psqlpass123 sqlExpert -e `"$($wf.sql)`""
                            dependence = @{}
                            conditionResult = @{
                                successNode = @()
                                failedNode = @()
                            }
                            waitStartTimeout = @{}
                        }
                        flag = "YES"
                        taskPriority = "MEDIUM"
                        workerGroup = "default"
                        timeoutFlag = "CLOSE"
                        timeoutNotifyStrategy = $null
                        timeout = 0
                    }
                )
                tenantId = 1
                timeout = 0
                releaseState = "ONLINE"
                param = $null
                executionType = "PARALLEL"
            }
            
            try {
                # 使用UTF-8编码创建JSON
                $body = $workflowDef | ConvertTo-Json -Depth 10
                $utf8Body = [System.Text.Encoding]::UTF8.GetBytes($body)
                
                # 尝试第一个端点
                $success = $false
                $errorMsg = ""
                
                try {
                    $responseRaw = Invoke-WebRequest -Uri $createUrl -Method Post -Body $utf8Body -Headers $headers -ContentType "application/json; charset=utf-8" -UseBasicParsing
                    $response = $responseRaw.Content | ConvertFrom-Json
                    $success = $true
                } catch {
                    $errorMsg = $_.Exception.Message
                    # 尝试第二个端点
                    try {
                        $responseRaw = Invoke-WebRequest -Uri $createUrl2 -Method Post -Body $utf8Body -Headers $headers -ContentType "application/json; charset=utf-8" -UseBasicParsing
                        $response = $responseRaw.Content | ConvertFrom-Json
                        $success = $true
                    } catch {
                        $errorMsg = $_.Exception.Message
                    }
                }
                
                if ($success -and $response.code -eq 0) {
                    Write-Host "✓ [$($taskNum.ToString().PadLeft(2)) /30] 创建工作流成功: $($wf.name)" -ForegroundColor Green
                    $createdCount++
                } else {
                    Write-Host "❌ [$($taskNum.ToString().PadLeft(2)) /30] 创建工作流失败: $($wf.name) - $($response.msg)" -ForegroundColor Red
                }
            } catch {
                Write-Host "❌ [$($taskNum.ToString().PadLeft(2)) /30] 创建工作流异常: $($wf.name) - $($_.Exception.Message)" -ForegroundColor Red
            }
        }
        
        Write-Host "`n完成! 成功创建 $createdCount/30 个工作流" -ForegroundColor $(if ($createdCount -eq 30) { "Green" } else { "Yellow" })
        
    } else {
        Write-Host "❌ 登录失败: $($loginResponse.msg)" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ 发生错误: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
