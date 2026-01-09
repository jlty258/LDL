# GitHub Personal Access Token 创建脚本
param(
    [Parameter(Mandatory=$true)]
    [string]$Username,
    
    [Parameter(Mandatory=$true)]
    [string]$Password
)

$ErrorActionPreference = "Continue"

Write-Host "正在尝试通过GitHub API创建Personal Access Token..." -ForegroundColor Yellow
Write-Host "注意：此方法可能因GitHub安全策略而失败" -ForegroundColor Yellow
Write-Host ""

# 准备Basic Auth
$credentials = $Username + ":" + $Password
$bytes = [System.Text.Encoding]::UTF8.GetBytes($credentials)
$encodedCredentials = [Convert]::ToBase64String($bytes)

# GitHub API URL
$apiUrl = "https://api.github.com/authorizations"

# 请求头
$authHeader = "Basic " + $encodedCredentials
$headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
$headers.Add("Authorization", $authHeader)
$headers.Add("Accept", "application/vnd.github.v3+json")
$headers.Add("User-Agent", "LDL-Project")

# 请求体
$bodyObj = New-Object PSObject
$bodyObj | Add-Member -MemberType NoteProperty -Name "scopes" -Value @("repo")
$bodyObj | Add-Member -MemberType NoteProperty -Name "note" -Value "LDL项目推送"
$bodyObj | Add-Member -MemberType NoteProperty -Name "note_url" -Value "https://github.com/jlty258/LDL"
$jsonBody = $bodyObj | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $apiUrl -Method Post -Headers $headers -Body $jsonBody -ContentType "application/json"
    
    Write-Host ""
    Write-Host "Token创建成功！" -ForegroundColor Green
    Write-Host "Token: $($response.token)" -ForegroundColor Green
    Write-Host ""
    Write-Host "请立即保存此token，它只会显示一次！" -ForegroundColor Red
    
    # 自动配置Git远程仓库
    Write-Host ""
    Write-Host "正在配置Git远程仓库..." -ForegroundColor Yellow
    $token = $response.token
    $remoteUrl = "https://jlty258" + ":" + $token + "@github.com/jlty258/LDL.git"
    
    Set-Location "D:\LDL"
    git remote set-url origin $remoteUrl
    
    Write-Host "Git远程仓库已配置" -ForegroundColor Green
    Write-Host ""
    Write-Host "正在推送代码..." -ForegroundColor Yellow
    
    git push -u origin main
    
    Write-Host ""
    Write-Host "代码推送成功！" -ForegroundColor Green
    
} catch {
    $statusCode = 0
    if ($_.Exception.Response) {
        $statusCode = [int]$_.Exception.Response.StatusCode
    }
    $errorMessage = $_.Exception.Message
    
    Write-Host ""
    Write-Host "Token创建失败" -ForegroundColor Red
    if ($statusCode -ne 0) {
        Write-Host "状态码: $statusCode" -ForegroundColor Red
    }
    Write-Host "错误信息: $errorMessage" -ForegroundColor Red
    
    if ($statusCode -eq 401) {
        Write-Host ""
        Write-Host "可能的原因：" -ForegroundColor Yellow
        Write-Host "1. 用户名或密码错误" -ForegroundColor Yellow
        Write-Host "2. 账户启用了两步验证（2FA）" -ForegroundColor Yellow
        Write-Host "3. GitHub已不再支持通过API使用密码创建token" -ForegroundColor Yellow
    }
    
    Write-Host ""
    $separator = "=" * 60
    Write-Host $separator -ForegroundColor Cyan
    Write-Host "请手动创建Personal Access Token" -ForegroundColor Cyan
    Write-Host $separator -ForegroundColor Cyan
    Write-Host ""
    Write-Host "1. 访问: https://github.com/settings/tokens" -ForegroundColor White
    Write-Host "2. 点击 'Generate new token' -> 'Generate new token (classic)'" -ForegroundColor White
    Write-Host "3. 填写信息：" -ForegroundColor White
    Write-Host "   - Note: LDL项目推送" -ForegroundColor Gray
    Write-Host "   - Expiration: 选择有效期（建议90天或更长）" -ForegroundColor Gray
    Write-Host "   - 勾选权限: repo (完整仓库访问权限)" -ForegroundColor Gray
    Write-Host "4. 点击 'Generate token'" -ForegroundColor White
    Write-Host "5. 复制生成的token（只显示一次，请立即保存）" -ForegroundColor White
    Write-Host ""
    Write-Host "创建token后，运行以下命令：" -ForegroundColor Yellow
    Write-Host "  git remote set-url origin https://jlty258:YOUR_TOKEN@github.com/jlty258/LDL.git" -ForegroundColor Green
    Write-Host "  git push -u origin main" -ForegroundColor Green
    Write-Host ""
    Write-Host $separator -ForegroundColor Cyan
}
