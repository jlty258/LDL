# 测试使用用户名密码推送（虽然GitHub已不支持，但可以尝试）
param(
    [Parameter(Mandatory=$true)]
    [string]$Username,
    
    [Parameter(Mandatory=$true)]
    [string]$Password
)

Write-Host "注意：GitHub自2021年8月起不再支持密码认证" -ForegroundColor Yellow
Write-Host "此脚本仅用于测试，很可能失败" -ForegroundColor Yellow
Write-Host ""

# 设置远程URL包含凭据
$remoteUrl = "https://" + $Username + ":" + $Password + "@github.com/jlty258/LDL.git"

Set-Location "D:\LDL"
git remote set-url origin $remoteUrl

Write-Host "正在尝试推送..." -ForegroundColor Yellow
git push -u origin main

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "推送失败。GitHub已不再支持密码认证。" -ForegroundColor Red
    Write-Host "请使用Personal Access Token：" -ForegroundColor Yellow
    Write-Host "1. 访问: https://github.com/settings/tokens" -ForegroundColor White
    Write-Host "2. 创建新的token（classic）" -ForegroundColor White
    Write-Host "3. 勾选repo权限" -ForegroundColor White
    Write-Host "4. 使用token替代密码" -ForegroundColor White
}
