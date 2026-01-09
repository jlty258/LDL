# GitHub Token 配置和推送脚本
param(
    [Parameter(Mandatory=$true)]
    [string]$Token
)

Write-Host "正在配置Git远程仓库..." -ForegroundColor Yellow

$remoteUrl = "https://jlty258:" + $Token + "@github.com/jlty258/LDL.git"

Set-Location "D:\LDL"
git remote set-url origin $remoteUrl

Write-Host "Git远程仓库已配置" -ForegroundColor Green
Write-Host ""
Write-Host "正在推送代码..." -ForegroundColor Yellow

git push -u origin main

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "代码推送成功！" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "代码推送失败，请检查token是否正确" -ForegroundColor Red
}
