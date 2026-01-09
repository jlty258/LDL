@echo off
chcp 65001 >nul
echo 正在配置Chrome浏览器启动时最大化...
echo.

powershell -ExecutionPolicy Bypass -Command "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-Content scripts\set_chrome_maximize.ps1 -Encoding UTF8 | Out-String | Invoke-Expression"

pause
