@echo off
chcp 65001 >nul
echo ========================================
echo GitHub 代码推送工具
echo ========================================
echo.
echo 请先创建GitHub Personal Access Token:
echo 1. 访问: https://github.com/settings/tokens
echo 2. 点击 "Generate new token" -^> "Generate new token (classic)"
echo 3. 勾选权限: repo
echo 4. 生成并复制token
echo.
set /p TOKEN="请输入你的GitHub Token: "

if "%TOKEN%"=="" (
    echo 错误: Token不能为空
    pause
    exit /b 1
)

echo.
echo 正在配置Git远程仓库...
git remote set-url origin https://jlty258:%TOKEN%@github.com/jlty258/LDL.git

if %ERRORLEVEL% NEQ 0 (
    echo 配置失败
    pause
    exit /b 1
)

echo 正在推送代码...
git push -u origin main

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo 代码推送成功！
    echo ========================================
) else (
    echo.
    echo ========================================
    echo 代码推送失败，请检查token是否正确
    echo ========================================
)

pause
