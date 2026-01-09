@echo off
chcp 65001 >nul
cd /d %~dp0\..
python scripts\stat_database_tables.py
pause
