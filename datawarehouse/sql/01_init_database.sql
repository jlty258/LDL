-- 制造业数据仓库初始化脚本
-- 创建数据库和用户

CREATE DATABASE IF NOT EXISTS sqlExpert CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE sqlExpert;

-- 设置时区
SET time_zone = '+08:00';
