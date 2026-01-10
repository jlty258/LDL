-- 示例模型：从 PostgreSQL 数据库查询数据
-- 这个模型会查询 postgres 数据库中的表（如果存在）

select 
    'Hello from dbt!' as message,
    current_timestamp as query_time,
    version() as postgres_version
