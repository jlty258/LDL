
  
    

  create  table "sqlExpert"."dbt_test"."test_model__dbt_tmp"
  
  
    as
  
  (
    -- 简单的测试模型，用于验证 dbt 功能
with source_data as (
    select 
        1 as id,
        'dbt_test' as name,
        current_timestamp as created_at
    union all
    select 
        2 as id,
        'dbt_works' as name,
        current_timestamp as created_at
    union all
    select 
        3 as id,
        'dbt_validation' as name,
        current_timestamp as created_at
)

select * from source_data
  );
  