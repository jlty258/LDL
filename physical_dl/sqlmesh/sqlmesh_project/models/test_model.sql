MODEL (
  name test_model,
  kind FULL
);

SELECT 
  1 as id,
  'sqlmesh_test' as name,
  CURRENT_TIMESTAMP as created_at
UNION ALL
SELECT 
  2 as id,
  'sqlmesh_works' as name,
  CURRENT_TIMESTAMP as created_at
UNION ALL
SELECT 
  3 as id,
  'sqlmesh_validation' as name,
  CURRENT_TIMESTAMP as created_at;
