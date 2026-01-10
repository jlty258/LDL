MODEL (
  name example_model,
  kind FULL
);

SELECT 
  'Hello from SQLMesh!' as message,
  CURRENT_TIMESTAMP as query_time,
  version() as postgres_version;
