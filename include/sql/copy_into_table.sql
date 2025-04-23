INSERT INTO {{params.db_name}}.{{params.schema_name}}.{{params.table}}
SELECT * FROM {{params.db_name}}.{{params.prev_schema}}.{{params.table}};