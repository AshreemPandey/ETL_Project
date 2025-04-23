create TABLE IF NOT EXISTS {{params.db_name}}.{{params.schema_name}}.CATEGORIES (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_DESC VARCHAR(1024),
	primary key (ID)
);