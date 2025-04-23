create or replace TABLE {{params.db_name}}.{{params.schema_name}}.SUBCATEGORIES (
	ID NUMBER(38,0) NOT NULL,
	CATEGORY_ID NUMBER(38,0),
	SUBCATEGORY_DESC VARCHAR(256),
	primary key (ID),
	foreign key (CATEGORY_ID) references {{params.db_name}}.{{params.schema_name}}.CATEGORIES(ID)
);