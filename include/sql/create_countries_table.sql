create or replace TABLE {{params.db_name}}.{{params.schema_name}}.COUNTRIES (
	ID NUMBER(38,0) NOT NULL,
	COUNTRY_DESC VARCHAR(256),
	primary key (ID)
);