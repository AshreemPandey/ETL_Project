create or replace TABLE {{params.db_name}}.{{params.schema_name}}.REGIONS (
	ID NUMBER(38,0) NOT NULL,
	COUNTRY_ID NUMBER(38,0),
	REGION_DESC VARCHAR(256),
	primary key (ID),
	foreign key (COUNTRY_ID) references {{params.db_name}}.{{params.schema_name}}.COUNTRIES(ID)
);