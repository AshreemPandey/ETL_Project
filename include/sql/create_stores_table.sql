CREATE TABLE IF NOT EXISTS {{params.db_name}}.{{params.schema_name}}.STORES(
    ID NUMBER(38,0) NOT NULL,
    REGION_ID NUMBER(38,0),
    STORE_DESC VARCHAR(256),
    primary key (ID),
    foreign key (REGION_ID) references {{params.db_name}}.{{params.schema_name}}.REGIONS(ID)
);