create TABLE IF NOT EXISTS {{params.db_name}}.{{params.schema_name}}.PRODUCTS (
	ID NUMBER(38,0) NOT NULL,
	SUBCATEGORY_ID NUMBER(38,0),
	PRODUCT_DESC VARCHAR(256),
	primary key (ID),
	foreign key (SUBCATEGORY_ID) references {{params.db_name}}.{{params.schema_name}}.SUBCATEGORIES(ID)
);