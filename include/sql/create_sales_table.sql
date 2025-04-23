create TABLE IF NOT EXISTS {{params.db_name}}.{{params.schema_name}}.SALES (
	ID NUMBER(38,0) NOT NULL,
	STORE_ID NUMBER(38,0) NOT NULL,
	PRODUCT_ID NUMBER(38,0) NOT NULL,
	CUSTOMER_ID NUMBER(38,0),
	TRANSACTION_TIME TIMESTAMP_NTZ(9),
	QUANTITY NUMBER(38,0),
	AMOUNT NUMBER(20,2),
	DISCOUNT NUMBER(20,2),
	primary key (ID),
	foreign key (STORE_ID) references {{params.db_name}}.{{params.schema_name}}.STORES(ID),
	foreign key (PRODUCT_ID) references {{params.db_name}}.{{params.schema_name}}.PRODUCTS(ID),
	foreign key (CUSTOMER_ID) references {{params.db_name}}.{{params.schema_name}}.CUSTOMERS(ID)
);