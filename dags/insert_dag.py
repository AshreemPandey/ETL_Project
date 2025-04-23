import os

from airflow import DAG
from datetime import timedelta,datetime
from airflow.decorators import dag,task_group,task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain

database='RETAILWAREHOUSE'
schema='SOURCE'
snowflake_external_stage_name='FIRSTEXTERNALSTAGE'

dag_directory=os.path.dirname(os.path.abspath(__file__))

@dag(
    dag_display_name='Load data from Blob to Snowflake',
    dag_id='external_stage_to_source',
    schedule_interval=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025,3,5),
        'email_on_failure': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=2)
    },
    template_searchpath=[
        os.path.join(dag_directory,"../include/sql")
    ]
)

def load_to_snowflake():

    start=EmptyOperator(task_id="Start_pipeline")
    dimension_tables_ready=EmptyOperator(task_id="Extracted_dimension_tables")
    end=EmptyOperator(task_id="End_Extraction")

    Dimension_tables=['countries','categories','subcategories','customers','products','regions','stores']

    for dimension_table in Dimension_tables:

        @task_group(group_id=f"Ingest_{dimension_table}_from_Blob")
        def ingest_dim_csv():

            create_table=SQLExecuteQueryOperator(
                task_id=f"Create_table_{dimension_table}_if_not_exists",
                conn_id='snow_conn',
                sql=f"create_{dimension_table}_table.sql",
                params={
                    "db_name": database,
                    "schema_name": schema
                }
            )

            copy_from_external_stage=CopyFromExternalStageToSnowflakeOperator(
                task_id=f"Copy_into_source.{dimension_table}_from_{dimension_table}.csv_from_blob",
                snowflake_conn_id='snow_conn',
                database=database,
                schema=schema,
                table=dimension_table,
                stage=snowflake_external_stage_name,
                prefix=f"{dimension_table}.csv",
                file_format="(type='CSV', field_delimiter=',',skip_header=1, field_optionally_enclosed_by='\"')"
            )

            chain(
                create_table,
                copy_from_external_stage
            )
        
        ingest_dim_csv=ingest_dim_csv()

        chain(
            start,
            ingest_dim_csv,
            dimension_tables_ready
        )

    @task_group(group_id=f"Ingest_sales_from_Blob")
    def ingest_fact_csv():
        create_fact_table=SQLExecuteQueryOperator(
            task_id=f"Create_sales_table_if_not_exists",
            conn_id='snow_conn',
            sql='create_sales_table.sql',
            params={
                "db_name":database,
                "schema_name":schema
            }
        )

        copy_fact_from_external_stage=CopyFromExternalStageToSnowflakeOperator(
            task_id=f"Copy_into_source.sales_from_sales.csv_from_blob_storage",
            snowflake_conn_id='snow_conn',
            database=database,
            schema=schema,
            table='SALES',
            stage=snowflake_external_stage_name,
            prefix="sales.csv",
            file_format="(type='CSV', field_delimiter=',', skip_header=1, field_optionally_enclosed_by='\"')"
        )

        chain(
            dimension_tables_ready,
            create_fact_table,
            copy_fact_from_external_stage,
            end
        )
    
    ingest_fact_csv()

load_to_snowflake()