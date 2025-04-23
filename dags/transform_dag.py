import os

from airflow import DAG
from datetime import timedelta,datetime
from airflow.decorators import dag,task_group,task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain


database='RETAILWAREHOUSE'
schema='TEMP'
prev_schema='STAGE'

dag_directory=os.path.dirname(os.path.abspath(__file__))


@dag(
    dag_display_name='Load data from stage to temp and transform',
    dag_id='stage_to_temp',
    schedule_interval=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    default_args={
        'owner':'airflow',
        'depends_on_past':False,
        'start_date':datetime(2025,3,5),
        'email_on_failure':False,
        'retries':0,
        'retry_delay':timedelta(minutes=2)
    },
    template_searchpath=[
        os.path.join(dag_directory,"../include/sql")
    ]
)

def transform():

    start=EmptyOperator(task_id='Start_transformation_process')
    end=EmptyOperator(task_id='End_transformation_process')

    Dimension_tables=['countries','categories','subcategories','customers','products','regions','stores']

    start_dimension_tables=EmptyOperator(task_id="Start_of_transformation_of_dimension_tables")
    end_dimension_tables=EmptyOperator(task_id="End_of_transformation_of_dimension_tables")

    for dimension_table in Dimension_tables:

        @task_group(group_id=f"Transformation_on_{schema}_{dimension_table}_table")
        def L_T():
            create_temp=SQLExecuteQueryOperator(
                task_id=f"Create_temp_table_{dimension_table}_if_not_exists",
                conn_id='snow_conn',
                sql=f"create_{dimension_table}_table.sql",
                params={
                    'db_name':database,
                    'schema_name':schema,
                }
            )

            load_to_temp=SQLExecuteQueryOperator(
                task_id=f"Load_into_temp_{dimension_table}_table",
                conn_id='snow_conn',
                sql=f"copy_into_table.sql",
                params={
                    'db_name':database,
                    'prev_schema':prev_schema,
                    'schema_name':schema,
                    'table': dimension_table
                }
            )

            #perform transformation logic on temp.{dimension_table}

            chain(
                create_temp,
                load_to_temp,
            )

        transformation=L_T()

        chain(
            start,
            start_dimension_tables,
            transformation,
            end_dimension_tables
        )
    
    start_fact_transformation=EmptyOperator(task_id=f"Start_fact_table_transformation")
    end_fact_transformation=EmptyOperator(task_id=f"End_fact_table_transformation")

    @task_group(group_id=f"Transformation_of_fact_tables")
    def transform_fact():
        create_temp_fact=SQLExecuteQueryOperator(
            task_id=f"Create_temp_sales",
            conn_id='snow_conn',
            sql='create_sales_table.sql',
            params={
                'db_name':database,
                'schema_name':schema
            }
        )

        store_temp_fact=SQLExecuteQueryOperator(
            task_id=f"Store_into_temp_fact",
            conn_id='snow_conn',
            sql='copy_into_table.sql',
            params={
                'db_name':database,
                'prev_schema':prev_schema,
                'schema_name':schema,
                'table':'SALES'
            }
        )

        chain(
            end_dimension_tables,
            start_fact_transformation,
            create_temp_fact,
            store_temp_fact,
            end_fact_transformation
        )
    
    transform_fact=transform_fact()

    chain(
        end_fact_transformation,
        end
    )

transform()

