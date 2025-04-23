import os

from airflow import DAG
from datetime import datetime,timedelta
from airflow.decorators import dag,task_group, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain

database='RETAILWAREHOUSE'
schema='STAGE'
prev_schema='SOURCE'

dag_directory=os.path.dirname(os.path.abspath(__file__))

@dag(
    dag_display_name='Load data from source to stage',
    dag_id='source_to_stage',
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

def load_stage():

    start=EmptyOperator(task_id="Start_loading_to_stage")
    end=EmptyOperator(task_id="Completed_loading")

    Dimension_tables=['countries','categories','subcategories','customers','products','regions','stores']

    start_dimension_tables=EmptyOperator(task_id="Start_loading_dimensions")
    end_dimension_tables=EmptyOperator(task_id="End_loading_dimensions")
    for dimension_table in Dimension_tables:

        @task_group(group_id=f"Load_{dimension_table}_into_stage")
        def load():
            create_stage_table=SQLExecuteQueryOperator(
                task_id=f"Create_stage_table_{dimension_table}",
                conn_id='snow_conn',
                sql=f"create_{dimension_table}_table.sql",
                params={
                    'db_name':database,
                    'schema_name':schema
                }
            )

            load_stage_table=SQLExecuteQueryOperator(
                task_id=f"Load_{dimension_table}_into_stage",
                conn_id='snow_conn',
                sql='copy_into_table.sql',
                params={
                    'db_name':database,
                    'prev_schema':prev_schema,
                    'schema_name':schema,
                    'table':dimension_table
                }
            )

            chain(
                start_dimension_tables,
                create_stage_table,
                load_stage_table
            )
        
        load_to_stage=load()
        
        chain(
            start,
            start_dimension_tables,
            load_to_stage,
            end_dimension_tables
        )
    

    start_fact_loading=EmptyOperator(task_id="Starting_fact_loading")
    end_fact_loading=EmptyOperator(task_id="Ending_fact_loading")

    @task_group(group_id=f"Load_sales_fact")
    def load_fact():
        create_fact_table=SQLExecuteQueryOperator(
            task_id='Create_stage_sales_fact',
            conn_id='snow_conn',
            sql='create_sales_table.sql',
            params={
                'db_name':database,
                'schema_name':schema
            }
        )

        copy_fact_into_stage=SQLExecuteQueryOperator(
            task_id='Load_sales_fact_into_stage',
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
            start_fact_loading,
            create_fact_table,
            copy_fact_into_stage,
            end_fact_loading
        )

    load_fact()

    chain(
        end_fact_loading,
        end
    )

load_stage()

