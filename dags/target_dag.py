import os

from airflow import DAG
from datetime import datetime,timedelta
from airflow.decorators import dag,task_group, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain

database='RETAILWAREHOUSE'
schema='TARGET'
prev_schema='TEMP'

dag_directory=os.path.dirname(os.path.abspath(__file__))

@dag(
    dag_display_name='Load data from temp to target',
    dag_id='temp_to_target',
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

def load_target():

    start=EmptyOperator(task_id="Start_loading_to_target")
    end=EmptyOperator(task_id="Completed_loading")

    Dimension_tables=['countries','categories','subcategories','customers','products','regions','stores']

    start_dimension_tables=EmptyOperator(task_id="Start_loading_dimensions")
    end_dimension_tables=EmptyOperator(task_id="End_loading_dimensions")
    for dimension_table in Dimension_tables:

        @task_group(group_id=f"Load_{dimension_table}_into_target")
        def load():
            create_target_table=SQLExecuteQueryOperator(
                task_id=f"Create_target_table_{dimension_table}",
                conn_id='snow_conn',
                sql=f"create_{dimension_table}_table.sql",
                params={
                    'db_name':database,
                    'schema_name':schema
                }
            )

            load_target_table=SQLExecuteQueryOperator(
                task_id=f"Load_{dimension_table}_into_target",
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
                create_target_table,
                load_target_table
            )
        
        load_to_target=load()
        
        chain(
            start,
            start_dimension_tables,
            load_to_target,
            end_dimension_tables
        )
    

    start_fact_loading=EmptyOperator(task_id="Starting_fact_loading")
    end_fact_loading=EmptyOperator(task_id="Ending_fact_loading")

    @task_group(group_id=f"Load_sales_fact")
    def load_fact():
        create_fact_table=SQLExecuteQueryOperator(
            task_id='Create_target_sales_fact',
            conn_id='snow_conn',
            sql='create_sales_table.sql',
            params={
                'db_name':database,
                'schema_name':schema
            }
        )

        copy_fact_into_target=SQLExecuteQueryOperator(
            task_id='Load_sales_fact_into_target',
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
            copy_fact_into_target,
            end_fact_loading
        )

    load_fact()

    chain(
        end_fact_loading,
        end
    )

load_target()

