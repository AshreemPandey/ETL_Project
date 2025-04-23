from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain

@dag(
    dag_display_name='Master DAG',
    start_date=datetime(2025,3,5),
    max_active_runs=1,
    schedule='@daily',
    default_args={
        "owner":"airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries":1,
        "retry_delay":timedelta(minutes=2)
    },
    catchup=False
)
def master_dag():
    start_ETL=EmptyOperator(task_id="Start_ETL")

    end_ETL=EmptyOperator(task_id="End_ETL")

    trigger_external_stage_to_source_dag=TriggerDagRunOperator(
        task_id="move_from_external_stage_to_source",
        trigger_dag_id="external_stage_to_source",
        wait_for_completion=True,
        deferrable=True #leave worker slot after triggering
    )

    trigger_source_to_stage_dag=TriggerDagRunOperator(
        task_id="move_from_source_to_stage",
        trigger_dag_id="source_to_stage",
        wait_for_completion=True,
        deferrable=True
    )

    trigger_stage_to_temp_dag=TriggerDagRunOperator(
        task_id="move_from_stage_to_temp",
        trigger_dag_id="stage_to_temp",
        wait_for_completion=True,
        deferrable=True
    )

    trigger_temp_to_target_dag=TriggerDagRunOperator(
        task_id="move_from_temp_to_target",
        trigger_dag_id="temp_to_target",
        wait_for_completion=True,
        deferrable=True
    )


    chain(
        start_ETL,
        trigger_external_stage_to_source_dag,
        trigger_source_to_stage_dag,
        trigger_stage_to_temp_dag,
        trigger_temp_to_target_dag,
        end_ETL
    )

master_dag()