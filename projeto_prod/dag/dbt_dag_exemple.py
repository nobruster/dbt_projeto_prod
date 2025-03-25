from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3,25),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

dag = DAG('dbt_core_dag', default_args=default_args, schedule_interval=timedelta(days=1))

project_dir = "/usr/local/airflow/dags/projeto_prod"
profiles_dir = "/usr/local/airflow/dags/projeto_prod"

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f"dbt run --target prod -s +my_first_dbt_model --project-dir {project_dir}  --profiles-dir {profiles_dir} ",
    dag=dag
)
