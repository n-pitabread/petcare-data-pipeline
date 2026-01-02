from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='update_data_models',
    default_args=default_args,
    description='A simple DAG to run "dbt run"',
    schedule='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt'],
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='dbt run',
        cwd='/opt/airflow/dags/dbt'
    )

    run_dbt
