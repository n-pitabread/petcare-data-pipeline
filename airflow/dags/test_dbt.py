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
    dag_id='test_dbt_v2',
    default_args=default_args,
    description='A simple DAG to run "dbt debug"',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt'],
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='dbt debug',
        cwd='/opt/airflow/dags/dbt'
    )

    run_dbt