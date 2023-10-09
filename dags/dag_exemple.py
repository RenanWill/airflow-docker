from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'rwb',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='dag1',
    default_args=default_args,
    description='this is out first dag that we write',
    start_date=datetime(2023, 10, 3, 2),
    schedule_interval='@daily'
)as dag:
    task1 = BashOperator(
        task_id='dag1_task1',
        bash_command="echo task 1"
    )

    task2 = BashOperator(
        task_id='da1_task2',
        bash_command="echo task 2"
    )

    task3 = BashOperator(
        task_id='da1_task3',
        bash_command="echo task 3"
    )


    task1.set_downstream(task2)
    task1.set_downstream(task3)