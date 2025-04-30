from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import task

import pendulum
from datetime import timedelta

with DAG (
    dag_id='datapipline_subway_seoul_data',
    description="스케줄러 테스트용 dummy dag",
    schedule='0 3 */2 * *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'retries':3,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    dummy_task=BashOperator(
        task_id='bash_operator',
        bash_command=' echo "hello world" '
    )
    @task
    def dummy_function():
        print('hello_world')
    
    dummy_task >> dummy_function()
    

    