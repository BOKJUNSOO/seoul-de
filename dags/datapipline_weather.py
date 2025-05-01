from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from common.base.get_weather_data import get_data
from common.jobs.weather_ import make_grid , refine_weather_table, assign_weather_status
from common.jobs.repository import postgreSQL

import pendulum
from datetime import timedelta

with DAG (
    dag_id='datapipline_weather_public_data',
    description="(1시간단위) 기상청 초단기 예측 데이터를 수집하는 DAG 입니다. 매시 30분에 DAG가 실행됩니다.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2025,4,16, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'retries':3,
        'retry_delay':timedelta(minutes=5)
    }
) as dag:

    # make grid
    make_grid_= PythonOperator(
        task_id='make_grid_',
        python_callable=make_grid
    )

    # get weather data
    get_weather_data_=PythonOperator(
        task_id='get_weather_data_',
        python_callable=get_data,
        op_args=[Variable.get("portal_key_secret")]
    )

    # refine weather data
    refine_weather_table_=PythonOperator(
        task_id='refine_weather_data_',
        python_callable=refine_weather_table
    )

    # save to db
    save_weather_data_=PythonOperator(
        task_id='save_weather_data_',
        python_callable=postgreSQL("seoulmoa","datawarehouse","weather").save_to_weather_table
    )

    make_grid_ >> get_weather_data_ >> refine_weather_table_ >> save_weather_data_