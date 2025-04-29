from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from common.base.get_subway_meta_data import get_data
from common.jobs.subway_ import subwaystation_data
from common.jobs.repository import postgreSQL
import pendulum
from datetime import timedelta

# batch 처리 api key
api_key = Variable.get("seoul_api_key")
# 데이터베이스, 스키마, 테이블명 정의
save_to_db = postgreSQL("seoulmoa","datawarehouse","subway_station")

with DAG (
    dag_id='datapipline_subway_seoul_data',
    description="(2일단위) 지하철 역사 마스터 정보를 수집하는 DAG입니다. 격일 자정 00시에 실행됩니다.",
    schedule='0 3 */2 * *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'retries':3,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    
    get_data_=PythonOperator(
        task_id = 'get_data',
        python_callable=get_data,
        op_args=[api_key]
    )

    refine_data_=PythonOperator(
        task_id = 'refine_data',
        python_callable=subwaystation_data
    )

    save_data_=PythonOperator(
        task_id= 'save_data',
        python_callable=save_to_db.save_to_subway_table
    )

    get_data_ >> refine_data_ >> save_data_