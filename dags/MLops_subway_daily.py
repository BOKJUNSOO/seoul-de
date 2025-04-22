from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from common.base.get_subway_daily_data import get_data
from common.jobs.transfer import subwaystation_daily_data
from common.jobs.repository import postgreSQL
import pendulum

# batch 처리 api key
api_key = Variable.get("seoul_api_key")
# 데이터베이스, 스키마, 테이블명 정의
save_to_db = postgreSQL("seoulmoa","datawarehouse","subway_data_daily")
# for dump feature 
with DAG (
    dag_id='MLops_get_daily_data',
    description="(1일단위) 일 단위 지하철 사용량 예측을 위한 일일 데이터를 수집합니다. 매일 01시에 실행됩니다.",
    schedule='0 1 * * *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    get_data_=PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        op_args=[api_key]
    )

    refine_data_=PythonOperator(
        task_id='refine_data',
        python_callable=subwaystation_daily_data
    )

    save_data_=PythonOperator(
        task_id='save_data',
        python_callable=save_to_db.save_to_subwayDaily_table
    )

    get_data_>> refine_data_ >> save_data_