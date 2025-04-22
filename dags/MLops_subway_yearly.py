from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from common.base.init.get_last_year_initial_data_set import get_data
from common.jobs.transfer import subwaystation_montly_data
from common.jobs.repository import postgreSQL
import pendulum

# batch 처리 api key
api_key = Variable.get("seoul_api_key")
# 데이터베이스, 스키마, 테이블명 정의
save_to_db = postgreSQL("seoulmoa","datawarehouse","subway_data_prev_year")

with DAG (
    dag_id='MLops_init_get_lastyear',
    description="(init)(매년1회)작년의 월별 시간대별 데이터를 수집하는 DAG.",
    schedule='0 0 1 2 *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    get_data_=PythonOperator(
        task_id='get_data',
        python_callable=get_data, # 작년 월별/시간대별 데이터 수집
        op_args=[api_key]
    )

    refine_data_=PythonOperator(
        task_id='refine_data',
        python_callable=subwaystation_montly_data # 월별 데이터와 컬럼명 동일
    )

    save_data_=PythonOperator(
        task_id='save_data',
        python_callable=save_to_db.save_to_subwayDaily_table
    )

    get_data_>> refine_data_ >>save_data_