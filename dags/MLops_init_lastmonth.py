from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from common.base.init.get_subway_initial_data_set import get_data
from common.jobs.transfer import subwaystation_daily_data
from common.jobs.repository import postgreSQL
import pendulum

# batch 처리 api key
api_key = Variable.get("seoul_api_key")
# 데이터베이스, 스키마, 테이블명 정의
save_to_db = postgreSQL("seoulmoa","datawarehouse","subway_data_prev_month")

with DAG (
    dag_id='MLops_init_get_lastmonth',
    description="(init)(최초1회 이후 unpause 바랍니다.)초기 예측 모델 데이터셋 구축을 위한 DAG입니다. 추후에는 일일데이터 수집으로 대체되며 최초 1회만 실행합니다.",
    schedule='0 0 7 * *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    get_data_=PythonOperator(
        task_id='get_data',
        python_callable=get_data, # 전달 데이터를 모두 가져오는 함수
        op_args=[api_key]
    )

    refine_data_=PythonOperator(
        task_id='refine_data',
        python_callable=subwaystation_daily_data # 일일 데이터 수집 컬럼명과 동일
    )

    save_data_=PythonOperator(
        task_id='save_data',
        python_callable=save_to_db.save_to_subwayDaily_table
    )

    get_data_>> refine_data_ >>save_data_