from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from common.get_subway_data import get_data
from common.refine import refine_subway_data
from common.save import postgreSQL
import pendulum

# batch 처리 api key
api_key = Variable.get("seoul_api_key")
# 데이터베이스, 스키마, 테이블명 정의
save_to_db = postgreSQL("backend","datawarehouse","SubwayStation")
with DAG (
    dag_id='datapipline_subway_seoul_data',
    schedule='0 0 */2 * *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    get_data_=PythonOperator(
        task_id = 'get_data',
        python_callable=get_data,
        op_args=[api_key]
    )

    refine_data_=PythonOperator(
        task_id = 'refine_data',
        python_callable=refine_subway_data
    )

    save_data_=PythonOperator(
        task_id= 'save_data',
        python_callable=save_to_db.save_to_subway_table
    )

    get_data_ >> refine_data_ >> save_data_