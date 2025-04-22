from airflow import DAG
from airflow.operators.python import PythonOperator

from common.jobs.repository import postgreSQL
import pendulum

# target to read/ write table

# save_to_db = postgreSQL("seoulmoa","datawarehouse","SubwayPredict") # MLops result
read_from_db = postgreSQL("seoulmoa","datawarehouse","MontlySubwaystation") # test with event table

with DAG (
    dag_id='MLops_pipline',
    description="(1일단위) 지하철 사용량 예측 테이블을 생성하는 DAG 입니다. 매일 01시30분에 실행됩니다.",
    schedule='30 1 * * *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False
) as dag:

    #[connect with db]
    read_data_= PythonOperator(
        task_id = "read_data",
        python_callable=read_from_db.read_table
    )

    read_data_