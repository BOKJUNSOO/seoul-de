from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from common.get_culture_data import get_data
from common.refine_data import save_to_db
import pendulum

api_key = Variable.get("seoul_api_key")

with DAG (
    dag_id = "datapipline_with_seoul_data",
    schedule = "0 0 * * *",
    start_date = pendulum.datetime(2025,4,16, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    # [get_data task]
    get_data_ = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        op_args=[api_key]
    )

    save_to_db_ = PythonOperator(
        task_id="refine_data",
        python_callable=save_to_db
    )

    get_data_ >> save_to_db_