from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

from common.base.get_culture_data import get_data
from common.jobs.transfer import event_data
from common.jobs.repository import postgreSQL
from common.base.util.check_task import check_status
import pendulum

# batch 처리 api key
api_key = Variable.get("seoul_api_key")
# 데이터베이스, 스키마, 테이블명 정의
target_db = postgreSQL("seoulmoa","datawarehouse","event")
test_db = postgreSQL("seoulmoa","datawarehouse","event_sync")

with DAG (
    dag_id="datapipline_event_seoul_data",
    description="(1일단위) 문화행사 정보를 수집하는 DAG 입니다. 매일 00시에 DAG가 실행됩니다.",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025,4,16, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    # [check_database]
    check_data_=BranchPythonOperator(
        task_id='check_data_',
        python_callable=target_db.check_table
    )
    
    # [make event table task]
    make_event_data_=PythonOperator(
        task_id="make_event_data_",
        python_callable=get_data,
        op_args=[api_key]
    )
    
    # [make sync table task]
    make_sync_data_=PythonOperator(
        task_id="make_sync_data_",
        python_callable=get_data,
        op_args=[api_key]
    )

    # [refine data and pass df]
    refine_data_=PythonOperator(
        task_id="refine_data",
        python_callable=event_data,
        trigger_rule="none_failed"
    )

    # [check status]
    check_status_=BranchPythonOperator(
        task_id='check_status_',
        python_callable=check_status
    )

    # [save to event table]
    save_to_event_=PythonOperator(
        task_id="save_to_event_",
        python_callable=target_db.save_to_event_table
    )

    # [save to sync table]
    save_to_sync_=PythonOperator(
        task_id="save_to_sync_",
        python_callable=test_db.save_to_event_table
    )

    # [insert_task]
    insert_event_=PostgresOperator(
         task_id='insert_event',
         postgres_conn_id='seoul_moa_event_conn',
         sql="""
            WITH max_ev AS (
                SELECT COALESCE(MAX(event_id), 0) AS last_id
                  FROM datawarehouse.event
            ),
            missing AS (
                SELECT
                    s.title,
                    s.category_name,
                    s.gu,
                    s.location,
                    s.start_date,
                    s.end_date,
                    s.fee,
                    s.is_free,
                    s.latitude,
                    s.longtitude,
                    s.homepage,
                    s.image_url,
                    s.detail_url,
                    s.target_user,
                    s.event_description
                FROM datawarehouse.event_sync AS s
                LEFT JOIN datawarehouse.event AS e
                  ON s.homepage = e.homepage
                WHERE e.homepage IS NULL
            ),
            numbered AS (
                SELECT
                    ROW_NUMBER() OVER () + (SELECT last_id FROM max_ev) AS new_event_id,
                    title,
                    category_name,
                    gu,
                    location,
                    start_date,
                    end_date,
                    fee,
                    is_free,
                    latitude,
                    longtitude,
                    homepage,
                    image_url,
                    detail_url,
                    target_user,
                    event_description
                FROM missing
            )
            INSERT INTO datawarehouse.event (
                event_id,
                title,
                category_name,
                gu,
                location,
                start_date,
                end_date,
                fee,
                is_free,
                latitude,
                longtitude,
                homepage,
                image_url,
                detail_url,
                target_user,
                event_description
            )
            SELECT
                new_event_id,
                title,
                category_name,
                gu,
                location,
                start_date,
                end_date,
                fee,
                is_free,
                latitude,
                longtitude,
                homepage,
                image_url,
                detail_url,
                target_user,
                event_description
            FROM numbered;
                     """)

    check_data_ >> make_event_data_ >> refine_data_ >> check_status_ >> save_to_event_
    check_data_ >> make_sync_data_  >> refine_data_ >> check_status_ >> save_to_sync_ >> insert_event_