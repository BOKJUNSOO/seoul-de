from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

from common.base.get_culture_data import get_data, make_sync_table
from common.jobs.event_ import event_data, check_event_description, re_search_function, make_summary_ai
from common.jobs.repository import postgreSQL
from common.base.util.check_task import check_status, check_daily
import pendulum
from datetime import timedelta

# batch 처리 api key
# check hook flag true
api_key = Variable.get("seoul_api_key")
OPEN_AI_KEY=Variable.get("OPEN_AI_KEY")

# 데이터베이스, 스키마, 테이블명 정의(main table and sub table for compare)
target_db = postgreSQL("seoulmoa","datawarehouse","event")
test_db = postgreSQL("seoulmoa","datawarehouse","event_sync")

with DAG (
    dag_id="datapipline_event_seoul_data",
    description="(1일단위) 문화행사 정보를 수집하는 DAG 입니다. 매일 00시에 DAG가 실행됩니다.",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025,4,16, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'retries':3,
        'retry_delay': timedelta(minutes=5)
    }

) as dag:
    
    # [check_database]
    check_data_=BranchPythonOperator(
        task_id='check_data_',
        python_callable=target_db.check_table
    )
    
    # [make event table task]
    make_event_table_=PythonOperator(
        task_id="make_event_table_",
        python_callable=get_data,
        op_args=[api_key]
    )
    
    # [make sync table task]
    make_sync_table_=PythonOperator(
        task_id="make_sync_table_",
        python_callable=make_sync_table,
        op_args=[api_key]
    )

    # [init or daily]
    check_daily_=BranchPythonOperator(
        task_id='check_daily_',
        python_callable=check_daily,
        trigger_rule="none_failed"
    )

    # [refine data and pass df]
    refine_data_=PythonOperator(
        task_id="refine_data_",
        python_callable=event_data,
        trigger_rule="none_failed",
        op_args=["init"]
    )

    # [refine data and pass df]
    refine_data_s=PythonOperator(
        task_id="refine_data_s",
        python_callable=event_data,
        trigger_rule="none_failed",
        op_args=["sync"]
    )

    # compare table
    read_event_table_=PythonOperator(
        task_id="read_event_table",
        python_callable=target_db.read_table
    )

    # [check status]
    check_status_=BranchPythonOperator(
        task_id='check_status_',
        python_callable=check_status
    )

    # [check event description and filltering]
    check_event_description_i_=PythonOperator(
        task_id='check_event_description_i_',
        python_callable=check_event_description,
        op_args=["init"]
    )

    check_event_description_s_=PythonOperator(
        task_id='check_event_description_s_',
        python_callable=check_event_description,
        op_args=["sync"]
    )

    # [re_search_ for html page]
    re_search_ = PythonOperator(
        task_id='re_search_',
        python_callable=re_search_function,
        trigger_rule="none_failed"
    )

    #[make_ai_summary task]
    make_summary_ai_=PythonOperator(
        task_id='make_summary_ai_',
        python_callable=make_summary_ai,
        op_args=[OPEN_AI_KEY]
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
                    s.longitude,
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
                    longitude,
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
                longitude,
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
                longitude,
                homepage,
                image_url,
                detail_url,
                target_user,
                event_description
            FROM numbered;
                     """)
    # make initial data
    check_data_ >> make_event_table_ >> check_daily_ >> refine_data_ >> check_event_description_i_>> re_search_>> make_summary_ai_>> check_status_ >> save_to_event_

    check_data_ >> make_sync_table_  >> check_daily_ >> refine_data_s >> read_event_table_ >> check_event_description_s_>> re_search_ >> make_summary_ai_>> check_status_ >>save_to_sync_ >> insert_event_