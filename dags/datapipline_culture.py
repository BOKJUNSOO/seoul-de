from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

from common.base.get_culture_data import get_data #, make_sync_table
from common.jobs.event_ import event_data, check_event_description, re_search_function, make_summary_ai , check_status, check_daily
from common.jobs.repository import postgreSQL

import pendulum
from datetime import timedelta

# test name
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
    check_data_=PythonOperator(
        task_id='check_data_',
        python_callable=postgreSQL("seoulmoa","datawarehouse","event").check_table
    )
    
    # [make basic event table task]
    get_event_data_=PythonOperator(
        task_id="get_event_data_",
        python_callable=get_data,
        op_args=[Variable.get("seoul_api_key")]
    )

    # [init or sync]
    check_daily_=BranchPythonOperator(
        task_id='check_daily_',
        python_callable=check_daily,
        trigger_rule="none_failed"
    )

    # [refine data and pass df]
    refine_data_=PythonOperator(
        task_id="refine_data_",
        python_callable=event_data,
        trigger_rule="none_failed"
    )

    # compare table
    read_event_table_=PythonOperator(
        task_id="read_event_table_",
        python_callable=postgreSQL("seoulmoa","datawarehouse","event").read_table
    )

    # [check status]
    check_status_=BranchPythonOperator(
        task_id='check_status_',
        python_callable=check_status
    )

    # [check event description and filltering]
    check_event_description_i_=PythonOperator(
        task_id='check_event_description_i_',
        python_callable=check_event_description
    )

    check_event_description_s_=PythonOperator(
        task_id='check_event_description_s_',
        python_callable=check_event_description
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
        op_args=[Variable.get("OPEN_AI_KEY_secret")]
    )

    # [save to event table]
    save_to_event_=PythonOperator(
        task_id="save_to_event_",
        python_callable=postgreSQL("seoulmoa","datawarehouse","event").save_to_event_table
    )
    
    # [save to sync table]
    save_to_sync_=PythonOperator(
        task_id="save_to_sync_",
        python_callable=postgreSQL("seoulmoa","datawarehouse","event_sync").save_to_event_table
    )

    # [make note to batch_status table]
    insert_batch_status_=PostgresOperator(
        task_id='insert_batch_status',
        postgres_conn_id='seoul_moa_event_conn',
        sql = """
            INSERT INTO datawarehouse.batch_status (batch_id, execute_time, status)
            SELECT
                COALESCE(MAX(batch_id), 0) + 1,
                '{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M") }}'::timestamp,
                0
            FROM datawarehouse.batch_status;
            """
    )
    
    check_data_ >> get_event_data_ >> refine_data_ >> check_daily_ >> check_event_description_i_>> re_search_>> make_summary_ai_>> check_status_ >> save_to_event_

    check_data_ >> get_event_data_ >> refine_data_ >> check_daily_ >> read_event_table_ >> check_event_description_s_>> re_search_ >> make_summary_ai_>> check_status_ >>save_to_sync_ >> insert_batch_status_
