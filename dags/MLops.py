from airflow import DAG
from airflow.operators.python import PythonOperator

from common.jobs.repository import postgreSQL
from common.model.daily.preprocessing import modeling
from common.model.daily.postprocessing import predict_all_for_date # test
from common.model.daily.calculate import make_hourly_dataframe
from common.jobs.transfer import subwaystation_prediction_hourly_data
import pendulum

# target to read/ write table

read_from_db_1 = postgreSQL("seoulmoa","datawarehouse","subway_data_prev_month")
read_from_db_2 = postgreSQL("seoulmoa","datawarehouse","subway_data_daily")
read_from_db_3 = postgreSQL("seoulmoa","datawarehouse","subway_data_prev_year")

save_to_db = postgreSQL("seoulmoa","datawarehouse","subway_predict")
# save_to_test = postgreSQL("seoulmoa","datawarehouse","SubwayPredict_daily") #test
with DAG (
    dag_id='MLops_pipline',
    description="(1일단위) 지하철 사용량 예측 테이블을 생성하는 DAG 입니다. 매일 01시30분에 실행됩니다.",
    schedule='30 1 * * *',
    start_date=pendulum.datetime(2025,4,17, tz='Asia/Seoul'),
    catchup=False
) as dag:

    #[connect with db]
    read_montly_data_=PythonOperator(
        task_id="read_montly_data",
        python_callable=read_from_db_1.read_table
    )

    read_daily_data_=PythonOperator(
        task_id="read_daily_data",
        python_callable=read_from_db_2.read_table
    )

    #[preprocessed and train daily model]
    train_daily_model=PythonOperator(
        task_id='train_daily_model',
        python_callable=modeling
    )

    #[postprocessed and make daily prediction]
    make_daily_prediction=PythonOperator(
        task_id='make_daily_prediction',
        python_callable=predict_all_for_date
    )

    read_year_data_=PythonOperator(
        task_id='read_year_day',
        python_callable=read_from_db_3.read_table
    )

    calculate_ratio_=PythonOperator(
        task_id='calculate_ratio',
        python_callable=make_hourly_dataframe
    )

    refine_data_=PythonOperator(
        task_id='refine_data',
        python_callable=subwaystation_prediction_hourly_data
    )

    save_data_=PythonOperator(
        task_id='save_data',
        python_callable=save_to_db.save_to_hourly_predict
    )


    (
    [read_montly_data_, read_daily_data_] 
    >> train_daily_model 
    >>[make_daily_prediction ,read_year_data_]
#    >> test
    >> calculate_ratio_
    >> refine_data_
    >> save_data_
    )