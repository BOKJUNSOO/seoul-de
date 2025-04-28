# check_status_ 의 값을 결정하는 함수
def check_status(**kwargs):
    ti = kwargs['ti']
    status = str(ti.xcom_pull(task_ids='check_data_',key='key'))
    return status

# check_daily_의 값을 결정하는 함수
def check_daily(**kwargs):
    ti = kwargs['ti']
    status = str(ti.xcom_pull(task_ids='check_data_',key='key'))
    if status == "save_to_event_":
        status = "refine_data_"
    else:
        status = "refine_data_s"
    return status