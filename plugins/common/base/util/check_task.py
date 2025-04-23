# check_status_ 의 값을 결정하는 함수
def check_status(**kwargs):
    ti = kwargs['ti']
    status = str(ti.xcom_pull(task_ids='check_data_',key='key'))
    return status