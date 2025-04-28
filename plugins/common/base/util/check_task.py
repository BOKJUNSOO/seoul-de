# check_status_ task 의 값을 결정하는 함수
def check_status(**kwargs):
    """
    AI 요약 이후에 실행되는 BranchOperater 함수
    event 혹은 sync 테이블중 어떤 테이블을 이용할지 String 으로 리턴해준다.
    args:
        pull key : check_data_
            - 메인 파이프라인 시작 task에서 check table 이후(repository 메서드) 메모리에 존재하는 값을 이용한다.
            - 해당 task에서 event 처리인지 sync 처리인지가 결정되어 있다.
    
    return : 
        status : string
            - 다음 테스크를 결정해준다.
            - save_to_event_ 혹은 save_to_sync task 둘중 하나의 값을 가진다.
    """
    ti = kwargs['ti']
    status = str(ti.xcom_pull(task_ids='check_data_',key='key'))
    return status

# check_daily_ task 의 값을 결정하는 함수
def check_daily(**kwargs):
    """
    공공데이터 API 호출이 끝난 이후 실행되는 BranchOperator 함수
    flow의 흐름을 정리하기 위해 존재하는 중간단계의 함수
    args:
        pull key : check_data_
            - 메인 파이프라인 시작 task에서 check table 이후(repository 메서드) 메모리에 존재하는 값을 이용한다.
            - 해당 task에서 event 처리인지 sync 처리인지가 결정되어 있다.
    
    return : 
        status : string
            - 다음 테스크를 결정해준다.
            - refine_data_ 혹은 refine_data_s task 둘중 하나의 값을 가진다.
            - 전자의 경우 event 테이블 정제/ 후자의 경우 event sync 테이블 정제
    """
    ti = kwargs['ti']
    status = str(ti.xcom_pull(task_ids='check_data_',key='key'))
    if status == "save_to_event_":
        status = "refine_data_"
    else:
        status = "refine_data_s"
    return status