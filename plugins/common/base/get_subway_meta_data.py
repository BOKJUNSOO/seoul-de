result_list = []
def get_data(api_key:str,**kwargs):
    import requests
    import pandas as pd
    """
    역사 마스터 정보를 수집하는 함수
    subway_station 테이블을 생성한다.

    args:
        api_key : 서울 공공데이터 api키

    push key:
        row_dataframe
        - 단순히 호출된 데이터 이므로 row_dataframe으로 정의한다.
    """

    BATCH_DATE = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d")
    print("[INFO] - " + BATCH_DATE + "일자의 BATCH 처리를 시작합니다.")
    print("[INFO] - 지하철 역사 마스터 정보 요청")

    
    try:
        url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/subwayStationMaster/1/1/'
        first_response = requests.get(url,timeout=5)
        first_response.raise_for_status()

        json_data = first_response.json()
        result_list.extend(json_data['subwayStationMaster']['row']) # 테스트용 데이터 저장

        # 요청 page 수
        end_page = json_data['subwayStationMaster']['list_total_count']
        print(f"[INFO] - 전체 데이터 건수: {end_page}")

    except requests.exceptions.RequestException as e:
        print(f'[EXCEPTION] - api_key를 확인해주세요. 혹은 API SERVER 자체 오류입니다.')
    
    # api 요청
    for page in range(2,end_page+1):
        if page % 20 == 0:
            print(f'[INFO] - {page}/{end_page} 를 호출중입니다.')
        for retry in range(1,4):
            try:
                url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/subwayStationMaster/{page}/{page}/'
                response = requests.get(url,timeout=5)
                if response.status_code == 200:
                    response = response.json()
                    data = response['subwayStationMaster']['row']
                    result_list.extend(data)
                    break
                
            except requests.exceptions.RequestException as e:
                print(f"[EXCEPTION] - 페이지 {page} - {e} 호출중 오류발생")
                print(f"[CATACH] - 10초후 재시도 합니다 재요청 횟수 : {retry}/4")
                continue
    
    # json to table
    df = pd.DataFrame(result_list)
    print(f"[INFO] - 최종 수집 건수:{len(df)}")
    # task instance
    ti = kwargs['ti']
    print("[INFO] - xcom_push - key : row_dataframe, value : dataframe")
    ti.xcom_push(key='row_dataframe',value=df)
