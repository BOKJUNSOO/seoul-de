import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

result_list = []
def get_data(api_key:str,**kwargs):
    """
    전달받은 api_key를 이용해 역사마스터 데이터를 요청하고

    airflow task instance에 해당 데이터를 push 한다
    """

    BATCH_DATE = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d")
    print(BATCH_DATE + "일자의 BATCH 처리를 시작합니다.")
    print("지하철 역사 마스터 정보 요청")

    
    try:
        url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/subwayStationMaster/1/1/'
        first_response = requests.get(url,timeout=5)
        first_response.raise_for_status()

        json_data = first_response.json()
        result_list.extend(json_data['subwayStationMaster']['row']) # 테스트용 데이터 저장

        # 요청 page 수
        end_page = json_data['subwayStationMaster']['list_total_count']
        print(f"전체 데이터 건수: {end_page}")

    except requests.exceptions.RequestException as e:
        print(f'api_key를 확인해주세요. 혹은 API SERVER 자체 오류입니다.')
    
    # api 요청
    for page in range(2,end_page):
        if page % 20 == 0:
            print(f'{page}/{end_page} 를 호출중입니다.')
        try:
            url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/subwayStationMaster/{page}/{page}/'
            response = requests.get(url,timeout=5)
            if response.status_code != 200:
                print(f"[오류] 페이지 {page} - 상태 코드 {response.status_code}")
            response = response.json()
            data = response['subwayStationMaster']['row']
            result_list.extend(data)
        except requests.exceptions.RequestException as e:
            print(f"[예외 발생] 페이지 {page} - {e}")
            continue
    
    # json to table
    df = pd.DataFrame(result_list)
    print(f"최종 수집 건수:{len(df)}")
    print(df)
    # task instance
    ti = kwargs['ti']
    ti.xcom_push(key='row_dataframe',value=df)
