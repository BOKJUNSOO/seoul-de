import requests
import pandas as pd
from datetime import datetime

result_list = []
def get_data(api_key:str,**kwargs) -> pd.DataFrame:
    """
    전달받은 api_key 를 이용해 문화데이터를 호출하고, 필요한 값을 파싱하는 함수
    파싱한 값을 airflow task instance에 push한다.
    """
    BATCH_DATE = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d")
    print(BATCH_DATE +"일자의 BATCH 처리를 시작합니다.")

    try:
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/culturalEventInfo/1/1"
        first_response = requests.get(url, timeout=5)
        first_response.raise_for_status()

        json_data = first_response.json()
        result_list.extend(json_data['culturalEventInfo']['row'])
        end_page = json_data['culturalEventInfo']['list_total_count']

        # test용 상수 지울것
        end_page = 20

        print(f"전체 데이터 건수: {end_page}")

    except requests.exceptions.RequestException as e:
        print(f"Open Api Server Error!")
    
    # 2. 유효 페이지 수만큼 루프
    for page in range(2, end_page+1):
        if page % 20 == 0:
            print(f"{page}/{end_page} 를 호출중입니다.")
        try:
            url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/culturalEventInfo/{page}/{page}"
            response = requests.get(url, timeout=5)
            if response.status_code != 200:
                print(f"[오류] 페이지 {page} - 상태 코드 {response.status_code}")
                break

            response = response.json()
            data = response['culturalEventInfo']['row']

            # 유효 일자 필터링
            start_date = data[0]['STRTDATE']
            end_date = data[0]['END_DATE']

            # 아직 행사가 시작되지 않은 경우는 모두 수집 대상
            if start_date >= BATCH_DATE:
                result_list.extend(data)
                continue
            
            # 행사가 진행중인 경우
            if (start_date <= BATCH_DATE)and(end_date >= BATCH_DATE):
                result_list.extend(data)
                continue

        except requests.exceptions.RequestException as e:
            print(f"[예외 발생] 페이지 {page} - {e}")
            continue

    
    df = pd.DataFrame(result_list)
    print(f"최종 수집 건수: {len(df)}")
    # task instance
    ti = kwargs['ti']
    ti.xcom_push(key='dataframe', value=df)

if __name__ == "__main__":
    # 테스트시 apikey, 오늘날짜 명시 필요
    pass