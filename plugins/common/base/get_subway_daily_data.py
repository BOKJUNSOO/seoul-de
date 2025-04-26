import requests
import pandas as pd
import time
result_list=[]
def get_data(api_key,**kwargs):
    """
    전달받은 api_key를 이용해 일일단위 지하철 이용량 데이터를 요청하고

    airflow task instance에 해당 데이터를 push 한다
    """

    BATCH_DATE = kwargs["data_interval_end"].in_timezone("Asia/Seoul").subtract(days=4).strftime("%Y%m%d")
    print(BATCH_DATE + "BATCH 처리를 시작합니다.")
    print(BATCH_DATE + "모델 Feature 쌓기 위한 데이터 호출중.")
    try:
        url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayStatsNew/1/1/{BATCH_DATE}/'
        first_response = requests.get(url)
        first_response.raise_for_status()
        
        json_data = first_response.json()
        result_list.extend(json_data['CardSubwayStatsNew']['row'])

        end_page = json_data['CardSubwayStatsNew']['list_total_count']
        print(f"전체 데이터 건수: {end_page}, 이는 수집된 지하철 역사 갯수와 동일합니다.")
    except requests.exceptions.RequestException as e:
        print(f"api_key를 확인해주세요. 혹은 API SERVER 오류입니다.")

    for page in range(2,end_page +1):
        if page % 20 ==0:
            print(f"{page}/{end_page} 를 호출중입니다.")
        for retry in range(1,4):
            try:
                url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayStatsNew/{page}/{page}/{BATCH_DATE}/'
                response = requests.get(url)
                if response.status_code == 200:
                    json_data = response.json()
                    data = json_data['CardSubwayStatsNew']['row']
                    result_list.extend(data)
                    break

            except requests.exceptions.RequestException as e:
                print(f"[예외 발생] 페이지 {page} - {e}")
                print(f"10초후 재시도 합니다 재요청 횟수 : {retry}/4")
                time.sleep(10)
                continue

    df = pd.DataFrame(result_list)
    print(f'최종 수집 건수:{len(df)}')
    
    ti = kwargs['ti']
    ti.xcom_push(key='row_dataframe',value=df)
