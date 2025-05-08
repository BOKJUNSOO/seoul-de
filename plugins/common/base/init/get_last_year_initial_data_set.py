result_list=[]
def get_data(api_key:str,**kwargs):
    from common.base.util.set_time import get_last_year_months
    import requests
    import pandas as pd
    import time
    """
    월별/시간별/역사별
    작년데이터를 수집합니다.

    모델이 시간대 및 계절성에 대한 비율을 학습하기 위한 데이터로 이용됩니다.
    """

    BATCH_YEAR = int(kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y"))
    target_year_month = get_last_year_months(BATCH_YEAR)

    for ym in target_year_month:
        print(f"[INFO] - {ym} 일자의 데이터를 요청합니다.")
        try:
            url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayTime/1/1/{ym}'
            first_response=requests.get(url)
            first_response.raise_for_status()

            json_data =first_response.json()
            result_list.extend(json_data['CardSubwayTime']['row'])

            end_page=json_data['CardSubwayTime']['list_total_count']
            print(f'[INFO] - 전체 데이터 건수:{end_page}, 이는 해당 월의 수집된 역사 갯수와 동일합니다.')
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] - api_key를 확인해주세요. 혹은 API SERVER 오류입니다.")
        
        for page in range(2,end_page+1):
            if page % 20 ==0:
                print(f"[INFO] - {page}/{end_page} 를 호출중입니다.")
            for retry in range(1,4):
                try:
                    url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayTime/{page}/{page}/{ym}'
                    response=requests.get(url)
                    if response.status_code == 200:
                        json_data =response.json()
                        data = json_data['CardSubwayTime']['row']
                        result_list.extend(data)
                        break

                except requests.exceptions.RequestException as e:
                    print(f"[EXCEPTION] - 페이지 {page} - {e}")
                    print(f"[CATCH] - 10초후 재시도 합니다 재요청 횟수 : {retry}/4")
                    time.sleep(10)
                    continue
    df = pd.DataFrame(result_list)
    print(f"[INFO] - 최종 수집 건수:{len(df)}")

    ti = kwargs['ti']
    print("[INFO] - xcom_push - key : row_dataframe, value : dataframe")
    ti.xcom_push(key='row_dataframe',value=df)
