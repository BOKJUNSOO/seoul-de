result_list=[]
def get_data(api_key,**kwargs):
    from common.base.util.set_time import get_month_dates
    import requests
    import pandas as pd
    import time
    """
    전달받은 api_key를 이용해 "전달의 모든" 일일 지하철 이용량 데이터를 요청하고

    airflow task instance에 해당 데이터를 push 한다
    """
    BATCH_YEAR = int(kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y"))
    BATCH_MONTH = int(kwargs["data_interval_end"].in_timezone("Asia/Seoul").subtract(months=1).strftime("%m"))
    
    dates=get_month_dates(BATCH_YEAR,BATCH_MONTH)
    for date in dates:
        print(f'[INFO] - {date} 일자의 데이터를 요청합니다.')
        try:
            url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayStatsNew/1/1/{date}/'
            first_response = requests.get(url)
            first_response.raise_for_status()

            json_data = first_response.json()
            result_list.extend(json_data['CardSubwayStatsNew']['row'])

            end_page = json_data['CardSubwayStatsNew']['list_total_count']
            print(f"[INFO] - 전체 데이터 건수: {end_page}, 이는 수집된 지하철 역사 갯수와 동일합니다.")
        except requests.exceptions.RequestException as e:
            print(f"[EXCEPTION] - api_key를 확인해주세요. 혹은 API SERVER 오류입니다.")

        for page in range(2,end_page +1):
            if page % 20 ==0:
                print(f"[INFO] - {page}/{end_page} 를 호출중입니다.")
            for retry in range(1,4):
                try:
                    url = f'http://openapi.seoul.go.kr:8088/{api_key}/json/CardSubwayStatsNew/{page}/{page}/{date}/'
                    response = requests.get(url)
                    if response.status_code == 200:
                        json_data = response.json()
                        data = json_data['CardSubwayStatsNew']['row']
                        result_list.extend(data)
                        break
                    
                except requests.exceptions.RequestException as e:
                    print(f"[EXCEPTION] - 페이지 {page} - {e} 호출중 에러 발생")
                    print(f"[catch] - 10초후 재시도 합니다 재요청 횟수 : {retry}/4")
                    time.sleep(10)
                    continue
    df = pd.DataFrame(result_list)
    print(f'[INFO] - 최종 수집 건수:{len(df)}')
    
    ti = kwargs['ti']
    print("[INFO] - xcom_push - key : row_dataframe, value : dataframe")
    ti.xcom_push(key='row_dataframe',value=df)

