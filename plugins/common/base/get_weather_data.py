result_list=[]
def get_data(api_key:str,**kwargs):
    import requests
    import pandas as pd
    import time
    """
    매 시 정각마다 api 요청을 한다.
    ex) 22시 요청 -> 22시 데이터가 필요하므로 2130 basetime을 갖는 header를 설정해 호출
    
    api 호출 부분
    args:
        api_key: 공공데이터 포털의 api key
    
    xcom_pull : 
        - grids
            - 서울시 행정구 위치 벡터데이터
            - get data 보다 앞쪽에 실행된다.
        - key
            - init or sync
            - grids를 만드는 task 보다 앞쪽에서 catch 이에따라 호출하는 params의 차이가 존재한다.
    xcom_push:
        - row_dataframe
            - 실시간 호출된 날씨 데이터
    """
    # pull value
    ti = kwargs['ti']
    
    grids=ti.xcom_pull(key='grids')

    base_date = kwargs["data_interval_end"].in_timezone("Asia/Seoul").subtract(hours=1).strftime("%Y%m%d")
    base_time = kwargs['data_interval_end'].in_timezone("Asia/Seoul").subtract(hours=1).strftime('%H')
    base_time = f"{base_time}30"
    
    url =  "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst"
    for _,grid in grids.iterrows():
        nx = grid['nx']
        ny = grid['ny']
        gu = grid['gu']
        params={
            'serviceKey':api_key,
            'pageNo':1,
            'numOfRows':'60',
            'dataType':'json',
            'base_date':base_date,
            'base_time':base_time,
            'nx':nx,
            'ny':ny
        }
        for retry in range(1,5):
            try:
                print(f"[INFO] - {_+1}/25 번째 데이터 {gu}의 데이터를 호출중 입니다.")
                res = requests.get(url=url,params=params)
                data = res.json()
                data = data['response']['body']['items']['item']
                for item in data:
                    item['gu'] = gu
                result_list.extend(data)
                break

            except requests.exceptions.RequestException as e:
                print(f"[EXCEPTION] - 공공데이터 포털 weather api 호출중 에러 - {e}")
                print(f"[EXCEPTION] - {_+1}/25 번째 데이터 {gu}의 데이터를 호출중 에러 발생")
                print(f"[CATCH] - 10초후 재시도 합니다.{retry}/4")
                time.sleep(10)
                continue
        
    df = pd.DataFrame(result_list) # 사용할 data는 refine task
    print("[INFO] - xcom_push - key : row_dataframe, value : dataframe")
    ti.xcom_push(key='row_dataframe',value=df)