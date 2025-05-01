def refine_weather_table(**kwargs):
    """
    xcom_pull:
        - row_dataframe : api 호출을 끝내고 가져온 dataframe

    xcom_push
        - refine_data : weather Status와 필요한 데이터만 필터링된 데이터 프레임
    """
    ti = kwargs['ti']
    df = ti.xcom_pull(key='row_dataframe')
    df = df[['fcstDate','fcstTime','gu','category','fcstValue']]

    # make weather status
    df['weatherStatus'] = df.apply(assign_weather_status, axis=1)

    df = df.loc[df['weatherStatus'] != "plain"]
    df = df[['fcstDate','fcstTime','gu','weatherStatus']]

    # rename task
    df = df.rename(columns={
        'fcstData':'date',
        'fcstTime':'time',
        'gu':'gu',
        'weatherStatus':'weatherStatus'
    })

    ti.xcom_push(key='refine_dataframe',value=df)

def make_grid(**kwargs):
    import pandas as pd
    """
    grid table를 생성하는 함수

    xcom_push: 
        - grids
            - 직접 작성한 grid 인스턴스를 push
    """
    ti = kwargs['ti']
    grids = [
    ("강남구", 61, 126),
    ("강동구", 62, 126),
    ("강북구", 61, 128),
    ("강서구", 58, 126),
    ("관악구", 59, 125),
    ("광진구", 62, 127),
    ("구로구", 58, 125),
    ("금천구", 59, 124),
    ("노원구", 62, 129),
    ("도봉구", 61, 129),
    ("동대문구", 61, 127),
    ("동작구", 59, 125),
    ("마포구", 59, 127),
    ("서대문구", 59, 127),
    ("서초구", 60, 125),
    ("성동구", 61, 127),
    ("성북구", 61, 128),
    ("송파구", 62, 126),
    ("양천구", 58, 126),
    ("영등포구", 58, 125),
    ("용산구", 60, 126),
    ("은평구", 59, 128),
    ("종로구", 60, 127),
    ("중구", 60, 127),
    ("중랑구", 62, 128),
    ]
    grids = pd.DataFrame(grids, columns=["gu", "nx", "ny"])
    ti.xcom_push(key='grids',value=grids)

def assign_weather_status(row):
    category = row['category']
    value = str(row['fcstValue'])  # 문자열로 비교

    # 낙뢰
    if category == 'LGT':
        if int(value) > 1:
            return '낙뢰'

    # 강수형태
    if category == 'PTY':
        if value in ['1', '2', '4', '5', '6']:
            return '비'
        if value in ['3', '7']:
            return '눈'

    # 하늘 상태
    if category == 'SKY':
        if value == '1':
            return '맑음'
        elif value == '3':
            return '구름많음'
        elif value == '4':
            return '흐림'

    return "plain"