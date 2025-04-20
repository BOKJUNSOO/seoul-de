import pandas as pd
from common.repository.repository import postgreSQL
def refine_subway_data(**kwargs):
    """
    Subway 테이블을 생성하는 airflow task 함수

    airflow task instance에서 dataframe을 pull 하여 정제한다.
    schema에 맞게 정제한 테이블을 task instance에 push 한다..
    """
    print("start refine task!")
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='row_dataframe')
    # refine
    df = df.rename(columns ={
        'BLDN_ID':'station_id',
        'BLDN_NM':'name',
        'ROUTE':'line',
        'LAT':'latitude',
        'LOT':'longitude'
    })
    
    ti.xcom_push(key='refine_dataframe',value=df)
    print("refine task done!")



def refine_event_data(**kwargs):
    """
    Event 테이블을 생성하는 airflow task 함수

    airflow task instance에서 dataframe을 pull 하여 정제한다.
    schema에 맞게 정제한 테이블을 postgreSQL에 저장한다.
    """
    print("start refine task!")
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='row_dataframe')
    # refine
    df["BOOL_FEE"] = df["IS_FREE"].map({'유료':False,
                                       '무료':True})
    
    df["STRTDATE"] = pd.to_datetime(df["STRTDATE"])
    df["STRTDATE"] = df["STRTDATE"].dt.date
    df["END_DATE"] = pd.to_datetime(df["END_DATE"])
    df["END_DATE"] = df["END_DATE"].dt.date
    # add column
    df["ROW_NUMBER"] = range(len(df))
    
    # refine column
    condtion = df['CODENAME'].str.contains("축제", na = False)
    df.loc[condtion,'CODENAME'] = "축제"

    # follow schema
    columns = ['ROW_NUMBER','TITLE','CODENAME','GUNAME','PLACE','STRTDATE','END_DATE','USE_FEE','BOOL_FEE','LAT','LOT','HMPG_ADDR','MAIN_IMG','ORG_LINK','USE_TRGT','ALT']
    df = df[columns]

    df = df.rename(columns={
        'ROW_NUMBER':'event_id',
        'TITLE':'title',
        'CODENAME':'category_id',
        'GUNAME':'gu',
        'PLACE':'location',
        'STRTDATE':'start_date',
        'END_DATE':'end_date',
        'USE_FEE':'fee',
        'BOOL_FEE':'is_free',
        'LAT':'latitude',
        'LOT':'longtitude',
        'HMPG_ADDR':'homepage',
        'MAIN_IMG':'image_url',
        'ORG_LINK':'detail_url',
        'USE_TRGT':'target_user',
        'ALT':'event_description'
    })
    ti.xcom_push(key='refine_dataframe',value=df)
    print("refine task done!")
    

if __name__ == "__main__":
    pass