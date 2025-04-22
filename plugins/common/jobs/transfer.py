import pandas as pd
# 원하는 형태로 테이블을 정제하는 모듈
# 사전에 정의한 스키마로 컬러머명을 변경

def subwaystation_data(**kwargs):
    """
    Subway 테이블을 생성하는 airflow task 함수

    airflow task instance에서 dataframe을 pull 하여 정제한다.
    schema에 맞게(컬럼명 조정) 정제한 테이블을 task instance에 push 한다..
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



def event_data(**kwargs):
    """
    Event 테이블을 생성하는 airflow task 함수

    airflow task instance에서 dataframe을 pull 하여 정제한다.
    schema에 맞게(컬럼명조정) 정제한 테이블을 postgreSQL에 저장한다.
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
    df["ROW_NUMBER"] = range(1,len(df)+1)
    
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


def subwaystation_montly_data(**kwargs):
    """
    task instance로 부터 pandas df를 pull하고
    Feature engineering한 테이블형태로 정제하며 

    airflow task instance에해당 데이터를 push 한다.
    """
    print("start refine task!")
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='row_dataframe')

    # 시간대 컬럼 리스트 추출
    hourly_on_off_columns = [col for col in df.columns if col.startswith('HR_')]
    print(hourly_on_off_columns)
    
    # melt
    feature_table_melted = pd.melt(
        df,
        id_vars=['USE_MM', 'STTN', 'SBWY_ROUT_LN_NM'],
        value_vars=hourly_on_off_columns,
        var_name='hour_type',
        value_name='count'
    )

    # 안전한 split
    split_columns = feature_table_melted['hour_type'].str.split('_', expand=True)
    feature_table_melted['hour'] = split_columns[1].astype(int)
    feature_table_melted['ride_type'] = split_columns[3]
    feature_table_melted.drop(columns='hour_type',inplace=True)
    
    feature_table_melted['get_on'] = feature_table_melted.apply(
    lambda row: row['count'] if row['ride_type'] == 'ON' else 0, axis=1
    )

    feature_table_melted['get_off'] = feature_table_melted.apply(
        lambda row: row['count'] if row['ride_type'] == 'OFF' else 0, axis=1
    )

    # 이제 hour 단위로 ON/OFF 한 줄로 묶기 (groupby)
    feature_table_grouped = feature_table_melted.groupby(
        ['USE_MM', 'STTN', 'SBWY_ROUT_LN_NM', 'hour'],
        as_index=False
    )[['get_on', 'get_off']].sum()

    # 필요한 컬럼 정리 (rename 등)
    feature_table_grouped.rename(columns={
        'USE_MM': 'use_month',
        'STTN': 'name',
        'SBWY_ROUT_LN_NM': 'line'
    }, inplace=True)

    # total 컬럼 추가
    feature_table_grouped['total'] = feature_table_grouped['get_on'] + feature_table_grouped['get_off']

    ti.xcom_push(key='refine_dataframe',value=feature_table_grouped)
    print("refine task done!")


def subwaystation_daily_data(**kwargs):
    """
    task instance로 부터 pandas df를 pull하고

    컬럼명 변경
    """
    print("start refine task!")
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='row_dataframe')
    df = df.rename(columns ={
        'USE_YMD':'service_date',
        'SBWY_ROUT_LN_NM':'line',
        'SBWY_STNS_NM':'name',
        'GTON_TNOPE':'get_on_d',
        'GTOFF_TNOPE':'get_off_d'
    })
    df = df.drop(columns='REG_YMD')

    ti.xcom_push(key='refine_dataframe',value=df)
    print("refine task done!")

def subwaystation_prediction_hourly_data(**kwargs):
    """
    예측한 시간대별 데이터를 pull 하고 

    스키마지정
    """
    print("start refine task!")
    ti = kwargs['ti']
    #
    df = ti.xcom_pull(key='row_dataframe')
    df = df.rename(columns ={
        'row_number':'row_number',
        'name':'name',
        'date':'service_date',
        'hour':'hour',
        'predicted_total':'predicted_total'
    })

if __name__ == "__main__":
    pass