# 원하는 형태로 테이블을 정제하는 모듈
# 사전에 정의한 스키마로 컬러머명을 변경

def subwaystation_data(**kwargs):
    from common.base.util.helper import refine_subway_name_data
    """
    subway_station 테이블을 생성하는 airflow task 함수

    airflow task instance에서 dataframe을 pull 하여 정제한다.
    schema에 맞게(컬럼명 조정) 정제한 테이블을 task instance에 push 한다..
    """
    print("[INFO] - start refine task!")
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
    # name 정제
    df = refine_subway_name_data(df)
    
    ti.xcom_push(key='refine_dataframe',value=df)
    print("[INFO] - refine task done!")
    
def subwaystation_montly_data(**kwargs):
    import pandas as pd
    from common.base.util.helper import refine_subway_name_data
    """
    앞선 task에서 수집한 작년의 월별/시간대별 데이터를 정제한다.

    xcom_pull : row_dataframe
    xcom_pust : refine_dataframe
    """
    print("[INFO] - start refine task!")
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='row_dataframe')

    # 시간대 컬럼 리스트 추출
    hourly_on_off_columns = [col for col in df.columns if col.startswith('HR_')]
    
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
    
    # name 정제
    feature_table_grouped = refine_subway_name_data(feature_table_grouped)
    
    # total 컬럼 추가
    feature_table_grouped['total'] = feature_table_grouped['get_on'] + feature_table_grouped['get_off']

    ti.xcom_push(key='refine_dataframe',value=feature_table_grouped)
    print("[INFO] - refine task done!")


def subwaystation_daily_data(**kwargs):
    from common.base.util.helper import refine_subway_name_data
    """
    일일단위 (4일전) 데이터를 수집하고 정제하는 함수

    xcom_pull : row_dataframe
    xcom_push : refine_dataframe
    """
    print("[INFO] - start refine task!")
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
    df = refine_subway_name_data(df)
    ti.xcom_push(key='refine_dataframe',value=df)
    print("[INFO] - refine task done!")

def subwaystation_prediction_hourly_data(**kwargs):
    from common.base.util.helper import refine_subway_name_data
    """
    Modeling 과정을 통해 생성된 데이터를 정제하는 함수

    xcom_pull : row_dataframe
    xcom_pust : refine_dataframe
    """
    print("[INFO] - start refine task!")
    ti = kwargs['ti']
    df = ti.xcom_pull(key='row_dataframe')
    #
    df = df.groupby(['name','date','hour'])['predicted_total'].sum().reset_index()
    
    end_ = len(df)
    df['row_number'] = range(1,end_+1)
    df = df[['row_number', 'name', 'date', 'hour', 'predicted_total']]
    df = df.rename(columns ={
        'row_number':'row_number',
        'name':'name',
        'date':'service_date',
        'hour':'hour',
        'predicted_total':'predicted_total'
    })
    df = refine_subway_name_data(df)
    df.info()
    ti.xcom_push(key='refine_dataframe',value=df)
    
if __name__ == "__main__":
    pass