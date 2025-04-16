import pandas as pd
from common.save import postgreSQL

def save_to_db(**kwargs):
    """
    airflow task instance에서 dataframe을 pull 하여 정제한다.
    schema에 맞게 정제한 테이블을 postgreSQL에 저장한다.
    """
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='dataframe')
    # refine
    df["BOOL_FEE"] = df["IS_FREE"].map({'유료':False,
                                       '무료':True})
    
    df["STRTDATE"] = pd.to_datetime(df["STRTDATE"])
    df["STRTDATE"] = df["STRTDATE"].dt.date
    df["END_DATE"] = pd.to_datetime(df["END_DATE"])
    df["END_DATE"] = df["END_DATE"].dt.date
    # add column
    df["ROW_NUMBER"] = range(len(df))

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

    print("refine task done!")
    print("--------save task is running--------")
    # database -> schema -> table 순으로 인자 전달
    database = postgreSQL('backend','datawarehouse','Event')
    database.save_data(df)
    print("save task done!")

# if __name__ == "__main__":
#     df = get_data()
#     refine_data(df)