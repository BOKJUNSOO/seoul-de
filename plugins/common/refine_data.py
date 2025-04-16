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

    columns = ['TITLE','CODENAME','GUNAME','PLACE','STRTDATE','END_DATE','USE_FEE','BOOL_FEE','LAT','LOT','HMPG_ADDR','MAIN_IMG','ORG_LINK','USE_TRGT']
    df = df[columns]
    # 컬럼명 변경
    df = df.rename(columns={
        'TITLE':'title',
        'CODENAME':'category',
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
        'USE_TRGT':'target_user'
    })

    print("refine task done!")
    print("--------save task is running--------")
    database = postgreSQL('backend')
    database.save_data(df)
    print("save task done!")

# if __name__ == "__main__":
#     df = get_data()
#     refine_data(df)