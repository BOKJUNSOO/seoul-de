from sqlalchemy import create_engine
from sqlalchemy import String ,DateTime, Boolean, String, Float, Integer
import pandas as pd

class postgreSQL():
    def __init__(self,db_name,schema,table_name):
        self.db_user = 'airflow'
        self.db_password = 'airflow'
        self.db_host = 'postgres'
        self.db_port = '5432'
        self.db_name = db_name
        self.schema = schema
        self.table_name = table_name

    def save_to_event_table(self,**kwargs):
        print("--------save task is running--------")
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')
        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
            dtype={
                'event_id':Integer,
                'title': String,
                'category_id': String,
                'gu': String,
                'location':String,
                'start_date':DateTime,
                'end_date':DateTime,
                'fee':String,
                'is_free':Boolean,
                'latitude':Float,
                'longtitude':Float,
                'hompage':String,
                'image_url':String,
                'target_user':String,
                'event_description':String
            }
        )
        print("save task done!")
        
    def save_to_subway_table(self,**kwargs):
        print("--------save task is running--------")
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')
        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
            dtype={
                'station_id':String,
                'name':String,
                'line':String,
                'latitude':Float,
                'longitude':Float
            }
        )
        print("save task done!")
