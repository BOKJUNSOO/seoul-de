from sqlalchemy import create_engine
from sqlalchemy import String ,DateTime, Boolean, String, Float, Integer
import pandas as pd
import psycopg2

class postgreSQL():
    def __init__(self,db_name,schema,table_name):
        self.db_user = 'airflow'
        self.db_password = 'airflow'
        self.db_host = 'postgres'
        self.db_port = '5432'
        self.db_name = db_name
        self.schema = schema
        self.table_name = table_name

    def save_data(self, df:pd.DataFrame):
        # self.create_database_if_not_exists()

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
    # def create_database_if_not_exists(self):
    #     # 기본 DB에 연결
    #     connection = psycopg2.connect(
    #         dbname='postgres',  # 기본 데이터베이스로 연결
    #         user=self.db_user,
    #         password=self.db_password,
    #         host=self.db_host,
    #         port=self.db_port
    #     )
    #     connection.autocommit = True
    #     cursor = connection.cursor()

    #     # 데이터베이스가 존재하는지 확인
    #     cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}'")
    #     exists = cursor.fetchone()
    #     if not exists:
    #         cursor.execute(f'CREATE DATABASE {self.db_name}')
    #         print(f" Database {self.db_name} created successfully!")
    #     else:
    #         print(f" Database {self.db_name} already exists.")
        
    #     cursor.close()
    #     connection.close()
        
