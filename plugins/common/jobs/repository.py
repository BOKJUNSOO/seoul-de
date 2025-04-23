from sqlalchemy import create_engine
from sqlalchemy import String ,DateTime, Boolean, String, Float, Integer, Text
import psycopg2
import pandas as pd

class postgreSQL():
    """
    실제 Database postgresl에 접근하는 class

    read : connection 지정
    write : 데이터 타입 및 스키마 지정
    """
    def __init__(self,db_name,schema,table_name):
        # production 환경에서는 db_user, password ,ip 를 Variable로 처리
        self.db_user = 'airflow'
        self.db_password = 'airflow'
        self.db_host = 'postgres'
        self.db_port = '5432'
        self.db_name = db_name
        self.schema = schema
        self.table_name = table_name
    
    # check_database - branchOperator callable function
    def check_table(self,**kwargs):
        print("--------check database is running--------")
        ti = kwargs['ti']
        conn_params = {
            "host": self.db_host,
            "port": self.db_port,
            "dbname":self.db_name,
            "user":self.db_user,
            "password":self.db_password
        }
        table = self.table_name # target_table

        # connection 
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # search db, schema
        cur.execute("SELECT current_database(), current_schema();")
        db, schema = cur.fetchone()
        print(f"연결된 DB: {db}, search_path 스키마: {schema}")

        # schema list
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemata = [row[0] for row in cur.fetchall()]
        print("스키마:", schemata)
        # check 'datawarehouse' schema
        if self.schema not in schemata:
            raise RuntimeError(f"{self.schema} 스키마를 설정해주세요.")
        
        # st search_path
        cur.execute("SET search_path TO datawarehouse;")
        print("search_path --> datawarehouse 설정.")

        # search table
        cur.execute("""
              SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = 'datawarehouse';
            """)
        
        tables = [row[0] for row in cur.fetchall()]
        print("datawarehouse 스키마의 테이블들:", tables)
        
        if table not in tables:
            print(f"{table}이 존재하지 않습니다.")
            ti.xcom_push(key="key",value="save_to_event_")
            next_task="make_event_data_"
            
            return next_task
        else:
            print(f"{table}이 존재합니다. sync table을 생성합니다.")
            ti.xcom_push(key="key",value="save_to_sync_")
            next_task="make_sync_data_"
            return next_task

    # getter
    def read_table(self,**kwargs):
        print("--------read task is running--------")
        
        # needed params for read database
        conn_params = {
            "host": self.db_host,
            "port": self.db_port,
            "dbname":self.db_name,
            "user":self.db_user,
            "password":self.db_password
        }
        try:
            # 읽어올 테이블 이름
            table = self.table_name

            # connect
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor()
            print(f"{self.db_host}에 연결되었습니다.")

            # search db, schema
            cur.execute("SELECT current_database(), current_schema();")
            db, schema = cur.fetchone()
            print(f"연결된 DB: {db}, search_path 스키마: {schema}")

            # schema list
            cur.execute("SELECT schema_name FROM information_schema.schemata;")
            schemata = [row[0] for row in cur.fetchall()]
            print("스키마:", schemata)

            # check 'datawarehouse' schema
            if self.schema not in schemata:
                raise RuntimeError(f"{self.schema} 스키마를 찾을 수 없습니다.")

            # st search_path
            cur.execute("SET search_path TO datawarehouse;")
            print("search_path --> datawarehouse 설정.")

            # search table
            cur.execute("""
              SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = 'datawarehouse';
            """)
            tables = [row[0] for row in cur.fetchall()]
            print("datawarehouse 스키마의 테이블들:", tables)

            if table not in tables:
                raise RuntimeError(f"{table} 테이블을 찾을 수 없습니다.")

            # table 객체 저장
            #colnames = [desc[0] for desc in cur.description]
            df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
            print(f"success for read {table}db!")
            
            
            # 모델에 전달하기 위해 데이터 저장
            ti = kwargs['ti']
            ti.xcom_push(key=f"{table}",value=df)
            ti.xcom_push(key="row_number",value=len(df))

        except psycopg2.Error as e:
            print(f"psycopg2 error: {e}")
            raise
        except Exception as ex:
            print(f"runtime error: {ex}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()
        


    # setter
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
                'category_name': String,
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
                'event_description':Text
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
    
    def save_to_subwayMontly_table(self,**kwargs):
        print("--------save task is running--------")
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')
        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='append',
            index=False,
            dtype={
                'use_month':String,
                'station_id':String,
                'line':String,
                'hour':Integer,
                'get_on':Integer,
                'get_off':Integer,
                'total':Integer
            }
        )
        print("save task done!")

    def save_to_subwayDaily_table(self,**kwargs):
        print("--------save task is running--------")
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')

        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='append',
            index=False,
            dtype={
                'service_date':String,
                'line':String,
                'name':String,
                'get_on_d':Integer,
                'get_off_d':Integer
            }
        )
        print("save task done!")

    def save_to_hourly_predict(self,**kwargs):
        print("--------save task is running--------")
        ti = kwargs['ti']
        df = ti.xcom_pull(key='row_dataframe')

        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
            dtype={
                'row_number':Integer,
                'name':String,
                'line':String,
                'service_date':DateTime,
                'hour':Integer,
                'predicted_get_on_d':Integer
            }
        )
        print("save task done!")
    
    # for test!
    def save_to_daily_predict(self,**kwargs):
        print("--------save task is running--------")
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='make_daily_prediction',key='daily_dataframe')

        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
        )
        print("save task done!")