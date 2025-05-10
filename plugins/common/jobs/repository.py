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
        import psycopg2
        print("[INFO] - now checking database..")
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
        print(f"[INFO] - connected database : {db}, search for this schema: {schema}")

        # schema list
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemata = [row[0] for row in cur.fetchall()]
        print("[INFO] - this is my schema :", schemata)
        # check 'datawarehouse' schema
        if self.schema not in schemata:
            raise RuntimeError(f"[EXCEPTION] - {self.schema} is not set yet check README file and init database..")
        
        # st search_path
        cur.execute(f"SET search_path TO {self.schema};")
        print(f"[INFO] - SET search_parth to {self.schema}")

        # search table
        cur.execute("""
              SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = 'datawarehouse';
            """)
        
        tables = [row[0] for row in cur.fetchall()]
        print(f"[INFO] {self.schema} `s table list :", tables)
        
        # 검사한 테이블(evnet 테이블)이 존재하지 않는 경우
        if table not in tables:
            print(f"[INFO] - {table} dose not exist.")
            print(f"[INFO] - CREATE {table} ..!")
            print(f'[INFO] - xcom_push - key : key, value : init')
            ti.xcom_push(key="key",value="init")
        
        if table in tables:
            print(f"[INFO] - {table} is already exist.")
            print(f"[INFO] - CREATE sync ..!")
            print(f'[INFO] - xcom_push - key : key, value : sync')
            ti.xcom_push(key="key",value="sync")

    # getter
    def read_table(self,**kwargs):
        import psycopg2
        import pandas as pd
        print("[INFO] - now reading database..")
        
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
            print(f"[INFO] - connected host : {self.db_host}")

            # search db, schema
            cur.execute("SELECT current_database(), current_schema();")
            db, schema = cur.fetchone()
            print(f"[INFO] - connected database : {db}, search for this schema: {schema}")

            # schema list
            cur.execute("SELECT schema_name FROM information_schema.schemata;")
            schemata = [row[0] for row in cur.fetchall()]
            print("[INFO] - this is my schema :", schemata)

            # check 'datawarehouse' schema
            if self.schema not in schemata:
                raise RuntimeError(f"[EXCEPTION] - {self.schema} is not set yet check README file and init database..")

            # st search_path
            cur.execute("SET search_path TO datawarehouse;")
            print(f"[INFO] - SET search_parth to {self.schema}")

            # search table
            cur.execute("""
              SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = 'datawarehouse';
            """)
            tables = [row[0] for row in cur.fetchall()]
            print(f"[INFO] - {self.schema} `s table list :", tables)

            if table not in tables:
                raise RuntimeError(f"[EXCEPTION] - {table} is delete in airflow runtime.")
            
            # table 객체 저장
            #colnames = [desc[0] for desc in cur.description]
            df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
            print(f"[INFO] - success for read {table}!")
            
            
            # 모델에 전달하기 위해 데이터 저장
            ti = kwargs['ti']
            ti.xcom_push(key=f"{table}",value=df)
            ti.xcom_push(key="row_number",value=len(df))
            print(f'[INFO] - xcom_push - key : {table}, value : dataframe')
            print(f'[INFO] - xcom_push - key : row_number, value : {len(df)}')

        except psycopg2.Error as e:
            print(f"[EXCEPTION] - psycopg2 error: {e}")
            raise
        except Exception as ex:
            print(f"[EXCEPTION] - runtime error: {ex}")
        finally:
            if 'cur' in locals():
                print("[INFO] - finally close executer")
                cur.close()
            if 'conn' in locals():
                print("[INFO] - finally close connection")
                conn.close()
        

    # setter
    def save_to_event_table(self,**kwargs):
        from sqlalchemy import create_engine
        from sqlalchemy import String ,DateTime, Boolean,Float, Text , BigInteger
        print("[INFO] - now save to event table..")
        
        ti = kwargs['ti']
        df = ti.xcom_pull(key='to_save_data')
        print(f'[INFO] - xcom_pull - key : to_save_data, value : dataframe')
        
        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        print("[INFO] - create database engine..")
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
            dtype={
                'event_id':BigInteger,
                'title': String,
                'category_name': String,
                'gu': String,
                'location':String,
                'start_date':DateTime,
                'end_date':DateTime,
                'fee':String,
                'is_free':Boolean,
                'latitude':Float,
                'longitude':Float,
                'hompage':String,
                'image_url':Text,
                'detail_url':Text,
                'target_user':String,
                'event_description':Text
            }
        )
        print("[INFO] - save task is done ! check your RDBMS..")
        
    def save_to_subway_table(self,**kwargs):
        from sqlalchemy import create_engine
        from sqlalchemy import String ,Float
        print("[INFO] - now save to subway master table..")
        
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')
        print(f'[INFO] - xcom_pull - key : refine_dataframe, value : dataframe')
    
        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        print("[INFO] - create database engine..")
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
        
        print("[INFO] - save task is done ! check your RDBMS..")
    
    def save_to_subwayMontly_table(self,**kwargs):
        from sqlalchemy import create_engine
        from sqlalchemy import String ,Integer
        
        print("[INFO] - now save to subway montly table..")
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')
        print(f'[INFO] - xcom_pull - key : refine_dataframe, value : dataframe')
        
        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        print("[INFO] - create database engine..")

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
        print("[INFO] - save task is done ! check your RDBMS..")

    def save_to_subwayDaily_table(self,**kwargs):
        from sqlalchemy import create_engine
        from sqlalchemy import String ,Integer
        
        print("[INFO] - now save to subway daily table..")
        
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')
        print(f'[INFO] - xcom_pull - key : refine_dataframe, value : dataframe')
    

        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        print("[INFO] - create database engine..")

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
        print("[INFO] - save task is done ! check your RDBMS..")

    def save_to_hourly_predict(self,**kwargs):
        from sqlalchemy import create_engine
        from sqlalchemy import String ,DateTime,Integer
        print("[INFO] - now save to subway hourly prediction table..")
        
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe',task_ids='refine_data')
        print(f'[INFO] - xcom_pull key : refine_dataframe, value : dataframe')

        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        print("[INFO] - create database engine..")
        
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
            dtype={
                'row_number':Integer,
                'name':String,
                'service_date':DateTime,
                'hour':Integer,
                'predicted_get_on_d':Integer
            }
        )
        print("[INFO] - save task is done ! check your RDBMS..")
    
    def save_to_weather_table(self,**kwargs):
        from sqlalchemy import create_engine
        from sqlalchemy import String ,DateTime,String,BigInteger, Integer
        
        print("[INFO] - now save to weather table..")
        
        ti = kwargs['ti']
        df = ti.xcom_pull(key='refine_dataframe')
        print(f'[INFO] - xcom_pull - key : refine_dataframe, value : dataframe')

        engine = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        print("[INFO] - create database engine..")

        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
            dtype={
                'id':BigInteger,
                'fcst_date':DateTime,
                'time':String,
                'gu':String,
                'weather_status':String,
                'temperture':Integer
            }
        )
        print("[INFO] - save task is done ! check your RDBMS..")