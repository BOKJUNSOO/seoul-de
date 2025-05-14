from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
import pandas as pd
import pickle
import base64

def modeling(**kwargs):
    """
    ti로부터 데이터셋을 받고, 모델 학습후 
    데이터셋, 모델, 인코딩 정보를 ti로 push 하는 python callable 함수
    """
    ti = kwargs['ti']
    df_daily = ti.xcom_pull(key='subway_data_prev_month')
    df_new_daily = ti.xcom_pull(key='subway_data_daily')
    print("[INFO] - daily modeling task is now running..(total usage)")
    print("[INFO] - this is preprocessing")
    print("[INFO] - xcom_pull - key : subway_data_prev_month")
    print("[INFO] - xcom_pull - key : subway_data_daily")

    df = pd.concat([df_daily, df_new_daily], ignore_index=True).drop_duplicates(
        subset=['service_date','line','name']
    )

    df['service_date'] = pd.to_datetime(df['service_date'])
    df['day_of_week'] = df['service_date'].dt.weekday
    df['is_weekend'] = df['day_of_week'].isin([5,6]).astype(int)

    df = df.drop(columns=['get_off_d','service_date'])

    # target coloum for encoding
    encoding_col = ['line', 'name', 'get_on_d', 'day_of_week', 'is_weekend']

    # save label
    encoders = {}

    # encode
    for col in encoding_col:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
        encoders[col] = le
    pickled_encoders = base64.b64encode(pickle.dumps(encoders)).decode()
    
    
    train_x = df.drop(columns=['get_on_d'])
    train_y = df['get_on_d']

    train_x = df.drop(columns=['get_on_d'])
    train_y = df['get_on_d']
    X_TRAIN,X_TEST , Y_TRAIN,Y_TEST = train_test_split(train_x,train_y, test_size=0.3,random_state=2025)
    model = RandomForestRegressor()
    model.fit(X_TRAIN,Y_TRAIN)

    model_serialized = pickle.dumps(model)
    model_b64 = base64.b64encode(model_serialized).decode('utf-8')

    y_pred = model.predict(X_TEST)
    print("[INFO] - today`s model score :" + str(r2_score(Y_TEST,y_pred)) + "/1")
    
    ti.xcom_push(key='trained_dataset',value=df)
    print("[INFO] - xcom_push - key : trained_dataset")

    ti.xcom_push(key='ML_encoders', value=pickled_encoders)
    print("[INFO] - xcom_push - key : ML_encoders")

    ti.xcom_push(key='ML_model',value=model_b64)
    print("[INFO] - xcom_push - key : ML_model")

    
