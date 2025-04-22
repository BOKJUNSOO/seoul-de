import pandas as pd
import pickle
import base64

def predict_all_for_date(**kwargs)->pd.DataFrame:
    """
    실제 존재하는 line-name 조합에 대해 예측 수행
    Parameters (get by task instance):
        date_str (str): 예측 날짜
        encoders (dict): 학습 시 사용한 LabelEncoder 딕셔너리
        model: 학습된 모델
        df (DataFrame): 학습에 사용된 line-name 조합 (디코딩 상태)
    
    Returns:
        pd.DataFrame: 예측 결과 포함된 DataFrame
    """
    # date timestamp!
    date_str = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d")

    ti = kwargs['ti']
    df = ti.xcom_pull(key='trained_dataset')
    
    pickled_encoders = ti.xcom_pull(key='ML_encoders')
    encoders = pickle.loads(base64.b64decode(pickled_encoders.encode()))
    print(encoders['line'].classes_)
    # refact model
    model_64 = ti.xcom_pull(key='ML_model')
    model = pickle.loads(base64.b64decode(model_64))
        
    decoded_df = decode_unique_line_name(df, encoders)

    # add datetime
    with_date = attach_date_features(decoded_df, date_str)

    # encoding for new predict
    encoded_input = encode_for_model(with_date.copy(), encoders)

    # daily output prediction
    predictions = model.predict(encoded_input)
    with_date['predicted_get_on_d'] = predictions.astype(int)

    # daily output
    result = with_date[['date', 'line_str', 'name_str', 'predicted_get_on_d']]
    result = result.rename(columns={'line_str': 'line', 'name_str': 'name'})

    ti.xcom_push(key='daily_dataframe', value=result)

def decode_unique_line_name(df, encoders_info):
    """
    인코딩된 조합을 디코딩하여 원본 값으로 복원
    encoders_info는 'classes_' 리스트 정보만 포함
    """
    # LabelEncoder 객체 복원
    # encoders = {}
    # for col, classes in encoders_info.items():
    #     le = LabelEncoder()
    #     le.classes_ = np.array(classes)
    #     encoders[col] = le
    
    # 고유한 인코딩된 조합 추출
    unique_df = df[['line', 'name']].drop_duplicates().copy()
    print("encoder line",encoders_info['line'])
    # 디코딩
    unique_df['line_str'] = encoders_info['line'].inverse_transform(unique_df['line'])
    unique_df['name_str'] = encoders_info['name'].inverse_transform(unique_df['name'])
    return unique_df[['line', 'name', 'line_str', 'name_str']]

def attach_date_features(df, date_str):
    date = pd.to_datetime(date_str)
    day_of_week = date.weekday()
    is_weekend = 1 if day_of_week in [5, 6] else 0

    df['date'] = date_str
    df['day_of_week'] = day_of_week
    df['is_weekend'] = is_weekend

    return df

def encode_for_model(df, encoders):
    df['line'] = encoders['line'].transform(df['line_str'])
    df['name'] = encoders['name'].transform(df['name_str'])
    df['day_of_week'] = encoders['day_of_week'].transform(df['day_of_week'])
    df['is_weekend'] = encoders['is_weekend'].transform(df['is_weekend'])
    return df[['line', 'name', 'day_of_week', 'is_weekend']]
