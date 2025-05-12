import pandas as pd
from dateutil.relativedelta import relativedelta

def make_hourly_dataframe(**kwargs):
    print("[INFO] - make hourly_data frame with calculate")
    ti = kwargs['ti']
    print("[INFO] - xcom_pull - key: daily_dataframe")
    print("[INFO] - xcom_pull - key: subway_data_prev_year")
    df_daily_pred = ti.xcom_pull(task_ids='make_daily_prediction' ,key='daily_dataframe')
    df_prev_year = ti.xcom_pull(task_ids='read_year_day',key='subway_data_prev_year')
    df_daily_pred.info()
    df_prev_year.info()
    
    # calculate last yearly data for ratio
    df_grouped = df_prev_year.groupby(['use_month', 'name','hour'])['total'].sum().reset_index()
    df_station_monthly_total = df_grouped.groupby(['use_month', 'name'])['total'].sum().reset_index() # grouping for 2 phase

    df_grouped = df_grouped.merge(df_station_monthly_total, on=['use_month', 'name'], suffixes=('', '_monthly_total'))
    df_grouped['usage_ratio'] = df_grouped['total'] / df_grouped['total_monthly_total']

    # preprocessed daily prediction data (select for join column)
    df_daily_pred['use_month'] = pd.to_datetime(df_daily_pred['date']).dt.strftime("%Y%m")
    df_daily_pred['date'] = pd.to_datetime(df_daily_pred['date'])

    df_daily_pred['use_month_minus_1y'] = df_daily_pred['date'].apply(lambda x: x - relativedelta(years=1))
    df_daily_pred['use_month'] = df_daily_pred['use_month_minus_1y'].dt.strftime("%Y%m")
    df_daily_pred = df_daily_pred.drop(columns='use_month_minus_1y')

    # target month
    target = df_daily_pred['use_month'].unique()[0]
    filtered_df = df_grouped[df_grouped['use_month']==target].reset_index()

    # merge
    merged_df = pd.merge(filtered_df,df_daily_pred)
    # calculate hourly prediction
    merged_df['predicted_total'] = merged_df['usage_ratio'] * merged_df['predicted_get_on_d']
    merged_df['predicted_total'] = merged_df['predicted_total'].astype(int)

    select_col = ['line','name','date','hour','predicted_total']
    merged_df = merged_df[select_col]
    print("[INFO] - MLops task is done.")
    ti.xcom_push(key='row_dataframe',value=merged_df)
    print("[INFO] - xcom_push - key: row_dataframe")