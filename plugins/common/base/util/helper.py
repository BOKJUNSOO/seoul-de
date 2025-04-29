# event 정제 함수

def corret_lat_lot(row):
    lat = row['LAT']
    lot = row['LOT']

    if lat > lot:
        row['LAT'], row['LOT'] = lot, lat
        print("lat과 lot의 값을 수정합니다")
    return row

# subway 관련 daily 데이터 정제함수
# `(` 뒤의 값을 날리고, "역을 삭제한후 다시 붙히는 형식"
def refine_subway_name_data(df):
    df['name'] = df['name'].str.replace(r"\s*\(.*\)", "", regex=True)
    df['name'] = df['name'].apply(lambda x: x if x.endswith('역') else x + '역')
    return df