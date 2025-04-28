# event 정제 함수
def corret_lat_lot(row):
    lat = row['LAT']
    lot = row['LOT']

    if lat > lot:
        row['lat'], row['lot'] = lot, lat
    print("lat과 lot의 값을 수정합니다")
    return row