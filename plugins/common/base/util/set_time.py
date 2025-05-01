def get_month_dates(year:int, month:int) -> list:
    import datetime
    import calendar
    """
    전달의 데이터를 수집하기 위해 `최초 1회` 사용되는 함수
    get_last_initial_data_set 모듈에서 사용된다.
    args:
        year: int
            - 현재 년도를 인자로 받는다
        month : int
            - 피처를 쌓기 시작한 월을 적는다
    
    return : 
        list
            - 년-월 의 모든 `일`자를 담은 리스트를 리턴
    """
    # 해당 월의 마지막 날 계산
    last_day = calendar.monthrange(year, month)[1]
    
    # 날짜 리스트 생성
    return [datetime.date(year, month, day).strftime("%Y%m%d") for day in range(1, last_day + 1)]

def get_last_year_months(current_year) -> list:
    """
    작년의 월별 데이터를 수집하기 위해 `최초 1회` 사용되는 함수
    get_subway_initial_data_set 모듈에서 사용된다.
    args:
        current_year: int
            - 현재 년도를 인자로 받는다
    return : 
        list
            - 예를들어 202501 형태의 값을 갖는 이스트를 리턴한다.
    """
    last_year = current_year - 1
    return [last_year * 100 + month for month in range(1, 13)]