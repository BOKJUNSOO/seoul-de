import datetime
import calendar

def get_month_dates(year:int, month:int) -> list:
    """
    연도와 달을 입력하면
    
    해당 달의 일자를 %Y%m%d형태로 list에 담아 리턴하는 함수
    """
    # 해당 월의 마지막 날 계산
    last_day = calendar.monthrange(year, month)[1]
    
    # 날짜 리스트 생성
    return [datetime.date(year, month, day).strftime("%Y%m%d") for day in range(1, last_day + 1)]