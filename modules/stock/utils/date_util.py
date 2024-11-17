from datetime import datetime

def get_dated_filename(base_name):
    today = datetime.now().strftime('%Y%m%d')
    return f"{base_name}_{today}.csv"

def get_date():
    return datetime.now().strftime('%Y%m%d')

def is_weekday_and_not_holiday(execution_date):
    """주어진 실행 날짜가 평일이고 공휴일이 아닌지 확인하는 함수."""
    date = execution_date.date()
    return date.weekday() < 5