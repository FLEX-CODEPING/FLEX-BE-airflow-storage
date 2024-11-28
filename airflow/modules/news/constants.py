from enum import Enum

class XComKeys(str, Enum):
    """XCom 키 정의"""
    FETCH_RESULT = 'fetch_result'
    EXTRACT_RESULT = 'extract_result'

PRESS_LIST = ["한국경제", "서울경제", "매일경제"]
KEYWORD_LIST = ["국내주식", "해외주식", "크립토", "ETF", "정치", "경제", "환율", "부동산", "주가지수"] 