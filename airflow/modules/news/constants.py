from enum import Enum
from typing import List
from pydantic_settings import BaseSettings

class XComKeys(str, Enum):
    """XCom 키 정의"""
    FETCH_RESULT = 'fetch_result'
    EXTRACT_RESULT = 'extract_result'
    SUMMARY_RESULT = 'summary_result'

class Settings(BaseSettings):
    # Database settings
    DATABASE_HOST: str
    DATABASE_PORT: str
    DATABASE_USERNAME: str
    DATABASE_PASSWORD: str
    DATABASE_NEWS_SCHEMA: str

    # OpenAI settings
    OPENAI_API_KEY: str

    # Redis settings
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: str
    REDIS_DB: int

    # News settings
    KEYWORD_LIST: List[str] = [
        "국내주식",
        "해외주식",
        "크립토",
        "ETF",
        "정치",
        "경제",
        "환율",
        "부동산",
        "주가지수",
    ]

    PRESS_LIST: List[str] = [
        "한국경제",
        "서울경제",
        "매일경제",
    ]

    class Config:
        env_file = ".env"

settings = Settings()