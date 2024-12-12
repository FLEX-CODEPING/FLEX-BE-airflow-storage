from sqlalchemy import create_engine
from modules.news.constants import settings


def get_database_connection():
    try:
        connection_info = f"mysql+pymysql://{settings.DATABASE_USERNAME}:{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{settings.DATABASE_NEWS_SCHEMA}"
        if not connection_info:
            raise ValueError("데이터 베이스 연결정보가 없습니다.")

        engine = create_engine(connection_info)
        print("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
        return engine
    except Exception as e:
        print(f"데이터베이스 연결 오류: {e}")
        return None