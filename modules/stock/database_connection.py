import os
from sqlalchemy import create_engine

def get_database_connection():
    try:
        connection_string = os.environ.get('MYSQL_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("데이터 베이스 연결정보가 없습니다.")
        
        engine = create_engine(connection_string)
        print("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
        return engine
    except Exception as e:
        print(f"데이터베이스 연결 오류: {e}")
        return None