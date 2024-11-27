import redis
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost') 
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379)) 
REDIS_DB = int(os.getenv('REDIS_DB', 4))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

def get_redis_connection():
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
    try:
        r.ping()  
        print(f"레디스 연결 성공: {REDIS_HOST}:{REDIS_PORT}, DB {REDIS_DB}")
        return r
    except redis.ConnectionError as e:
        print(f"레디스 연결 실패: {e}")
        return None 
