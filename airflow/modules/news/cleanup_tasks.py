import os
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
from airflow.models import TaskInstance
from modules.news.news_fetcher import NewsFetcher
from modules.news.content_extractor import ContentExtractor
from modules.news.constants import XComKeys as XCOM_KEYS
from modules.news.news_database_connection import get_database_connection
from sqlalchemy import text
import shutil

logger = logging.getLogger(__name__)

def cleanup_old_data() -> None:
    """7일 이상 된 뉴스 데이터 파일과 DB 레코드를 삭제하는 task"""
    try:
        # 7일 전 날짜 계산
        cutoff_datetime = datetime.now() - timedelta(days=7)
        
        # 파일 정리
        cleanup_files(cutoff_datetime)
        
        # DB 정리
        cleanup_database(cutoff_datetime)
        
    except Exception as e:
        logger.error(f"데이터 정리 중 오류 발생: {str(e)}")
        raise

def cleanup_files(cutoff_datetime: datetime) -> None:
    """7일 이상 된 CSV 파일 삭제"""
    try:
        # 기본 경로 설정
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        data_dir = os.path.join(base_path, 'data', 'news')
        
        # 키워드별 디렉토리 순회
        for keyword_dir in os.listdir(data_dir):
            keyword_path = os.path.join(data_dir, keyword_dir)
            if not os.path.isdir(keyword_path):
                continue
                
            # 날짜별 디렉토리 순회
            for date_dir in os.listdir(keyword_path):
                try:
                    # 날짜 디렉토리명을 datetime으로 변환
                    dir_date = datetime.strptime(date_dir, '%Y%m%d')
                    dir_path = os.path.join(keyword_path, date_dir)
                    
                    # 7일 이상 된 디렉토리 삭제
                    if dir_date < cutoff_datetime:
                        shutil.rmtree(dir_path)
                        logger.info(f"삭제된 디렉토리: {dir_path}")
                        
                except ValueError:
                    logger.warning(f"잘못된 날짜 형식의 디렉토리: {date_dir}")
                    continue
                    
    except Exception as e:
        logger.error(f"파일 정리 중 오류 발생: {str(e)}")
        raise

def cleanup_database(cutoff_datetime: datetime) -> None:
    """7일 이상 된 DB 레코드 삭제"""
    try:
        engine = get_database_connection()
        if not engine:
            raise ValueError("데이터베이스 연결 실패")
            
        with engine.connect() as connection:
            # 7일 이상 된 레코드 삭제
            delete_query = text("""
                DELETE FROM news_articles 
                WHERE published_date < :cutoff_datetime
            """)
            
            with connection.begin():
                result = connection.execute(delete_query, {"cutoff_datetime": cutoff_datetime})
            
            logger.info(f"삭제된 DB 레코드 수: {result.rowcount}")
            
    except Exception as e:
        logger.error(f"데이터베이스 정리 중 오류 발생: {str(e)}")
        raise