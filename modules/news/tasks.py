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

def fetch_news(keyword: str, press: str, **context) -> Dict:
    """뉴스 수집 태스크"""
    try:
        fetcher = NewsFetcher()
        # ThreadPoolExecutor를 사용한 병렬 처리
        with ThreadPoolExecutor(max_workers=3) as executor:
            result = executor.submit(
                fetcher.fetch_news, 
                period=1, 
                keyword=keyword, 
                press=press
            ).result()

        task_instance: TaskInstance = context['task_instance']
        task_instance.xcom_push(key=XCOM_KEYS.FETCH_RESULT, value=result)

        return result
    except Exception as e:
        logger.error(f"뉴스 수집에 실패한 지점은 {press}-{keyword}: {str(e)}")
        raise

def extract_contents(**context) -> List[Dict]:
    """컨텐츠 추출 태스크"""
    try:
        task_instance: TaskInstance = context['task_instance']
        press: str = context['press']
        keyword: str = context['keyword']
        
        fetch_result = task_instance.xcom_pull(
            task_ids=f'news_process_{keyword}.fetch_news_{press}',
            key=XCOM_KEYS.FETCH_RESULT
        )
        
        if not fetch_result:
            logger.error(f"수집 결과 없음: {press} - {keyword}")
            return []

        extractor = ContentExtractor()
        
        logger.info(f"본문 추출 시도: {press} - {keyword}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for article in fetch_result['articles']:
                future = executor.submit(
                    extractor.extract_content,
                    article['url']
                )
                futures.append((article, future))
            
            # 결과 수집
            results = []
            for article, future in futures:
                try:
                    content = future.result()
                    results.append({
                        'url': article['url'],
                        'title': article['title'],
                        'content': content,
                        'published_date': article['published_date'],
                        'press': press,
                        'keyword': keyword
                    })
                except Exception as e:
                    logger.warning(f"Failed to extract content from {article['url']}: {str(e)}")
                    continue

        task_instance.xcom_push(key=XCOM_KEYS.EXTRACT_RESULT, value=results)    
        return results
    except Exception as e:
        logger.error(f"본문 추출이 실패한 지점은 {press}-{keyword}: {str(e)}")
        raise

def save_to_rdb_csv(**context) -> str:
    """추출된 컨텐츠를 RDB와 CSV 파일에 저장"""
    task_instance = context['task_instance']
    press = context['press']
    keyword = context['keyword']
    
    # 이전 태스크에서 결과 가져오기
    extracted_contents = task_instance.xcom_pull(
        task_ids=f'news_process_{keyword}.extract_content_{press}',
        key=XCOM_KEYS.EXTRACT_RESULT
    )
    
    if not extracted_contents:
        logger.error(f"저장할 컨텐츠 없음: {press} - {keyword}")
        return ''
    
    date_str = datetime.now().strftime('%Y%m%d')

    df = pd.DataFrame(extracted_contents)
    logger.info(f"df: {df}")

    engine = get_database_connection()
    
    if engine is None:
        raise ValueError("DB 연결 실패")
    try:
        df.to_sql(
            name='news_articles',
            con=engine,
            if_exists='append',
            index=False
        )

        logger.info(f"DB 저장 성공: {date_str}, {press}, {keyword}")
    except Exception as e:
        logger.error(f"DB에 컨텐츠 저장 실패: {str(e)}")
    
    try:
        # CSV 파일명 생성
        filename = f"news_{press}_{keyword}_{date_str}.csv"
        
        # 저장 경로 생성
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        save_dir = os.path.join(base_path, 'data', 'news', keyword, date_str)
        os.makedirs(save_dir, exist_ok=True)
        
        filepath = os.path.join(save_dir, filename)
        
        # DataFrame 생성 및 저장
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        logger.info(f"Saved {len(extracted_contents)} articles to {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"컨텐츠 저장에 실패한 지점은 {press}-{keyword}: {str(e)}")
        raise

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