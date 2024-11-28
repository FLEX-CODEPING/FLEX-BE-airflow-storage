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
            if keyword == "주가지수":
                keyword = "주가%20지수"
                result = executor.submit(
                    fetcher.fetch_news, 
                    period=1, 
                    keyword=keyword, 
                    press=press
                ).result()
            else:
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