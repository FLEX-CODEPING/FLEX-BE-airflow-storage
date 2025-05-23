from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain, cross_downstream
from airflow.utils.task_group import TaskGroup
import sys
import os 
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from modules.news.crawling_tasks import fetch_news, extract_contents, summarize_contents, save_to_rdb_csv
from modules.news.constants import settings


# DAG 정의
with DAG(
    dag_id="news_crawling",
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
    },
    concurrency=10,
    max_active_runs=1,
    schedule_interval="0 6 * * *", # 매일 KST 06시에 실행
    start_date=datetime(2024, 11, 1),
    tags=["news", "crawl", "pipeline"],
    catchup=False
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    keyword_groups = []

    # 키워드별로 뉴스 수집 파이프라인 생성
    for keyword in settings.KEYWORD_LIST:
        with TaskGroup(group_id=f'news_process_{keyword}') as keyword_group:
            # 각 단계별 태스크들 담을 리스트
            fetch_tasks = []
            extract_tasks = []
            summarize_tasks = []
            save_tasks = []

            # 언론사별로 뉴스 수집
            for press in settings.PRESS_LIST:
                fetch = PythonOperator(
                    task_id=f'fetch_news_{press}',
                    python_callable=fetch_news,
                    provide_context=True,
                    op_kwargs={
                        'keyword': keyword, 
                        'press': press
                    }
                )
                fetch_tasks.append(fetch)

            # 언론사별로 뉴스 컨텐츠 추출
            for press in settings.PRESS_LIST:
                extract = PythonOperator(
                    task_id=f'extract_content_{press}',
                    python_callable=extract_contents,
                    provide_context=True,
                    op_kwargs={
                        'press': press,
                        'keyword': keyword
                    }
                )
                extract_tasks.append(extract)

            # 언론사별로 키워드별 기사들 개별 요약
            for press in settings.PRESS_LIST:
                summarize = PythonOperator(
                    task_id=f'summarize_content_{press}',
                    python_callable=summarize_contents,
                    provide_context=True,
                    op_kwargs={
                        'press': press,
                        'keyword': keyword
                    }
                )
                summarize_tasks.append(summarize)

            # 언론사별로 CSV로 저장
            for press in settings.PRESS_LIST:
                save = PythonOperator(
                    task_id=f'save_content_{press}',
                    python_callable=save_to_rdb_csv,
                    provide_context=True,
                    op_kwargs={
                        'press': press,
                        'keyword': keyword
                    }
                )
                save_tasks.append(save)

                # 태스크 의존성 설정
                # fetch_tasks >> extract_tasks >> save_tasks
                cross_downstream(fetch_tasks, extract_tasks)
                cross_downstream(extract_tasks, summarize_tasks)
                cross_downstream(summarize_tasks, save_tasks)
                keyword_groups.append(keyword_group)
            
            # 전체 태스크그룹 의존성 설정
            start >> keyword_groups >> end