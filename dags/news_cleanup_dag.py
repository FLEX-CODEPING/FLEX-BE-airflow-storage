from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import sys
import os 
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from modules.news.tasks import cleanup_old_data

with DAG(
    dag_id="news_cleanup",
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
    },
    concurrency=10,
    max_active_runs=1,
    schedule_interval="15 6 * * *", # 매일 KST 06시 15분에 실행
    start_date=datetime(2024, 11, 27),
    tags=["news", "crawl", "pipeline"],
) as dag:
    start = DummyOperator(task_id="start")

    cleanup = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data
    )
    
    end = DummyOperator(task_id="end")

    start >> cleanup >> end