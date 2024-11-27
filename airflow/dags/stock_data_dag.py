from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from modules.stock.stock_data_collector import (
    get_ticker_list,
    collect_ohlcv_data,
    collect_market_cap_data
)
from modules.stock.stock_data_loader import load_market_cap_data, load_ohlcv_data

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'modules', 'stock', 'utils'))
from date_util import get_date

local_tz = pendulum.timezone("Asia/Seoul")
now = pendulum.now("Asia/Seoul") 
today_date = get_date()
start_date = now.subtract(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': start_date, 
    'retries': 0,
    'catchup': False
}

def get_tickers_and_return(**kwargs):
    kor_ticker_list_df = get_ticker_list()
    kor_ticker_list = kor_ticker_list_df['stockcode'].tolist()
    kwargs['ti'].xcom_push(key='kor_ticker_list', value=kor_ticker_list)
    return kor_ticker_list

def collect_ohlcv_with_tickers(**kwargs):
    kor_ticker_list = kwargs['ti'].xcom_pull(key='kor_ticker_list', task_ids='get_ticker_list')
    collect_ohlcv_data(kor_ticker_list)

def collect_market_cap_with_tickers(**kwargs):
    kor_ticker_list = kwargs['ti'].xcom_pull(key='kor_ticker_list', task_ids='get_ticker_list')
    collect_market_cap_data(kor_ticker_list)


# 오후 6시에 데이터 수집
with DAG(
    dag_id='stock_data_collection_dag',
    default_args=default_args,
    schedule_interval='0 18 * * *',  
    catchup=False,
    tags=['pykrx'],
) as dag:

    get_tickers_task = PythonOperator(
        task_id='get_ticker_list',
        python_callable=get_tickers_and_return,
        provide_context=True,
        dag=dag,
    )

    collect_ohlcv_task = PythonOperator(
        task_id='collect_ohlcv_data',
        python_callable=collect_ohlcv_with_tickers,
        op_kwargs={'start_date': start_date, 'today_date': today_date},
        provide_context=True,
        dag=dag,
    )

    collect_market_cap_task = PythonOperator(
        task_id='collect_market_cap_data',
        python_callable=collect_market_cap_with_tickers,
        op_kwargs={'start_date': start_date, 'today_date': today_date},
        provide_context=True,
        dag=dag,
    )

    load_ohlcv_to_mysql = PythonOperator(
        task_id='load_ohlcv_data',
        python_callable=load_ohlcv_data,
        op_kwargs={'start_date': start_date, 'today_date': today_date},
        provide_context=True,
        dag=dag,
    )

    load_market_cap_to_mysql = PythonOperator(
        task_id='load_market_cap_data',
        python_callable=load_market_cap_data,
        op_kwargs={'start_date': start_date, 'today_date': today_date},
        provide_context=True,
        dag=dag,
    )

    get_tickers_task >> [
        collect_ohlcv_task, 
        collect_market_cap_task
    ]

    collect_ohlcv_task >> load_ohlcv_to_mysql
    collect_market_cap_task >> load_market_cap_to_mysql
