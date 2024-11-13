from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from modules.stock.stock_data_collection import (
    get_ticker_list,
    collect_ohlcv_data,
    collect_market_cap_data,
    collect_fundamental_data,
)
from modules.stock.database_connection import load_csv_to_mysql

local_tz = pendulum.timezone("Asia/Seoul")
now = pendulum.now("Asia/Seoul") 
today_date1 = now.strftime('%Y%m%d')
start_date = pendulum.datetime(2024, 11, 1, tz="Asia/Seoul")

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
    collect_ohlcv_data(kor_ticker_list, kwargs['start_date'], kwargs['today_date1'])

def collect_market_cap_with_tickers(**kwargs):
    kor_ticker_list = kwargs['ti'].xcom_pull(key='kor_ticker_list', task_ids='get_ticker_list')
    collect_market_cap_data(kor_ticker_list, kwargs['start_date'], kwargs['today_date1'])

def collect_fundamental_with_tickers(**kwargs):
    kor_ticker_list = kwargs['ti'].xcom_pull(key='kor_ticker_list', task_ids='get_ticker_list')
    collect_fundamental_data(kor_ticker_list, kwargs['start_date'], kwargs['today_date1'])

with DAG(
    dag_id='data_collection_and_loading_dag',
    default_args=default_args,
    schedule_interval='0 6 * * *',  
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
        op_kwargs={'start_date': start_date, 'today_date1': today_date1},
        provide_context=True,
        dag=dag,
    )

    collect_market_cap_task = PythonOperator(
        task_id='collect_market_cap_data',
        python_callable=collect_market_cap_with_tickers,
        op_kwargs={'start_date': start_date, 'today_date1': today_date1},
        provide_context=True,
        dag=dag,
    )

    collect_fundamental_task = PythonOperator(
        task_id='collect_fundamental_data',
        python_callable=collect_fundamental_with_tickers,
        op_kwargs={'start_date': start_date, 'today_date1': today_date1},
        provide_context=True,
        dag=dag,
    )

    load_to_mysql_task = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql,
        op_kwargs={'start_date': start_date, 'today_date1': today_date1},
        provide_context=True,
        dag=dag,
    )

    get_tickers_task >> [collect_ohlcv_task, collect_market_cap_task, collect_fundamental_task] >> load_to_mysql_task