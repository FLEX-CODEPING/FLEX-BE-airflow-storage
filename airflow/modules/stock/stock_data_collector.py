import pandas as pd
from pykrx import stock
from datetime import datetime
import sys
import os

from .utils.date_util import get_dated_filename, get_date

def save_to_csv(df, file_name, date):
    current_dir = os.path.dirname(__file__)
    stock_data_path = os.path.join(current_dir, '..', '..', 'data', 'stock', date)
    if not os.path.exists(stock_data_path):
        os.makedirs(stock_data_path)

    dated_file_name = get_dated_filename(file_name)
    file_path = os.path.join(stock_data_path, dated_file_name)
    df.to_csv(file_path, index=False, mode='w')
    print(f"Data saved to {file_path}")

import pandas as pd
from datetime import datetime

def get_ticker_list():
    """KOSPI와 KOSDAQ의 종목 리스트를 가져오는 함수."""
    today_date = get_date()
    market_list = ['KOSPI', 'KOSDAQ']
    kor_ticker_list_df = pd.DataFrame()

    for market_nm in market_list:
        ticker_list = stock.get_market_ticker_list(today_date, market=market_nm)
        
        for stockcode in ticker_list:
            corp_name = stock.get_market_ticker_name(stockcode)
            print(f'Processing stockcode: {stockcode}, corp_name: {corp_name}')
            df = pd.DataFrame({'stockcode': stockcode, 'corp_name': corp_name, 'market': market_nm}, index=[0])
            kor_ticker_list_df = pd.concat([kor_ticker_list_df, df], ignore_index=True)

    return kor_ticker_list_df.reset_index(drop=True)

def collect_ohlcv_data(kor_ticker_list):
    """종목별 OHLCV 데이터를 수집하는 함수."""
    today_date = get_date()
    file_name = 'kor_stock_ohlcv'
    all_data = pd.DataFrame()

    for stockcode in kor_ticker_list: 
        try:
            df_raw = stock.get_market_ohlcv(today_date, today_date, stockcode)
            df_raw = df_raw.reset_index()
            df_raw['stockcode'] = stockcode
            all_data = pd.concat([all_data, df_raw])
            print(f'{stockcode} OHLCV success')
        except Exception as e:
            print(f'{stockcode} OHLCV fail: {str(e)}')

    save_to_csv(all_data, file_name, today_date) 

def collect_market_cap_data(kor_ticker_list):
    """종목별 시가총액 데이터를 수집하는 함수."""
    today_date = '20241127'
    file_name = 'kor_market_cap'
    all_data = pd.DataFrame()

    for stockcode in kor_ticker_list:
        try:
            print(f"{stockcode}")
            df_raw = stock.get_market_cap(today_date, today_date, stockcode)
            df_raw = df_raw.reset_index()
            df_raw['stockcode'] = stockcode
            all_data = pd.concat([all_data, df_raw])
            print(f'{stockcode} market cap success')
        except Exception as e:
            print(f'{stockcode} market cap fail: {str(e)}')
    
    if 'index' in all_data.columns:
        all_data.drop(columns=['index'], inplace=True)

    save_to_csv(all_data, file_name, today_date)

def collect_fundamental_data(kor_ticker_list):
    """종목별 기본 재무 데이터를 수집하는 함수."""
    today_date = '20241127'
    file_name = 'kor_stock_fundamental'
    all_data = pd.DataFrame()

    for stockcode in kor_ticker_list:
        try:
            df_raw = stock.get_market_fundamental(today_date, today_date, stockcode)
            df_raw = df_raw.reset_index()
            df_raw['stockcode'] = stockcode
            all_data = pd.concat([all_data, df_raw])
            print(f'{stockcode} fundamental success')
        except Exception as e:
            print(f'{stockcode} fundamental fail: {str(e)}')

    if 'index' in all_data.columns:
        all_data.drop(columns=['index'], inplace=True)

    save_to_csv(all_data, file_name, today_date)

if __name__ == "__main__":
    ticker_list_df = get_ticker_list()
