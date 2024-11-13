import pandas as pd
from pykrx import stock
from datetime import datetime

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def save_to_csv(df, file_name):
    """DataFrame을 CSV 파일로 저장하는 함수."""
    if not os.path.exists('data/stock'):
        os.makedirs('data/stock')
    file_path = f'data/stock/{file_name}.csv'
    if not os.path.exists(file_path):
        df.to_csv(file_path, index=False, mode='w')
    else:
        df.to_csv(file_path, index=False, mode='a', header=False)

def get_ticker_list():
    """KOSPI와 KOSDAQ의 종목 리스트를 가져오는 함수."""
    now = datetime.now()
    today_date1 = now.strftime('%Y%m%d')
    market_list = ['KOSPI', 'KOSDAQ']
    kor_ticker_list_df = pd.DataFrame()

    for market_nm in market_list:
        ticker_list = stock.get_market_ticker_list(today_date1, market=market_nm)
        for stockcode in ticker_list:
            corp_name = stock.get_market_ticker_name(stockcode)
            df = pd.DataFrame({'stockcode': stockcode, 'corp_name': corp_name, 'market': market_nm}, index=[0])
            kor_ticker_list_df = pd.concat([kor_ticker_list_df, df])

    return kor_ticker_list_df.reset_index(drop=True)

def collect_ohlcv_data(kor_ticker_list):
    """종목별 OHLCV 데이터를 수집하는 함수."""
    for stockcode in kor_ticker_list:
        file_name = 'kor_stock_ohlcv'
        try:
            df_raw = stock.get_market_ohlcv(start_date, today_date1, stockcode)
            df_raw = df_raw.reset_index()
            df_raw['stockcode'] = stockcode
            
            save_to_csv(df_raw, file_name)
            print(f'{stockcode} OHLCV success')
        except Exception as e:
            print(f'{stockcode} OHLCV fail: {str(e)}')

def collect_market_cap_data(kor_ticker_list):
    """종목별 시가총액 데이터를 수집하는 함수."""
    for ticker_nm in kor_ticker_list:
        file_name = 'kor_market_cap'
        
        try:
            df_raw = stock.get_market_cap(start_date, today_date1, ticker_nm)
            df_raw = df_raw.reset_index()
            
            print(f"{ticker_nm} columns: {df_raw.columns.tolist()}")
            
            df_raw['stockcode'] = ticker_nm
            
            save_to_csv(df_raw, file_name)
            print(f'{ticker_nm} market cap success')
        except Exception as e:
            print(f'{ticker_nm} market cap fail: {str(e)}')

def collect_fundamental_data(kor_ticker_list):
    """종목별 기본 재무 데이터를 수집하는 함수."""
    for ticker_nm in kor_ticker_list:
        file_name = 'kor_stock_fundamental'
        
        try:
            df_raw = stock.get_market_fundamental(start_date, today_date1, ticker_nm)
            df_raw = df_raw.reset_index()
            df_raw['stockcode'] = ticker_nm
            
            save_to_csv(df_raw, file_name)
            print(f'{ticker_nm} fundamental success')
        except Exception as e:
            print(f'{ticker_nm} fundamental fail: {str(e)}')

if __name__ == "__main__":
    # 날짜 설정
    now = datetime.now()
    today_date1 = now.strftime('%Y%m%d')
    start_date = '20180101'

    # 종목 리스트 가져오기
    kor_ticker_list_df = get_ticker_list()
    kor_ticker_list_df.to_csv('data/stock/kor_ticker_list.csv', index=False)
    
    # 종목 코드 리스트
    kor_ticker_list = kor_ticker_list_df['stockcode']

    # 데이터 수집
    collect_ohlcv_data(kor_ticker_list)
    collect_market_cap_data(kor_ticker_list)
    collect_fundamental_data(kor_ticker_list)