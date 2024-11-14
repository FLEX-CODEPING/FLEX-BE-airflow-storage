import pandas as pd
import sqlalchemy
from sqlalchemy import types
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from date_util import get_dated_filename
from database_connection import get_database_connection


def load_csv_to_mysql():
    mysql_engine = get_database_connection()
    if not mysql_engine:
        return

    current_dir = os.path.dirname(__file__)
    data_stock_path = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'stock'))

    # csv_path = os.path.join(data_stock_path, 'kor_ticker_list.csv')
    # df = pd.read_csv(csv_path)
    # df.to_sql('stock', con=mysql_engine, if_exists='append', index=False,
    #           dtype={
    #               'stockcode': types.VARCHAR(50),
    #               'corp_name': types.VARCHAR(50),
    #               'market': types.VARCHAR(50)
    #           })

    # kor_stock_ohlcv.csv 로드 및 처리
    load_ohlcv_data(mysql_engine, data_stock_path)

    # kor_market_cap.csv 로드 및 처리
    load_market_cap_data(mysql_engine, data_stock_path)

def load_ohlcv_data(mysql_engine, data_stock_path):
    try:
        ohlcv_csv_path = os.path.join(data_stock_path, get_dated_filename('kor_stock_ohlcv'))
        ohlcv_df = pd.read_csv(ohlcv_csv_path)

        ohlcv_df.rename(columns={
            '날짜': 'date', '시가': 'open', '고가': 'high', '저가': 'low',
            '종가': 'close', '거래량': 'volume', '등락률': 'change_rate', '종목코드': 'stockcode'
        }, inplace=True)
        
        ohlcv_df['date'] = pd.to_datetime(ohlcv_df['date'], errors='coerce')
        
        ohlcv_df.to_sql(
            name='stock_ohlcv',
            con=mysql_engine,
            if_exists='append',
            index=False,
            dtype={
                'date': types.Date(), 'open': types.Float(), 'high': types.Float(),
                'low': types.Float(), 'close': types.Float(), 'volume': types.BigInteger(),
                'change_rate': types.Float(), 'stockcode': types.String(50)
            }
        )
        print("kor_stock_ohlcv.csv 파일이 성공적으로 적재되었습니다.")
    
    except Exception as e:
        print(f"kor_stock_ohlcv.csv 파일 처리 중 오류 발생: {e}")

def load_market_cap_data(mysql_engine, data_stock_path):
    try:
        market_cap_csv_path = os.path.join(data_stock_path, get_dated_filename('kor_market_cap'))
        market_cap_df = pd.read_csv(market_cap_csv_path)

        market_cap_df.rename(columns={
            '날짜': 'date', '시가총액': 'market_cap', '거래량': 'volume',
            '거래대금': 'trading_value', '상장주식수': 'listed_shares', '종목코드': 'stockcode'
        }, inplace=True)

        market_cap_df['date'] = pd.to_datetime(market_cap_df['date'], errors='coerce')

        market_cap_df.to_sql(
            name='stock_market_cap',
            con=mysql_engine,
            if_exists='append',
            index=False,
            dtype={
                'date': types.Date(), 'market_cap': types.BigInteger(),
                'volume': types.BigInteger(), 'trading_value': types.BigInteger(),
                'listed_shares': types.BigInteger(), 'stockcode': types.String(50)
            }
        )
        print("kor_market_cap.csv 파일이 성공적으로 적재되었습니다.")

    except Exception as e:
        print(f"kor_market_cap.csv 파일 처리 중 오류 발생: {e}")
