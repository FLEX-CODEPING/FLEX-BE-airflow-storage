import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, types
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from date_util import get_dated_filename

def load_csv_to_mysql():
    try:
        mysql_engine = create_engine('mysql+pymysql://admin:codeping1!@172.16.212.31:3306/flex_stock')
        print("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
    except Exception as e:
        print(f"데이터베이스 연결 오류: {e}")
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
    
    #kor_stock_ohlcv.csv 로드 및 처리
    try:
        ohlcv_csv_path = os.path.join(data_stock_path, get_dated_filename('kor_stock_ohlcv'))
        ohlcv_df = pd.read_csv(ohlcv_csv_path)

        # 열 이름 변경 및 날짜 형식 변환
        ohlcv_df.rename(columns={
            '날짜': 'date',
            '시가': 'open',
            '고가': 'high',
            '저가': 'low',
            '종가': 'close',
            '거래량': 'volume',
            '등락률': 'change_rate',
            '종목코드': 'stockcode'
        }, inplace=True)
        
        # 날짜 형식 확인 및 변환
        ohlcv_df['date'] = pd.to_datetime(ohlcv_df['date'], errors='coerce')
        
        # DataFrame을 SQL 테이블에 적재
        ohlcv_df.to_sql(
            name='stock_ohlcv',
            con=mysql_engine,
            if_exists='append',
            index=False,
            dtype={
                'date': types.Date(),
                'open': types.Float(),
                'high': types.Float(),
                'low': types.Float(),
                'close': types.Float(),
                'volume': types.BigInteger(),
                'change_rate': types.Float(),
                'stockcode': types.String(50)
            }
        )
        print("kor_stock_ohlcv.csv 파일이 성공적으로 적재되었습니다.")
    
    except ValueError as ve:
        print(f"ValueError 발생: {ve}")
    except Exception as e:
        print(f"kor_stock_ohlcv.csv 파일 처리 중 오류 발생: {e}")

    # kor_market_cap.csv 로드 및 처리
    try:
        market_cap_csv_path = os.path.join(data_stock_path, get_dated_filename('kor_market_cap'))
        market_cap_df = pd.read_csv(market_cap_csv_path)

        market_cap_df.rename(columns={
            '날짜': 'date',
            '시가총액': 'market_cap',
            '거래량': 'volume',
            '거래대금': 'trading_value',
            '상장주식수': 'listed_shares',
            '종목코드': 'stockcode'
        }, inplace=True)

        # 날짜 형식 확인 및 변환
        market_cap_df['date'] = pd.to_datetime(market_cap_df['date'], errors='coerce')

        # DataFrame을 SQL 테이블에 적재
        market_cap_df.to_sql(
            name='stock_market_cap',
            con=mysql_engine,
            if_exists='append',
            index=False,
            dtype={
                'date': types.Date(),
                'market_cap': types.BigInteger(),
                'volume': types.BigInteger(),
                'trading_value': types.BigInteger(),
                'listed_shares': types.BigInteger(),
                'stockcode': types.String(50)
            }
        )
        print("kor_market_cap.csv 파일이 성공적으로 적재되었습니다.")

    except ValueError as ve:
        print(f"ValueError 발생: {ve}")
    except Exception as e:
        print(f"kor_market_cap.csv 파일 처리 중 오류 발생: {e}")