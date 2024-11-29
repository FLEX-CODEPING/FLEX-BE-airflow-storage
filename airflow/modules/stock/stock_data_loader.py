import pandas as pd
import sqlalchemy
from sqlalchemy import types
import logging
import sys
import os
import json
from datetime import timedelta
from utils.date_util import get_dated_filename, get_date
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from infrastructure.mysql.mysql_connection import get_mysql_connection
from infrastructure.redis.redis_connection import get_redis_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # ticker list 저장 
    # csv_path = os.path.join(data_stock_path, 'kor_ticker_list.csv')
    # df = pd.read_csv(csv_path)
    # df.to_sql('stock', con=mysql_engine, if_exists='append', index=False,
    #           dtype={
    #               'stockcode': types.VARCHAR(50),
    #               'corp_name': types.VARCHAR(50),
    #               'market': types.VARCHAR(50)
    #           })

def load_ohlcv_data():
    mysql_engine = get_mysql_connection()
    if not mysql_engine:
        logging.error("MySQL 연결을 생성할 수 없습니다.")
        return

    current_dir = os.path.dirname(__file__)
    data_path = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'stock', get_date()))
    
    try:
        ohlcv_csv_path = os.path.join(data_path, get_dated_filename('kor_stock_ohlcv'))
        logging.info(f"CSV 파일 경로: {ohlcv_csv_path}")

        # CSV 파일 읽기
        ohlcv_df = pd.read_csv(ohlcv_csv_path)
        logging.info(f"CSV 파일에서 {len(ohlcv_df)}개의 행을 읽었습니다.")

        # 컬럼 이름 변경
        ohlcv_df.rename(columns={
            '날짜': 'date', 
            '시가': 'open_price', 
            '고가': 'high_price', 
            '저가': 'low_price',
            '종가': 'close_price', 
            '거래량': 'volume', 
            '등락률': 'change_rate', 
            '종목코드': 'stockcode'
        }, inplace=True)

        # 날짜 형식 변환
        ohlcv_df['date'] = pd.to_datetime(ohlcv_df['date'], errors='coerce')
        
        # stock_id 생성
        ohlcv_df['stock_id'] = ohlcv_df['stockcode'] 

        with mysql_engine.connect() as connection:
            stock_mapping = pd.read_sql('SELECT stockcode, stockcode AS stock_id FROM stock', con=connection)
            stockcode_set = set(stock_mapping['stockcode'])

        # 유효한 종목코드 필터링
        ohlcv_df = ohlcv_df[ohlcv_df['stockcode'].isin(stockcode_set)]
        
        logging.info(f"필터링된 데이터: {len(ohlcv_df)}개의 행이 남았습니다.")

        # stock_id 컬럼 제거
        ohlcv_df.drop(columns=['stock_id'], inplace=True)

        # 데이터베이스에 적재
        ohlcv_df = ohlcv_df.merge(
            stock_mapping,
            on='stockcode',
            how='inner'
        )

        ohlcv_df.to_sql(
            name='stock_ohlcv',
            con=mysql_engine,
            if_exists='append',
            index=False,
            dtype={
                'date': types.Date(), 
                'open_price': types.Float(), 
                'high_price': types.Float(),
                'low_price': types.Float(), 
                'close_price': types.Float(), 
                'volume': types.BigInteger(),
                'change_rate': types.Float(), 
                'stockcode': types.String(50),
                'stock_id': types.String(50) 
            }
        )
        
        logging.info("kor_stock_ohlcv.csv 파일이 성공적으로 적재되었습니다.")
    
    except Exception as e:
        logging.error(f"kor_stock_ohlcv.csv 파일 처리 중 오류 발생: {e}", exc_info=True)

def load_market_cap_data():
    current_dir = os.path.dirname(__file__)
    data_path = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'stock', get_date()))
    
    try:
        market_cap_csv_path = os.path.join(data_path, get_dated_filename('kor_market_cap'))
        logging.info(f"CSV 파일 경로: {market_cap_csv_path}")

        # CSV 파일 읽기
        market_cap_df = pd.read_csv(market_cap_csv_path)
        logging.info(f"CSV 파일에서 {len(market_cap_df)}개의 행을 읽었습니다.")

        # 컬럼 이름 변경
        market_cap_df.rename(columns={
            '날짜': 'date', 
            '시가총액': 'market_cap', 
            '거래량': 'volume',
            '거래대금': 'trading_value', 
            '상장주식수': 'listed_shares', 
            '종목코드': 'stockcode'
        }, inplace=True)

        # 날짜 형식 변환
        market_cap_df['date'] = pd.to_datetime(market_cap_df['date'], errors='coerce')
        
        # stock_id 생성
        market_cap_df['stock_id'] = market_cap_df['stockcode'] 

        mysql_engine = get_mysql_connection()
        if not mysql_engine:
            logging.error("MySQL 연결을 생성할 수 없습니다.")
            return

        with mysql_engine.connect() as connection:
            stock_mapping = pd.read_sql('SELECT stockcode, stockcode AS stock_id FROM stock', con=connection)
            stockcode_set = set(stock_mapping['stockcode'])

        # 유효한 종목코드 필터링
        market_cap_df = market_cap_df[market_cap_df['stockcode'].isin(stockcode_set)]
        
        logging.info(f"필터링된 데이터: {len(market_cap_df)}개의 행이 남았습니다.")

        # stock_id 컬럼 제거
        market_cap_df.drop(columns=['stock_id'], inplace=True)

        market_cap_df = market_cap_df.merge(
            stock_mapping,
            on='stockcode',
            how='inner'
        )

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
                'stockcode': types.String(50),
                'stock_id': types.String(50) 
            }
        )
        
        logging.info("kor_market_cap.csv 파일이 성공적으로 적재되었습니다.")

    except Exception as e:
        logging.error(f"kor_market_cap.csv 파일 처리 중 오류 발생: {e}", exc_info=True)

expiration_days = int(os.getenv('FUNDAMENTAL_DATA_EXPIRATION', 3))

def load_fundamental_data():
    current_dir = os.path.dirname(__file__)
    data_path = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'stock', get_date()))
    
    try:
        fundamental_data_csv_path = os.path.join(data_path, get_dated_filename('kor_stock_fundamental'))
        logging.info(f"CSV 파일 경로: {fundamental_data_csv_path}")

        # CSV 파일 읽기
        fundamental_data_df = pd.read_csv(fundamental_data_csv_path)
        logging.info(f"CSV 파일에서 {len(fundamental_data_df)}개의 행을 읽었습니다.")

        fundamental_data_df.rename(columns={
            '날짜': 'date'
        }, inplace=True)

        # 날짜 형식 변환
        fundamental_data_df['date'] = pd.to_datetime(fundamental_data_df['date'], errors='coerce')
        
        # stock_id 생성
        fundamental_data_df['stock_id'] = fundamental_data_df['stockcode']

        redis = get_redis_connection()

        for _, row in fundamental_data_df.iterrows():   
            date_str = row['date'].strftime('%Y-%m-%d')
            
            key = f"fundamental:{row['stockcode']}:{date_str}"
            data = row.to_dict()
            data['date'] = date_str

            json_data = json.dumps(data)
            
            expiration_time = timedelta(days=expiration_days).total_seconds()
            
            redis.setex(key, int(expiration_time), json_data)
        logging.info("kor_stock_fundamental 파일이 성공적으로 적재되었습니다.")

    except Exception as e:
        logging.error(f"kor_stock_fundamental 파일 처리 중 오류 발생: {e}", exc_info=True)