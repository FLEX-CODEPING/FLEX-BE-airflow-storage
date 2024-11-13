import sys
import os
import pandas as pd
from sqlalchemy import create_engine, types, Table, Column, BigInteger, String, MetaData, Float, Date

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def load_csv_to_mysql():
    mysql_engine = create_engine('mysql+pymysql://admin:codeping1!@172.16.212.31:3306/flex_stock')
    
    # 메타데이터 객체 생성
    metadata = MetaData()

    # 테이블 정의 (id를 BigInteger로 변경)
    stock_table = Table('stock', metadata,
        Column('id', BigInteger, primary_key=True, autoincrement=True),
        Column('stockcode', String(50)),
        Column('corp_name', String(50)),
        Column('market', String(50))
    )

    ohlcv_table = Table('stock_ohlcv', metadata,
        Column('id', BigInteger, primary_key=True, autoincrement=True),
        Column('date', Date),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume', BigInteger),
        Column('change_rate', Float),
        Column('stockcode', String(50))
    )

    market_cap_table = Table('stock_market_cap', metadata,
        Column('id', BigInteger, primary_key=True, autoincrement=True),
        Column('date', Date),
        Column('market_cap', BigInteger),
        Column('volume', BigInteger),
        Column('trading_value', BigInteger),
        Column('listed_shares', BigInteger),
        Column('stockcode', String(50))
    )

    metadata.create_all(mysql_engine)
    
    # kor_ticker_list.csv 적재
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'stock', 'kor_ticker_list.csv')
    df = pd.read_csv(csv_path)
    df.to_sql('stock', con=mysql_engine, if_exists='append', index=False,
              dtype={
                  'stockcode': types.VARCHAR(50),
                  'corp_name': types.VARCHAR(50),
                  'market': types.VARCHAR(50)
              })

    # kor_stock_ohlcv.csv 적재
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'stock', 'kor_stock_ohlcv.csv')
    df = pd.read_csv(csv_path)
    df.rename(columns={
        '날짜': 'date',
        '시가': 'open',
        '고가': 'high',
        '저가': 'low',
        '종가': 'close',
        '거래량': 'volume',
        '등락률': 'change_rate',
        '종목코드': 'stockcode'
    }, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df.to_sql('stock_ohlcv', con=mysql_engine, if_exists='append', index=False,
              dtype={
                  'date': types.Date(),
                  'open': types.Float(),
                  'high': types.Float(),
                  'low': types.Float(),
                  'close': types.Float(),
                  'volume': types.BigInteger(),
                  'change_rate': types.Float(),
                  'stockcode': types.VARCHAR(50)
              })

    # kor_market_cap.csv 적재
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'stock', 'kor_market_cap.csv')
    df = pd.read_csv(csv_path)
    df.rename(columns={
        '날짜': 'date',
        '시가총액': 'market_cap',
        '거래량': 'volume',
        '거래대금': 'trading_value',
        '상장주식수': 'listed_shares',
        '종목코드': 'stockcode'
    }, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df.to_sql('stock_market_cap', con=mysql_engine, if_exists='replace', index=False,
              dtype={
                  'date': types.Date(),
                  'market_cap': types.BigInteger(),
                  'volume': types.BigInteger(),
                  'trading_value': types.BigInteger(),
                  'listed_shares': types.BigInteger(),
                  'stockcode': types.VARCHAR(50)
              })
    
    print("모든 CSV 파일이 성공적으로 MySQL 데이터베이스에 적재되었습니다.")