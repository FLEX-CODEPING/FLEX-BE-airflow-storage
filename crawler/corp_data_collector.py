import requests
import pandas as pd
from io import BytesIO
import csv
import os, sys
from sqlalchemy import create_engine, Column, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from modules.stock.mysql_connection import get_mysql_connection
import dart_fss as dart
from dotenv import load_dotenv


sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from models.corp_info import Base, CorpInfo

def set_dart_client():
   load_dotenv()
   api_key = os.getenv('API_KEY', 'localhost') 
   dart.set_api_key(api_key=api_key)
   return dart

def get_valid_df():
    all = dart.api.filings.get_corp_code()
    all[0]
    df = pd.DataFrame(all)
    return df[df['stock_code'].notnull()]
            
def get_and_save_corp_info(csv_file_path, df_listed):
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            stockcode = row['stockcode']
            filtered_df = df_listed[df_listed['stock_code'] == stockcode]
    
            if filtered_df.empty:
                print(f"No matching stock code found for: {stockcode}")
                continue
            
            corp_code = filtered_df.iloc[0, 0] 
            response_data = dart.api.filings.get_corp_info(corp_code)  # API 호출

            save_corp_info(response_data)

def save_corp_info(corp_data):
    """Save corporate information to the database."""
    try:
        new_corp_info = CorpInfo(
            stockcode=corp_data['stock_code'], 
            corp_number=corp_data['corp_code'], 
            corp_name=corp_data['corp_name'],   
            stock_name=corp_data['stock_name'],  
            ceo_name=corp_data['ceo_nm'],       
            corp_class=corp_data['corp_cls'],     
            corp_regist_no=corp_data['jurir_no'],
            bs_regist_no=corp_data['bizr_no'],   
            address=corp_data['adres'],         
            home_url=corp_data['hm_url'],       
            industry_code=corp_data['induty_code'],
            establishment_date=pd.to_datetime(corp_data['est_dt'], format='%Y%m%d', errors='coerce').date() if corp_data['est_dt'] else None,  
            accounting_month=corp_data['acc_mt']  
        )
        session.add(new_corp_info)  
        session.commit()              
        print(f"Corporate information successfully saved: {corp_data['corp_name']}")
        
    except Exception as e:
        print(f"Error inserting data for {corp_data['stock_code']}: {e}")


if __name__ == "__main__":
    mysql_engine = get_mysql_connection()
    current_dir = os.path.dirname(__file__)
    csv_file_path = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'stock', 'kor_ticker_list.csv'))
    Session = sessionmaker(bind=mysql_engine)
    session = Session()

    set_dart_client()
    df_listed = get_valid_df()
    get_and_save_corp_info(csv_file_path, df_listed)

