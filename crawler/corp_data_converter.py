import requests
import pandas as pd
from io import BytesIO
import csv
import os, sys
from sqlalchemy import create_engine, Column, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from modules.stock.mysql_connection import get_mysql_connection
import pandas as pd
from sqlalchemy import update, select
from sqlalchemy.orm import Session

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from models.corp_info import Base, CorpInfo

def get_industry_class_name(csv_file_path):
    df = pd.read_csv(csv_file_path)
    industry_mapping = df[df['class_type'] == 'IND'].set_index('new_class_code')['new_class_name'].to_dict()
    return industry_mapping

def convert_code_to_industry_name(industry_mapping):
    try:
        stmt = select(CorpInfo)
        results = session.execute(stmt).scalars().all()
        for corp in results:
            if corp.industry_code in industry_mapping:
                update_stmt = (
                    update(CorpInfo)
                    .where(CorpInfo.stockcode == corp.stockcode)
                    .values(industry_name=industry_mapping[corp.industry_code])
                )
                session.execute(update_stmt)

        session.commit()
        print("산업 분류 항목명 업데이트 완료")

    except Exception as e:
        session.rollback()
        print(f"업데이트 중 오류 발생: {e}")

    finally:
        session.close()

if __name__ == "__main__":
    mysql_engine = get_mysql_connection()
    Session = sessionmaker(bind=mysql_engine)
    session = Session()

    current_dir = os.path.dirname(__file__)
    csv_file_path = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'corp', 'corp_class_list_20240927.csv'))
    industry_mapping = get_industry_class_name(csv_file_path)

    convert_code_to_industry_name(industry_mapping)