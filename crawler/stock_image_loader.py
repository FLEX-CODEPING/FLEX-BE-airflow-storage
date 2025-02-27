from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from modules.stock.mysql_connection import get_mysql_connection
import csv
import os

# SQLAlchemy 설정
Base = declarative_base()

class ImageURL(Base):
    __tablename__ = 'stock_image'
    
    stockcode = Column(String(50), primary_key=True)
    image_url = Column(String(255), nullable=False)

def save_image_url(stockcode, image_url, engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        new_image_url = ImageURL(stockcode=stockcode, image_url=image_url)
        session.add(new_image_url)
        session.commit()
        print(f"{stockcode}의 이미지 URL이 데이터베이스에 저장되었습니다.")
    except Exception as e:
        print(f"이미지 URL 저장 중 오류 발생: {e}")
        session.rollback()
    finally:
        session.close()

def get_file_name(stockcode):
    return stockcode + "_symbol.svg"

def process_csv_and_save_to_db(csv_file_path, engine):
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            stockcode = row['stockcode']
            file_name = get_file_name(stockcode)
            image_url = f"https://4870-203-249-127-39.ngrok-free.app/disk1/{file_name}"
            
            # 이미지 URL 저장
            save_image_url(stockcode, image_url, engine)