from sqlalchemy import create_engine, Column, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
class CorpInfo(Base):
    __tablename__ = 'corp_info'

    stockcode = Column(String(50), primary_key=True, nullable=False)
    accounting_month = Column(String(10), default=None)
    address = Column(String(255), default=None)
    bs_regist_no = Column(String(50), default=None)
    ceo_name = Column(String(255), default=None)
    corp_class = Column(String(10), default=None)
    corp_name = Column(String(255), default=None)
    corp_number = Column(String(50), default=None)
    corp_regist_no = Column(String(50), default=None)
    establishment_date = Column(Date, default=None)
    home_url = Column(String(255), default=None)
    industry_code = Column(String(50), default=None)
    industry_name = Column(String(50), default=None)
    stock_name = Column(String(100), default=None)