from sqlalchemy import create_engine

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def load_csv_to_mysql():
    # MySQL 연결 정보 설정
    mysql_engine = create_engine('mysql+pymysql://root:joowojr0318!@localhost:3306/flex_stock')  # 적절한 정보로 변경
    
    # CSV 파일 읽기
    df = pd.read_csv('data/stock/kor_ticker_list.csv')  # 경로 변경 필요
    
    # DataFrame을 MySQL 테이블에 적재 (테이블 이름은 'your_table_name'으로 변경)
    df.to_sql('stock', con=mysql_engine, if_exists='replace', index=False)

# with DAG(
#     dag_id='data_loading_dag',
#     default_args=default_args,
#     schedule_interval=None,  # 수동 실행 또는 다른 DAG에서 호출 가능
# ) as dag:

#     load_to_mysql_task = PythonOperator(
#         task_id='load_csv_to_mysql',
#         python_callable=load_csv_to_mysql,
#     )