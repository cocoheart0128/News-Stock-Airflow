import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import sqlite3
import pandas as pd
import pendulum
from typing import List
import sqlite3


tz = pendulum.timezone('Asia/Seoul')
start_date = '{{execution_date + macros.timedelta(hours = 9,days=7).strftime("%Y-%m-%d")}}'
end_date = '{{execution_date + macros.timedelta(hours = 9,days=1).strftime("%Y-%m-%d")}}'

DB_PATH = "/opt/airflow/db/project.db"   # docker 환경 기준


def get_conn(path):
    conn = sqlite3.connect(DB_PATH,timeout=30)
    cursor = conn.cursor()
    return conn,cursor

def get_ticker(table_name):
    conn,cursor = get_conn(DB_PATH)
    cursor.execute(f"SELECT * FROM {table_name}")

    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    conn.close()

    return df['']




def get_stock_data(**context):
    ticker = "AAPL"  # 삼성전자 예시
    df = yf.download(ticker, period="1d")  # 오늘 데이터
    df.reset_index(inplace=True)
    context['ti'].xcom_push(key='stock_data', value=df.to_json())
    

def insert_data_sqlite(**context):
    stock_json = context['ti'].xcom_pull(key='stock_data')
    df = pd.read_json(stock_json)
    print(df)

    conn = sqlite3.connect(DB_PATH)
    df.to_sql("stock_price", conn, if_exists="append", index=False)
    conn.close()


with DAG(
    dag_id="yfinance_to_sqlite",
    start_date='2025-01-01',
    schedule="0 9 * * *",  # 매일 09시
    catchup=False
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Start DAG"),
    )

    collect_data = PythonOperator(
        task_id="collect_data",
        python_callable=get_stock_data,
    )

    insert_data = PythonOperator(
        task_id="insert_to_sqlite",
        python_callable=insert_data_sqlite,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("DAG end"),
    )

    start >> collect_data >> insert_data >> end
