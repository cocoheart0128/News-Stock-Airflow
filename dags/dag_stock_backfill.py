import airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import sqlite3
import pandas as pd
import pendulum
import sqlite3
from datetime import datetime,timedelta

from utils.query_loader import QueryLoader
import yfinance as yf


tz = pendulum.timezone('Asia/Seoul')
# start_date = '{{ds - macros.timedelta(hours = 9,days=7).strftime("%Y-%m-%d")}}'
# end_date = '{{ds - macros.timedelta(hours = 9,days=1).strftime("%Y-%m-%d")}}'
start_date = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=365)).strftime('%Y-%m-%d') }}"
end_date   = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"


DB_PATH = "/opt/airflow/db/project.db"   # docker 환경 기준
ql = QueryLoader("ticker_info.json")


def yf_stock_fetch(start_date,end_date,**kwargs):
    ti = kwargs['ti']
    ticker_rows = ti.xcom_pull(task_ids="ticker_info_task")
    tickers = [row[0] for row in ticker_rows]
    print(ql.getQueryString("insert_stock"))

    # execution_date = pd.to_datetime(execution_date_str)
    # start_date = (execution_date - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

    df = yf.download(tickers, start=start_date, end=end_date).stack(level=1).reset_index()
    df.rename(columns={'level_1':'Ticker'}, inplace=True)
    print(df)

    return df


with DAG(
    dag_id="dag_stock",
    start_date=datetime(2020,1,1),
    schedule=timedelta(days=365),  # 매일 09시
    catchup=True,
    template_searchpath=["/opt/airflow/include/sql"]
) as dag:
    
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    ticker_info_task = SQLExecuteQueryOperator(
    task_id="ticker_info_task",
    conn_id="sqlite_conn",
    sql = ql.getQueryString("ticker_info") ,
    do_xcom_push=True
)

    
    stock_fetch_task = PythonOperator(
    task_id='stock_fetch_task',
    python_callable=yf_stock_fetch,
    op_kwargs={'start_date': start_date ,'end_date': end_date},
    do_xcom_push=True
)
    
    delete_stock_task = SQLExecuteQueryOperator(
        task_id="delete_stock_task",
        conn_id="sqlite_conn",
        sql=ql.getQueryString("delete_stock"),  # 위 JSON 쿼리
        # sql = "delete_stock.sql",
        parameters=(start_date,end_date),
        do_xcom_push=True
    )

    
    def prepare_stock_values(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='stock_fetch_task')

        values_str = ", ".join([
            f"('{row['Date'].strftime('%Y-%m-%d')}', '{row['Ticker']}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Volume']})"
            for idx, row in df.iterrows()
        ])

        print(ql.getQueryString("insert_stock"))
        print(values_str)
        print(ql.getQueryString("insert_stock") +' '+ values_str)

        ti.xcom_push(key='values_str', value=values_str)

    prepare_values_task = PythonOperator(
        task_id='prepare_insert_values',
        python_callable=prepare_stock_values
    )

    # insert task
    insert_stock_task = SQLExecuteQueryOperator(
        task_id='insert_stock_task',
        conn_id='sqlite_conn',
        # sql='INSERT INTO stock_prices (Date,Ticker,Open,High,Low,Close,Volume) VALUES {{ ti.xcom_pull(task_ids="prepare_insert_values", key="values_str") }};',
        sql = ql.getQueryString("insert_stock") +' '+ '{{ ti.xcom_pull(task_ids="prepare_insert_values", key="values_str") }}'
    )


    start_dag >> ticker_info_task >> stock_fetch_task >> delete_stock_task >>prepare_values_task >> insert_stock_task >> end_dag

