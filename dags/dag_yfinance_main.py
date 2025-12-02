import airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import sqlite3
import pandas as pd
import pendulum
import sqlite3
from datetime import datetime

from utils.query_loader import QueryLoader
import yfinance as yf


tz = pendulum.timezone('Asia/Seoul')
start_date = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=7)).strftime('%Y-%m-%d') }}"
end_date   = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"


DB_PATH = "/opt/airflow/db/project.db"   # docker 환경 기준


with DAG(
    dag_id="dag_yfinance_pipeline",
    start_date=datetime(2025,1,1),
    schedule="0 9 * * *",  # 매일 09시
    catchup=True,
    template_searchpath=["/opt/airflow/include/sql"],
    default_args = {
        'depends_on_past': True,
    }
) as dag:
    
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")
    fork_stock_dag = EmptyOperator(task_id="fork_stock_dag")


    trigger_dag_stock = TriggerDagRunOperator(
            task_id="trigger_dag_stock",
            trigger_dag_id="dag_stock",  # 실행할 DAG의 ID
            wait_for_completion=True,  # True면 dag_b가 끝날 때까지 기다림
        )
    
    trigger_dag_exchange = TriggerDagRunOperator(
        task_id="trigger_dag_exchange",
        trigger_dag_id="dag_exchange_rate",  # 실행할 DAG의 ID
        wait_for_completion=True,  # True면 dag_b가 끝날 때까지 기다림
    )

    trigger_dag_index= TriggerDagRunOperator(
        task_id="trigger_dag_index",
        trigger_dag_id="dag_index",  # 실행할 DAG의 ID
        wait_for_completion=True,  # True면 dag_b가 끝날 때까지 기다림
    )



start_dag >> fork_stock_dag 
fork_stock_dag >> trigger_dag_stock >> end_dag
fork_stock_dag >> trigger_dag_exchange >> end_dag
fork_stock_dag >> trigger_dag_index >> end_dag

