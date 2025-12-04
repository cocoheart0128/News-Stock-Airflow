import airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

import sqlite3
import pandas as pd
import pendulum
import time
import sqlite3
from datetime import datetime
import os
from bs4 import BeautifulSoup
import requests
import re

from utils.query_loader import QueryLoader

tz = pendulum.timezone('Asia/Seoul')
start_date = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=7)).strftime('%Y-%m-%d') }}"
end_date   = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

naver_id = Variable.get("naver_client_id")
naver_key = Variable.get("naver_secret_key")
client_dict = {'client_id':naver_id,'client_secret': naver_key}


DB_PATH = "/opt/airflow/db/project.db"   # docker 환경 기준
ql = QueryLoader("ai_news_info.json")

with DAG(
    dag_id="dag_news_ai_analysis_pipeline",
    start_date=datetime(2025,12,3),
    schedule="@hourly",  ##매시간 실행
    catchup=False,
    tags = ['NEWS','AI'],
    template_searchpath=["/opt/airflow/include/sql"],
) as dag:
    
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    seq_info_task = SQLExecuteQueryOperator(
    task_id="seq_info_task",
    conn_id="sqlite_conn",
    sql = ql.getQueryString("unprocess_news_seq") ,
    do_xcom_push=True
)
    

    def ai_news_process(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='seq_info_task')
        df = pd.DataFrame(data)
        print(len(df))

    ai_news_process_task = PythonOperator(
        task_id='ai_news_process_task',
        python_callable=ai_news_process
    )
    
        


start_dag >> seq_info_task >> ai_news_process_task >> end_dag