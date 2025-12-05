import airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from google import genai

import sqlite3
import pandas as pd
import pendulum
import time
import sqlite3
from datetime import datetime
import os
import json
from bs4 import BeautifulSoup
import requests
import re
import lancedb

from utils.query_loader import QueryLoader
from utils.ai_process import AiProcess

tz = pendulum.timezone('Asia/Seoul')
# start_date = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=7)).strftime('%Y-%m-%d') }}"
# end_date   = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

gemini_api_key = Variable.get("gemini_api_key")
DB_PATH = "/opt/airflow/db/project.db"   # docker 환경 기준
VectorDB_NAME ='/opt/airflow/db/vector'
VectorDB_TB_NAME = 'tb_naver_news_ai_emb'
ql = QueryLoader("ai_news_info.json")

with DAG(
    dag_id="dag_news_ai_analysis_pipeline",
    start_date=datetime(2025,12,3),
    schedule="@daily",
    catchup=False,
    tags = ['NEWS','AI'],
    template_searchpath=["/opt/airflow/include/sql"],
) as dag:
    
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")
    fork_embed_dag = EmptyOperator(task_id="fork_embed_dag")
    fork_analysis_dag = EmptyOperator(task_id="fork_analysis_dag")

    seq_info_task = SQLExecuteQueryOperator(
    task_id="seq_info_task",
    conn_id="sqlite_conn",
    sql = ql.getQueryString("unprocess_news_seq") ,
    do_xcom_push=True
)

    def news_embedding_process(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='seq_info_task')
        df = pd.DataFrame(data)
        df = df.iloc[0:50]
        df.columns = ['seq','title','content','pubDate','media']

        service = AiProcess(api_key=gemini_api_key,vector_db_path=VectorDB_NAME)
        # embeddings = service.safe_gemini_batch_embedding(df) --- gemini 제한
        embeddings = service.safe_local_batch_embedding(df)
        tbl = service.lancedb_create_or_update(VectorDB_TB_NAME,embeddings,"seq")
        # print(tbl.to_pandas())

    news_embedding_process_task = PythonOperator(
        task_id='news_embedding_process_task',
        python_callable=news_embedding_process,
        do_xcom_push=True
    )

    def news_similarity_process(**kwargs):
        ti = kwargs['ti']
        search_dt = f"'{kwargs['ds']}%'"
        print(search_dt)

        # ▼ 1) 새로 임베딩한 뉴스들(seq 리스트)
        # 'seq_info_task'에서 XCom으로 전달된, 이번에 처리된 뉴스 목록
        data = ti.xcom_pull(task_ids='seq_info_task')
        df_new = pd.DataFrame(data).head(5)
        df_new.columns = ['seq','title','content','pubDate','media']
        
        # AiProcess 서비스 재사용
        service = AiProcess(api_key=gemini_api_key,vector_db_path=VectorDB_NAME)

        # ▼ 2) 벡터DB 로드 및 유사도 검색 수행
        similarity_results = service.news_similarity_process(df_new, VectorDB_TB_NAME,search_dt)
        print(similarity_results)
        similarity_results_str = service.df_to_sql_values_string(similarity_results)
        # ▼ 3) 결과를 XCom에 저장하거나, DB에 저장 (여기서는 XCom에 저장하여 다음 분석 Task로 전달)
        # 주의: 유사도 결과가 많을 경우 XCom에 부하를 줄 수 있으므로, 적절한 후속 DB 저장 태스크가 필요함
        ti.xcom_push(key='values_str', value=similarity_results_str)
        
    news_similarity_process_task = PythonOperator(
        task_id='news_similarity_process_task',
        python_callable=news_similarity_process,
        do_xcom_push=True
    )

    news_similarity_save_task = SQLExecuteQueryOperator(
    task_id="news_similarity_save_task",
    conn_id="sqlite_conn",
    sql = ql.getQueryString("insert_similarity_result") +' '+ '{{ ti.xcom_pull(task_ids="news_similarity_process_task", key="values_str") }}' ,
    do_xcom_push=True
)

start_dag >> seq_info_task >> news_embedding_process_task >> news_similarity_process_task >> news_similarity_save_task >> end_dag