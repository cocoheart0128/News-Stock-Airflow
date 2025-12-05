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
from bs4 import BeautifulSoup
import requests
import re
import lancedb

from utils.query_loader import QueryLoader


def lancedb_create_or_update(db_name,tb_name,doc_list,pk):
    db= lancedb.connect(db_name)
    if tb_name not in db.table_names():
        tbl = db.create_table(name=tb_name,data = doc_list)
        print('신규 테이블 생성되었습니다.')
        print(f'table name : {db_name}.{tb_name}')
        print(f'table cnt : {tbl.count_rows()}')
    else:
        tbl = db.open_table(tb_name)
        print('신규 테이블 업데이트되었습니다.')
        print(f'table name : {db_name}.{tb_name}')
        print(f'table cnt before: {tbl.count_rows()}')
        tbl.merge_insert(pk).when_matched_update_all().when_not_matched_insert_all().execute(doc_list)
        print(f'table cnt after: {tbl.count_rows()}')
    
    # return db

def gemini_batch_embedding(text_list):
    """
    text_list: ["title\ncontent", "title\ncontent", ...]
    """

    client = genai.Client(api_key=gemini_api_key)

    result = client.models.embed_content(
        model="gemini-embedding-001",
        contents=text_list
    )

    # result.embeddings → 각 문서 vector 리스트
    return [e.values for e in result.embeddings]


tz = pendulum.timezone('Asia/Seoul')
start_date = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=7)).strftime('%Y-%m-%d') }}"
end_date   = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

gemini_api_key = Variable.get("gemini_api_key")
DB_PATH = "/opt/airflow/db/project.db"   # docker 환경 기준
VectorDB_NAME ='/opt/airflow/db/vector'
VectorDB_TB_NAME = 'tb_naver_news_ai_emb'
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
    

    def news_embedding_process(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='seq_info_task')
        df = pd.DataFrame(data)
        df = df.head(5)
        df.columns = ['seq','title','content','pubDate','media']
        print(df.columns)

        # 2) 텍스트 리스트 만들기
        text_list = (df["title"] + "\n" + df["content"]).tolist()

        # 3) Batch embedding (LLM 호출 1번)
        embeddings = gemini_batch_embedding(text_list)
        # 4) LanceDB insert/upsert용 row 생성
        rows = []
        for i, row in df.iterrows():
            rows.append({
                "seq": row["seq"],
                "title": row["title"],
                "content": row["content"],
                "pubDate": row["pubDate"],
                "media": row["media"],
                "embedding": embeddings[i],
            })

        lancedb_create_or_update(VectorDB_NAME,VectorDB_TB_NAME,rows,"seq")

    news_embedding_process_task = PythonOperator(
        task_id='news_embedding_process_task',
        python_callable=news_embedding_process
    )

start_dag >> seq_info_task >> news_embedding_process_task >> end_dag