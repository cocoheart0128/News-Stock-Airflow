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
ql = QueryLoader("company_info.json")

def parsing_webpage(url, retries=3, timeout=10, delay=2):
    """
    안전하게 웹페이지를 파싱하는 함수
    - retries: 실패 시 재시도 횟수
    - timeout: requests 타임아웃
    - delay: 재시도 전 대기 시간
    """
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=timeout)
            res.raise_for_status()  # HTTP 오류가 나면 예외 발생
            soup = BeautifulSoup(res.text, "html.parser")

            meta = {}
            meta["title"] = soup.select_one('meta[property="og:title"]')["content"] if soup.select_one('meta[property="og:title"]') else ""
            meta["description"] = soup.select_one('meta[property="og:description"]')["content"] if soup.select_one('meta[property="og:description"]') else ""
            meta["url"] = soup.select_one('meta[property="og:url"]')["content"] if soup.select_one('meta[property="og:url"]') else url
            meta["image"] = soup.select_one('meta[property="og:image"]')["content"] if soup.select_one('meta[property="og:image"]') else ""
            meta["media"] = soup.select_one('meta[property="og:article:author"]')["content"].split('|')[0].replace(' ','') if soup.select_one('meta[property="og:article:author"]') else ""
            meta['content'] = soup.select_one("article#dic_area").get_text(" ", strip=True) if soup.select_one("article#dic_area") else ""
            
            report_tx = soup.select_one("div.byline span.byline_s").get_text(strip=True) if soup.select_one("div.byline span.byline_s") else ""
            meta['reporter_nm'] = "".join(re.findall(r'[가-힣\s]+', report_tx)).strip().replace('기자','')
            meta['reporter_email'] = "".join(re.findall(r'[A-Za-z@._]+', report_tx)).strip()

            return meta

        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            print(f"Attempt {attempt+1} failed for URL {url}: {e}")
            time.sleep(delay)
    # 재시도 모두 실패 시 빈 dict 반환
    print(f"All {retries} attempts failed for URL {url}")
    return {
        "title": "", "description": "", "url": url, "image": "",
        "media": "", "content": "", "reporter_nm": "", "reporter_email": ""
    }


def get_news_data(keyword_list,client_id,client_secret):
    # client_id = client_dict['client_id']
    # client_secret = client_dict['client_secret']

    all_dfs =[]

    for keyword in keyword_list:
        params = {"query": keyword, "sort": "sim","display":100}
        url = "https://openapi.naver.com/v1/search/news"
        headers = {
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret
        }
        res = requests.get(url, headers=headers, params=params)

        df = pd.DataFrame(res.json()['items'])
        df['image']=''
        df['content']=''
        df['media']=''
        df['reporter_nm']=''
        df['reporter_email']=''
        df = df[df['link'].str.startswith('https://n.news.naver.com/')].reset_index(drop=True)

        add_cols = ['image','content','media','reporter_nm','reporter_email']
        
        for i in range(len(df)):
            url = df['link'][i]
            data = parsing_webpage(url)
            for col in add_cols:
                df.loc[i, col]=data[col]
        
        df['pubDate'] = pd.to_datetime(df['pubDate'], utc=True)
        df['pubDate'] = df['pubDate'].dt.tz_convert("Asia/Seoul")
        # df['date'] = df['pubDate'].dt.strftime("%Y-%m-%d")
        df['pubDate'] = df['pubDate'].dt.strftime("%Y-%m-%d %H:%M:%S")
        df['comp'] = keyword  

        all_dfs.append(df)

        # 여러 키워드 데이터프레임 합치기
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    time.sleep(5)
    return final_df 

with DAG(
    dag_id="dag_news_pipeline",
    start_date=datetime(2025,12,1),
    schedule="@daily",  ##매시간 실행
    catchup=False,
    tags = ['NEWS','ETL'],
    template_searchpath=["/opt/airflow/include/sql"],
) as dag:
    
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    comp_info_task = SQLExecuteQueryOperator(
    task_id="comp_info_task",
    conn_id="sqlite_conn",
    sql = ql.getQueryString("comp_info_kor") ,
    do_xcom_push=True
)
    
    def get_news_data_by_keyword(naver_id,naver_key,**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='comp_info_task')
        keyword_list = [row[0] for row in data]
        df = get_news_data(keyword_list,naver_id,naver_key).reset_index(drop=True).drop_duplicates()

        def escape_sql(val):
            if val is None:
                return ''
            return str(val).replace("'", "''") # SQLite에서는 '를 ''로 escape

        values_str = ", ".join([
                f"('{escape_sql(row['title'])}', "
                f"'{escape_sql(row['originallink'])}', "
                f"'{escape_sql(row['link'])}', "
                f"'{escape_sql(row['description'])}', "
                f"'{escape_sql(row['pubDate'])}', "
                f"'{escape_sql(row['image'])}', "
                f"'{escape_sql(row['content'])}', "
                f"'{escape_sql(row['media'])}', "
                f"'{escape_sql(row['reporter_nm'])}', "
                f"'{escape_sql(row['reporter_email'])}', "
                f"'{escape_sql(row['comp'])}')"
                for idx, row in df.iterrows()
            ])

        print(f"News Count : {len(df)}")
        print(df.columns)

        ti.xcom_push(key='values_str', value=values_str)
    
    get_news_task = PythonOperator(
    task_id='get_news_task',
    python_callable=get_news_data_by_keyword,
    op_kwargs={'naver_key': naver_key,'naver_id':naver_id},
    do_xcom_push=True
)
    
    # insert task
    insert_news_task = SQLExecuteQueryOperator(
        task_id='insert_news_task',
        conn_id='sqlite_conn',
        sql = ql.getQueryString("insert_news") +' '+ '{{ ti.xcom_pull(task_ids="get_news_task", key="values_str") }}'
    )



start_dag >> comp_info_task >> get_news_task >>insert_news_task >> end_dag

