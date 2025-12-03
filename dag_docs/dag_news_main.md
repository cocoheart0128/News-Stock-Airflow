<img width="1512" height="982" alt="스크린샷 2025-12-03 오후 1 38 46" src="https://github.com/user-attachments/assets/d6b3146b-305b-4b31-8534-b1938bd25805" />


DAG: dag_news_main
개요

목적: 지정된 기업 리스트를 기반으로 Naver 뉴스 데이터를 수집하고 SQLite DB에 저장.

주요 기능:

기업 정보 조회 (comp_info_task)

뉴스 데이터 수집 (get_news_task)

뉴스 데이터 파싱 및 DB 삽입 (insert_news_task)

스케줄: 매일 09:00 한 번 실행 (schedule="0 9 * * *")

DB: SQLite (sqlite_conn)

DAG 구성
1️⃣ 시작 / 종료 Task

start_dag: DAG 시작 표시 (EmptyOperator)

end_dag: DAG 종료 표시 (EmptyOperator)

2️⃣ 기업 정보 조회

Task ID: comp_info_task

Operator: SQLExecuteQueryOperator

설명: ticker_info.json 또는 comp_info_kor 쿼리를 통해 기업 리스트 조회

XCom Push: do_xcom_push=True → 이후 Task에서 키워드 리스트 사용

comp_info_task = SQLExecuteQueryOperator(
    task_id="comp_info_task",
    conn_id="sqlite_conn",
    sql=ql.getQueryString("comp_info_kor"),
    do_xcom_push=True
)

3️⃣ 뉴스 데이터 수집

Task ID: get_news_task

Operator: PythonOperator

설명: comp_info_task에서 기업 리스트를 가져와 Naver 뉴스 수집

XCom Push: 수집한 뉴스 데이터 문자열(values_str)을 push → insert_news_task에서 사용

Python Callable 예시:

def get_news_data_by_keyword(naver_id, naver_key, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='comp_info_task')
    keyword_list = [row[0] for row in data]

    df = get_news_data(keyword_list, naver_id, naver_key).sample(5).reset_index(drop=True)

    values_str = ", ".join([
        f"('{row['title']}', '{row['originallink']}', '{row['link']}', '{row['description']}', '{row['pubDate']}', '{row['image']}', '{row['content']}', '{row['media']}', '{row['reporter_nm']}', '{row['reporter_email']}', '{row['comp']}')"
        for idx, row in df.iterrows()
    ])

    ti.xcom_push(key='values_str', value=values_str)


Operator 연결:

get_news_task = PythonOperator(
    task_id='get_news_task',
    python_callable=get_news_data_by_keyword,
    op_kwargs={'naver_id': naver_id, 'naver_key': naver_key}
)

4️⃣ 뉴스 DB 삽입

Task ID: insert_news_task

Operator: SQLExecuteQueryOperator

설명: XCom으로 전달받은 뉴스 데이터를 news 테이블에 insert

SQL 예시:

insert_news_task = SQLExecuteQueryOperator(
    task_id='insert_news_task',
    conn_id='sqlite_conn',
    sql=ql.getQueryString("insert_news") + ' ' +
        '{{ ti.xcom_pull(task_ids="get_news_task", key="values_str") }}'
)

DAG Flow
start_dag
    ↓
comp_info_task
    ↓
get_news_task
    ↓
insert_news_task
    ↓
end_dag

추가 사항

웹 요청 안전성: requests.get에서 ConnectionError 방지를 위해 safe_parsing_webpage 사용 권장

XCom 사용: values_str를 PythonOperator에서 SQLExecuteQueryOperator로 전달

스케줄: 매일 09:00 한 번 실행

예외 처리: 뉴스 수집 실패 시 빈 데이터 반환하여 DAG 실패 방지

