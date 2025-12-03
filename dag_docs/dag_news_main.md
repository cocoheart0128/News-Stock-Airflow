# DAG: dag_news_main

## 개요
- **목적**: 지정된 기업 리스트를 기반으로 Naver 뉴스 데이터를 수집하고 SQLite DB에 저장.  
- **주요 기능**:
  1. 기업 정보 조회 (`comp_info_task`)
  2. 뉴스 데이터 수집 (`get_news_task`)
  3. 뉴스 데이터 파싱 및 DB 삽입 (`insert_news_task`)  
- **스케줄**: 매일 09:00 한 번 실행 (`schedule="0 9 * * *"`)  
- **DB**: SQLite (`sqlite_conn`)  

---
<img width="1512" height="982" alt="스크린샷 2025-12-03 오후 1 38 46" src="https://github.com/user-attachments/assets/d6b3146b-305b-4b31-8534-b1938bd25805" />

---

## DAG 구성

### 1️⃣ 시작 / 종료 Task
- **`start_dag`**: DAG 시작 표시 (EmptyOperator)  
- **`end_dag`**: DAG 종료 표시 (EmptyOperator)  

---

### 2️⃣ 기업 정보 조회
- **Task ID**: `comp_info_task`  
- **Operator**: `SQLExecuteQueryOperator`  
- **설명**: `ticker_info.json` 또는 `comp_info_kor` 쿼리를 통해 기업 리스트 조회  
- **XCom Push**: `do_xcom_push=True` → 이후 Task에서 키워드 리스트 사용  

```python
comp_info_task = SQLExecuteQueryOperator(
    task_id="comp_info_task",
    conn_id="sqlite_conn",
    sql=ql.getQueryString("comp_info_kor"),
    do_xcom_push=True
)

---
### 3️⃣ 뉴스 데이터 수집
- **Task ID**: `get_news_task`  
- **Operator**: `PythonOperator`  
- **설명**: `comp_info_task`에서 XCom으로 전달받은 기업 리스트를 기반으로 Naver 뉴스 API에서 뉴스 데이터 수집  
- **수집 항목**:
  - `title` : 뉴스 제목
  - `originallink` : 원본 링크
  - `link` : 네이버 뉴스 링크
  - `description` : 기사 요약
  - `pubDate` : 게시일
  - `image` : 대표 이미지
  - `content` : 뉴스 본문
  - `media` : 언론사
  - `reporter_nm` : 기자 이름
  - `reporter_email` : 기자 이메일
  - `comp` : 관련 기업

```python
def get_news_data_by_keyword(naver_id, naver_key, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='comp_info_task')
    keyword_list = [row[0] for row in data]

    df = get_news_data(keyword_list, naver_id, naver_key).sample(5).reset_index(drop=True)

    values_str = ", ".join([
        f"('{row['title']}', '{row['originallink']}', '{row['link']}', '{row['description']}', "
        f"'{row['pubDate']}', '{row['image']}', '{row['content']}', '{row['media']}', "
        f"'{row['reporter_nm']}', '{row['reporter_email']}', '{row['comp']}')"
        for idx, row in df.iterrows()
    ])

    ti.xcom_push(key='values_str', value=values_str)


