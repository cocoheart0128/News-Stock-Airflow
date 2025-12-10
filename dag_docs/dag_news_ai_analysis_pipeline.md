# 뉴스 AI 분석 파이프라인 (Airflow DAG)

## 개요

`dag_news_ai_analysis_pipeline`은 **뉴스 데이터를 수집 → 임베딩 → 유사도 분석 → AI 요약 및 감성 분석 → DB 저장**까지 자동화하는 **AI 기반 뉴스 분석 파이프라인**입니다.
Airflow DAG를 활용해 **일일 단위 배치 처리**가 가능하며, Gemini API와 로컬 VectorDB(LanceDB)를 활용합니다.

---

## DAG 정보

* **DAG ID**: `dag_news_ai_analysis_pipeline`
* **스케줄**: `@daily`
* **시작일**: `2025-12-08`
* **타임존**: `Asia/Seoul`
* **태그**: `NEWS`, `AI`
* **템플릿 검색 경로**: `/opt/airflow/include/sql`

<img width="1512" height="982" alt="스크린샷 2025-12-10 오후 2 39 07" src="https://github.com/user-attachments/assets/ff96d6bd-23b7-482f-9eef-c819e1061421" />


---

## 주요 설정

```python
etl_date = "{{ (macros.datetime.strptime(ds, '%Y-%m-%d')).strftime('%Y-%m-%d') }}"
gemini_api_key = Variable.get("gemini_api_key")
DB_PATH = "/opt/airflow/db/project.db"
VectorDB_NAME ='/opt/airflow/db/vector'
VectorDB_TB_NAME = 'tb_naver_news_ai_emb'
ql = QueryLoader("ai_news_info.json")
service = AiProcess(api_key=gemini_api_key, vector_db_path=VectorDB_NAME)
```

* **ETL 날짜**: DAG 실행 날짜 기준
* **Gemini API Key**: Airflow Variable에서 가져오기
* **DB 경로**: SQLite + LanceDB
* **QueryLoader**: SQL 쿼리 관리
* **AiProcess**: AI 임베딩, 유사도, 요약 기능 래퍼

---

## DAG Task 흐름

```
start_dag
    │
    ▼
seq_info_task
    │
    ▼
fork_embed_dag
    │
    ▼
news_embedding_process_task
    │
    ▼
news_similarity_process_task
    │
    ▼
news_similarity_save_task
    │
    ▼
news_similarity_filter_task
    │
    ▼
fork_analysis_dag
    │
    ▼
news_ai_summary_process_task
    │
    ▼
news_ai_summary_insert_task
    │
    ▼
end_dag
```

---

## Task 상세 설명

### 1. `seq_info_task` (SQLExecuteQueryOperator)

* **기능**: 처리되지 않은 뉴스 `seq` 정보 조회
* **쿼리 파일**: `ai_news_info.json` → `"unprocess_news_seq"`

---

### 2. `news_embedding_process_task` (PythonOperator)

* **기능**:

  * 뉴스 제목/내용을 가져와 AI 임베딩 수행
  * 임베딩 결과를 VectorDB(LanceDB)에 저장
* **사용 함수**: `service.safe_local_batch_embedding()`

---

### 3. `news_similarity_process_task` (PythonOperator)

* **기능**:

  * 신규 임베딩 뉴스와 기존 뉴스간 유사도 계산
  * 결과를 XCom에 저장
* **사용 함수**: `service.news_similarity_process()`, `service.df_to_sql_values_string()`

---

### 4. `news_similarity_save_task` (SQLExecuteQueryOperator)

* **기능**: XCom으로 전달된 유사도 결과를 DB에 저장
* **쿼리 파일**: `"insert_similarity_result"`

---

### 5. `news_similarity_filter_task` (SQLExecuteQueryOperator)

* **기능**: 특정 날짜 기준으로 대표 뉴스 필터링
* **쿼리 파일**: `"filter_news_seq"`

---

### 6. `news_ai_summary_process_task` (PythonOperator)

* **기능**:

  * 필터링된 뉴스에 대해 AI 요약, 감성 분석 수행
  * Gemini API 호출 → 키워드/요약/감성/적합도 평가 결과 생성
* **사용 함수**: `service.gemini_ai_summary()`

---

### 7. `news_ai_summary_insert_task` (SQLExecuteQueryOperator)

* **기능**: AI 분석 결과를 DB에 저장
* **쿼리 파일**: `"insert_ai_news_result"`

---

## 핵심 포인트

* **XCom 활용**: PythonOperator에서 생성된 임베딩/유사도/AI 분석 결과를 다음 Task에 전달
* **Fork 구조**: `fork_embed_dag`, `fork_analysis_dag`를 통해 병렬 처리 확장 가능
* **Gemini API 제한 회피**: `safe_local_batch_embedding()` 사용
* **VectorDB**: 뉴스 임베딩 저장 및 유사도 검색 최적화
