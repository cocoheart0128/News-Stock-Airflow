# DAG: dag_yfinance_pipeline

## 개요
- **목적**: YFinance를 기반으로 주식, 환율, 지수 관련 데이터를 수집하고 관련 DAG를 순차적으로 실행  
- **주요 기능**:
  1. DAG 시작/종료 표시 (`start_dag`, `end_dag`)  
  2. 주식, 환율, 지수 관련 DAG 실행 (`dag_stock`, `dag_exchange_rate`, `dag_index`)  
- **스케줄**: 매일 09:00 한 번 실행 (`schedule="0 9 * * *"`)  
- **DB**: SQLite (`DB_PATH = "/opt/airflow/db/project.db"`)  


<img width="1512" height="982" alt="스크린샷 2025-12-03 오후 1 38 25" src="https://github.com/user-attachments/assets/5d40f521-8b13-417a-9345-5a824878200f" />

---

## DAG 구성

### 1️⃣ 시작 / 종료 Task
- **`start_dag`**: DAG 시작 표시 (EmptyOperator)  
- **`end_dag`**: DAG 종료 표시 (EmptyOperator)  
- **`fork_stock_dag`**: 후속 DAG 분기 처리 (EmptyOperator)  

---

### 2️⃣ Trigger DAGs
- **TriggerDagRunOperator**를 활용하여 다른 DAG 실행
- **설정**: `wait_for_completion=True` → 해당 DAG가 끝날 때까지 대기

| Task ID | 실행 DAG | 설명 |
|---------|---------|------|
| `trigger_dag_stock` | `dag_stock` | 주식 관련 데이터 수집 및 처리 DAG |
| `trigger_dag_exchange` | `dag_exchange_rate` | 환율 관련 데이터 수집 및 처리 DAG |
| `trigger_dag_index` | `dag_index` | 지수 관련 데이터 수집 및 처리 DAG |

```python
trigger_dag_stock = TriggerDagRunOperator(
    task_id="trigger_dag_stock",
    trigger_dag_id="dag_stock",
    wait_for_completion=True,
)

trigger_dag_exchange = TriggerDagRunOperator(
    task_id="trigger_dag_exchange",
    trigger_dag_id="dag_exchange_rate",
    wait_for_completion=True,
)

trigger_dag_index = TriggerDagRunOperator(
    task_id="trigger_dag_index",
    trigger_dag_id="dag_index",
    wait_for_completion=True,
)
```
