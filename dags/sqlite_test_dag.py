import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# providers version 확인
from airflow.providers.sqlite import __version__ as sqlite_provider_version
from airflow.providers.common.sql import __version__ as sql_common_provider_version
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime
# print("Airflow version:", airflow.__version__)
# print("SQLite provider version:", sqlite_provider_version)
# print("Common SQL provider version:", sql_common_provider_version)

with DAG(
    dag_id="sqlite_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_sql = SQLExecuteQueryOperator(
        task_id="test_sql_query",
        conn_id="sqlite_conn",   # Airflow UI에서 등록한 Connection ID
        sql="SELECT COUNT(*) AS cnt FROM tickers_info;",
        do_xcom_push=True
    )

    def print_count(ti):
        result = ti.xcom_pull(task_ids="test_sql_query")
        print("Row count:", result)

    print_task = PythonOperator(
        task_id="print_result",
        python_callable=print_count
    )
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    start_dag >> test_sql >> print_task >> end_dag