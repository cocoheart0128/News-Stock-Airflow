import airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
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
    dag_id="dag_yfinance_pipeline_backfill",
    start_date=datetime(2025,12,1),
    schedule="0 9 * * *",
    catchup=True,
) as dag:

    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    # Sub DAG 완료를 기다림 (backfill 가능)
    wait_stock = ExternalTaskSensor(
        task_id="wait_stock",
        external_dag_id="dag_stock",
        external_task_id="end_dag",  # dag_stock의 마지막 task
        mode="poke",
        poke_interval=10,
        timeout=600
    )

    wait_exchange = ExternalTaskSensor(
        task_id="wait_exchange",
        external_dag_id="dag_exchange_rate",
        external_task_id="end_dag",
        mode="poke",
        poke_interval=10,
        timeout=600
    )

    wait_index = ExternalTaskSensor(
        task_id="wait_index",
        external_dag_id="dag_index",
        external_task_id="end_dag",
        mode="poke",
        poke_interval=10,
        timeout=600
    )

    start_dag >> [wait_stock, wait_exchange, wait_index] >> end_dag