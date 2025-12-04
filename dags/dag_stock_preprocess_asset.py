from airflow.sdk import asset
from airflow import Asset
from airflow.decorators import task
import airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

import pandas as pd
import sqlite3
import numpy as np
import os
from datetime import datetime

raw_stock_asset = Asset(
    uri = "/opt/airflow/db/project.db",
    extra={"tag": "stock"},
)

processed_stock_asset = Asset(
    uri = "/opt/airflow/db/lstm/stock.csv",
    extra={"tag": "stock"},
)

# # ensure output path
# os.makedirs(LSTM_OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------
# 1) Load raw stock data from SQLite
# ---------------------------------------------------------
@task
def load_data_from_sqlite(src_asset,trg_asset):
    conn = sqlite3.connect(src_asset.uri)

    query = """
        SELECT Date, Ticker, Open, High, Low, Close, Volume
        FROM stock_prices
        ORDER BY Ticker, Date
    """

    df = pd.read_sql(query, conn)
    conn.close()

    df["Date"] = pd.to_datetime(df["Date"])
    df.limit(50).to_csv(trg_asset.uri)

raw_asset_task = load_data_from_sqlite(raw_stock_asset,processed_stock_asset)
