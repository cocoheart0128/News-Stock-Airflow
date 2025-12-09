# import os
# import logging
# import sqlite3
# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt

# import tensorflow as tf
# from tensorflow.keras.models import Sequential
# from tensorflow.keras.layers import LSTM, Dense

# import mlflow
# import mlflow.tensorflow

# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.standard.operators.empty import EmptyOperator
# from airflow.utils.dates import days_ago

# # ==========================
# # CONFIG
# # ==========================
# DB_PATH = "/opt/airflow/db/project.db"
# SOURCE_TABLE = "stock_daily"
# OUTPUT_DIR = "/opt/airflow/output"
# MLFLOW_DIR = "/opt/airflow/mlflow"

# SEQ_LEN = 60
# EPOCHS = 10
# BATCH = 16

# # ==========================
# # Helper Function
# # ==========================

# def query_available_tickers():
#     conn = sqlite3.connect(DB_PATH)
#     df = pd.read_sql(f"SELECT DISTINCT Ticker FROM {SOURCE_TABLE}", conn)
#     conn.close()
#     df.columns = ['Ticker']
#     tickers = df["Ticker"].tolist()
#     logging.info(f"Tickers detected: {tickers}")
#     return tickers


# def load_stock_data(ticker):
#     conn = sqlite3.connect(DB_PATH)
#     df = pd.read_sql(
#         f"""
#         SELECT Date, Close
#         FROM {SOURCE_TABLE}
#         WHERE ticker='{ticker}'
#         ORDER BY date ASC
#         """,
#         conn
#     )
#     conn.close()

#     df.columns = ['Date','Ticker']
#     df["Date"] = pd.to_datetime(df["Date"])
#     logging.info(f"{ticker} Loaded rows: {len(df)}")
#     return df


# def make_sequences(data, seq_len=SEQ_LEN):
#     X, y = [], []
#     for i in range(len(data) - seq_len):
#         X.append(data[i:i+seq_len])
#         y.append(data[i+seq_len])
#     return np.array(X), np.array(y)


# def train_lstm(df, ticker):
#     # Only Close price used
#     values = df["Close"].values.reshape(-1, 1)

#     # Normalize
#     from sklearn.preprocessing import MinMaxScaler
#     scaler = MinMaxScaler()
#     scaled = scaler.fit_transform(values)

#     # Sequence
#     X, y = make_sequences(scaled)
#     X = X.reshape(X.shape[0], X.shape[1], 1)

#     # Train/Test split
#     split = int(len(X) * 0.8)
#     X_train, X_test = X[:split], X[split:]
#     y_train, y_test = y[:split], y[split:]

#     # ======================
#     # MLflow Tracking Start
#     # ======================
#     mlflow.set_tracking_uri(f"file://{MLFLOW_DIR}")
#     mlflow.set_experiment("stock_lstm")

#     with mlflow.start_run(run_name=ticker):
#         model = Sequential([
#             LSTM(50, return_sequences=False, input_shape=(SEQ_LEN, 1)),
#             Dense(1)
#         ])

#         model.compile(optimizer="adam", loss="mse")
#         history = model.fit(
#             X_train, y_train,
#             validation_data=(X_test, y_test),
#             epochs=EPOCHS,
#             batch_size=BATCH,
#             verbose=0
#         )

#         # Predict test
#         pred = model.predict(X_test)
#         pred = scaler.inverse_transform(pred)
#         actual = scaler.inverse_transform(y_test.reshape(-1, 1))

#         # Log params
#         mlflow.log_param("ticker", ticker)
#         mlflow.log_param("epochs", EPOCHS)
#         mlflow.log_param("seq_len", SEQ_LEN)

#         # Log metrics
#         from sklearn.metrics import mean_squared_error
#         mse = mean_squared_error(actual, pred)
#         mlflow.log_metric("mse", mse)

#         # Save model
#         mlflow.tensorflow.log_model(model, artifact_path="model")

#         # ========================
#         # Save Graph
#         # ========================
#         os.makedirs(OUTPUT_DIR, exist_ok=True)

#         plt.figure(figsize=(12, 4))
#         plt.plot(actual, label="Actual")
#         plt.plot(pred, label="Prediction")
#         plt.title(f"LSTM Prediction — {ticker}")
#         plt.legend()

#         graph_path = f"{OUTPUT_DIR}/{ticker}_prediction.png"
#         plt.savefig(graph_path)
#         plt.close()

#         mlflow.log_artifact(graph_path)

#         logging.info(f"[{ticker}] Training done — MSE: {mse} — Graph saved: {graph_path}")

#     return True


# # ==========================
# # Airflow Task Functions
# # ==========================

# def run_training(**context):
#     tickers = query_available_tickers()

#     for t in tickers:
#         df = load_stock_data(t)
#         if len(df) < SEQ_LEN + 10:
#             logging.warning(f"[{t}] Not enough data. Skipped.")
#             continue

#         train_lstm(df, t)


# # ==========================
# # DAG
# # ==========================

# default_args = {
#     "owner": "airflow",
#     "start_date": days_ago(1),
#     "retries": 1
# }

# with DAG(
#     dag_id="stock_lstm_pipeline",
#     schedule="@daily",
#     catchup=False,
#     default_args=default_args,
#     tags=["stock", "lstm", "mlflow"]
# ) as dag:

#     start = EmptyOperator(task_id="start")

#     run_all = PythonOperator(
#         task_id="train_all_tickers",
#         python_callable=run_training
#     )

#     end = EmptyOperator(task_id="end")

#     start >> run_all >> end