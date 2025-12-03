import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import datetime

# DB 연결
DB_PATH = "project.db"  # SQLite 파일 경로
conn = sqlite3.connect(DB_PATH)

st.set_page_config(
    page_title="Finance Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 Finance Dashboard")
st.markdown("주식, 환율, 지수, 뉴스 통계를 한눈에 확인")

# Sidebar - 선택
st.sidebar.header("필터")
start_date = st.sidebar.date_input("Start Date", value=datetime(2025, 1, 1))
end_date = st.sidebar.date_input("End Date", value=datetime.now())
companies = st.sidebar.multiselect(
    "회사 선택", 
    pd.read_sql("SELECT DISTINCT Ticker FROM stock_prices", conn)["Ticker"].tolist(),
    default=None
)

# ================== 주식 시각화 ==================
st.header("📈 주식 시계열 비교")

query = f"""
SELECT Date, Ticker, Close, market_cap
FROM stock_prices
WHERE Date BETWEEN '{start_date}' AND '{end_date}'
"""

if companies:
    query += f" AND Ticker IN ({','.join([f'\"{c}\"' for c in companies])})"

df_stock = pd.read_sql(query, conn)
df_stock["Date"] = pd.to_datetime(df_stock["Date"])

if not df_stock.empty:
    fig_stock = px.line(
        df_stock, x="Date", y="Close", color="Ticker",
        markers=True, title="주식 종가 비교"
    )
    st.plotly_chart(fig_stock, use_container_width=True)

    st.dataframe(df_stock.groupby("Ticker").agg(
        latest_close=("Close", "last"),
        market_cap=("market_cap", "last"),
        mean_close=("Close", "mean")
    ).reset_index())
else:
    st.warning("선택된 기간/회사의 데이터가 없습니다.")

# ================== 뉴스 통계 ==================
st.header("📰 뉴스 통계")
query_news = f"""
SELECT comp, COUNT(*) as news_count
FROM tb_naver_news
WHERE insert_dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY comp
"""
df_news = pd.read_sql(query_news, conn)

if not df_news.empty:
    fig_news = px.bar(
        df_news, x="comp", y="news_count",
        title="회사별 뉴스 기사 수", text="news_count"
    )
    st.plotly_chart(fig_news, use_container_width=True)
    st.dataframe(df_news)
else:
    st.warning("뉴스 데이터가 없습니다.")

# ================== 환율 / 지수 ==================
st.header("💱 환율 & 지수")

# 환율
df_exchange = pd.read_sql(f"""
SELECT Date, currency, rate
FROM exchange_rates
WHERE Date BETWEEN '{start_date}' AND '{end_date}'
""", conn)
df_exchange["Date"] = pd.to_datetime(df_exchange["Date"])

if not df_exchange.empty:
    fig_ex = px.line(df_exchange, x="Date", y="rate", color="currency", title="환율 추이")
    st.plotly_chart(fig_ex, use_container_width=True)

# 지수
df_index = pd.read_sql(f"""
SELECT Date, index_name, close
FROM index_values
WHERE Date BETWEEN '{start_date}' AND '{end_date}'
""", conn)
df_index["Date"] = pd.to_datetime(df_index["Date"])

if not df_index.empty:
    fig_idx = px.line(df_index, x="Date", y="close", color="index_name", title="지수 추이")
    st.plotly_chart(fig_idx, use_container_width=True)

conn.close()
