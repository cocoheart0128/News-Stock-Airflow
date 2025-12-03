import streamlit as st
import pandas as pd
import sqlite3
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# =======================
# DB 연결
# =======================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# database/news.db 경로
DB_PATH = os.path.join(BASE_DIR, "db", "project.db")
conn = sqlite3.connect(DB_PATH)

# =======================
# 데이터 로딩
# =======================
@st.cache_data
def load_data():
    stock_df = pd.read_sql("SELECT * FROM stock_prices", conn, parse_dates=["Date"])
    exchange_df = pd.read_sql("SELECT * FROM exchange_rates", conn, parse_dates=["Date"])
    index_df = pd.read_sql("SELECT * FROM index_values", conn, parse_dates=["Date"])
    news_df = pd.read_sql("SELECT * FROM tb_naver_news", conn, parse_dates=["pubDate"])
    return stock_df, exchange_df, index_df, news_df

stock_df, exchange_df, index_df, news_df = load_data()

# =======================
# Streamlit 설정
# =======================
st.set_page_config(page_title="금융 대시보드", layout="wide")
st.title("📊 금융 데이터 대시보드")
st.markdown("주식, 뉴스, 환율, 지수를 카드형 레이아웃으로 확인할 수 있습니다.")

# =======================
# 선택 옵션
# =======================
tickers = stock_df["Ticker"].unique().tolist()
selected_tickers = st.multiselect("회사 선택", tickers, default=tickers[:3])

date_min = stock_df["Date"].min()
date_max = stock_df["Date"].max()
start_date, end_date = st.date_input("기간 선택", [date_min, date_max], min_value=date_min, max_value=date_max)

# =======================
# 필터링
# =======================
filtered_stock = stock_df[
    (stock_df["Ticker"].isin(selected_tickers)) &
    (stock_df["Date"] >= pd.to_datetime(start_date)) &
    (stock_df["Date"] <= pd.to_datetime(end_date))
]

filtered_news = news_df[news_df["comp"].isin(selected_tickers)]
filtered_exchange = exchange_df.copy()
filtered_index = index_df.copy()

# =======================
# 주식 카드
# =======================
st.header("💹 주식 데이터")
cols = st.columns(len(selected_tickers))
for i, ticker in enumerate(selected_tickers):
    last_close = filtered_stock[filtered_stock["Ticker"]==ticker]["Close"].iloc[-1]
    fig, ax = plt.subplots(figsize=(4,2))
    sns.lineplot(
        data=filtered_stock[filtered_stock["Ticker"]==ticker], 
        x="Date", y="Close", ax=ax, color="#1f77b4"
    )
    ax.set_title(ticker)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.grid(False)
    st.pyplot(fig, use_container_width=True)

# =======================
# 뉴스 카드
# =======================
st.header("📰 뉴스 데이터")
news_count = filtered_news.groupby("comp").size().reset_index(name="count")
cols = st.columns(len(selected_tickers))
for i, ticker in enumerate(selected_tickers):
    comp_count = news_count[news_count["comp"]==ticker]["count"].values
    comp_count = int(comp_count[0]) if len(comp_count) > 0 else 0
    with cols[i]:
        st.metric(label=f"{ticker} 뉴스 건수", value=comp_count)
        recent_news = filtered_news[filtered_news["comp"]==ticker].sort_values("pubDate", ascending=False).head(3)
        for idx, row in recent_news.iterrows():
            st.markdown(f"- [{row['title']}]({row['link']})")

# =======================
# 환율 카드
# =======================
st.header("💱 환율 데이터")
currencies = exchange_df["Currency"].unique()
selected_currency = st.multiselect("통화 선택", currencies, default=currencies[:3])
filtered_exchange = exchange_df[exchange_df["Currency"].isin(selected_currency)]

cols = st.columns(len(selected_currency))
for i, curr in enumerate(selected_currency):
    last_rate = filtered_exchange[filtered_exchange["Currency"]==curr]["Rate"].iloc[-1]
    fig, ax = plt.subplots(figsize=(4,2))
    sns.lineplot(
        data=filtered_exchange[filtered_exchange["Currency"]==curr],
        x="Date", y="Rate", ax=ax, color="#ff7f0e"
    )
    ax.set_title(curr)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.grid(False)
    st.pyplot(fig, use_container_width=True)

# =======================
# 지수 카드
# =======================
st.header("📈 지수 데이터")
indices = index_df["IndexName"].unique()
selected_index = st.multiselect("지수 선택", indices, default=indices[:3])
filtered_index = index_df[index_df["IndexName"].isin(selected_index)]

cols = st.columns(len(selected_index))
for i, idx_name in enumerate(selected_index):
    last_value = filtered_index[filtered_index["IndexName"]==idx_name]["Value"].iloc[-1]
    fig, ax = plt.subplots(figsize=(4,2))
    sns.lineplot(
        data=filtered_index[filtered_index["IndexName"]==idx_name],
        x="Date", y="Value", ax=ax, color="#2ca02c"
    )
    ax.set_title(idx_name)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.grid(False)
    st.pyplot(fig, use_container_width=True)