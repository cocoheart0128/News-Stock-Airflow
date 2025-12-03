import streamlit as st
import pandas as pd
import sqlite3
import os
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm


# =======================
# font 깨짐 방지
# =======================
# # 설치한 나눔폰트 사용
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
fontpath = os.path.join(BASE_DIR, "fonts", "NanumGothic-Bold.ttf")
fontprop = fm.FontProperties(fname=fontpath)
plt.rcParams['font.family'] = fontprop.get_name()
plt.rcParams['axes.unicode_minus'] = False  # 마이너스 깨짐 방지

# =======================
# DB 연결
# =======================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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
st.markdown("주식, 뉴스, 환율, 지수를 한눈에 비교할 수 있는 대시보드입니다.")

# =======================
# 선택 옵션
# =======================
tickers = stock_df["Ticker"].unique().tolist()
selected_tickers = st.multiselect("회사 선택", tickers, default=tickers[:3])

# currencies = exchange_df["Currency"].unique().tolist()
# selected_currency = st.multiselect("통화 선택", currencies, default=currencies[:3])

# indices = index_df["IndexName"].unique().tolist()
# selected_index = st.multiselect("지수 선택", indices, default=indices[:3])

date_min = min(stock_df["Date"].min(), exchange_df["Date"].min(), index_df["Date"].min())
date_max = max(stock_df["Date"].max(), exchange_df["Date"].max(), index_df["Date"].max())
start_date, end_date = st.date_input("기간 선택", [date_min, date_max], min_value=date_min, max_value=date_max)

# =======================
# 데이터 필터링
# =======================
filtered_stock = stock_df[
    (stock_df["Ticker"].isin(selected_tickers)) &
    (stock_df["Date"] >= pd.to_datetime(start_date)) &
    (stock_df["Date"] <= pd.to_datetime(end_date))
]

# filtered_news = news_df[news_df["comp"].isin(selected_tickers)]
# filtered_exchange = exchange_df[
#     (exchange_df["Currency"].isin(selected_currency)) &
#     (exchange_df["Date"] >= pd.to_datetime(start_date)) &
#     (exchange_df["Date"] <= pd.to_datetime(end_date))
# ]

# filtered_index = index_df[
#     (index_df["IndexName"].isin(selected_index)) &
#     (index_df["Date"] >= pd.to_datetime(start_date)) &
#     (index_df["Date"] <= pd.to_datetime(end_date))
# ]

# =======================
# 주식 비교 그래프
# =======================
st.header("💹 주식 비교")
fig, ax = plt.subplots(figsize=(12,4))
for ticker in selected_tickers:
    df = filtered_stock[filtered_stock["Ticker"]==ticker]
    sns.lineplot(data=df, x="Date", y="Close", ax=ax, label=ticker)
ax.set_xlabel("날짜")
ax.set_ylabel("종가")
ax.grid(True, linestyle="--", alpha=0.5)
ax.legend(title="회사")
st.pyplot(fig, use_container_width=True)

# # =======================
# # 뉴스 건수 비교
# # =======================
# st.header("📰 뉴스 건수 비교")
# news_count = filtered_news.groupby("comp").size().reset_index(name="count")
# fig, ax = plt.subplots(figsize=(8,3))
# sns.barplot(data=news_count, x="comp", y="count", palette="pastel", ax=ax)
# ax.set_xlabel("회사")
# ax.set_ylabel("뉴스 건수")
# ax.grid(axis="y", linestyle="--", alpha=0.5)
# st.pyplot(fig, use_container_width=True)

# # =======================
# # 환율 비교 그래프
# # =======================
# st.header("💱 환율 비교")
# fig, ax = plt.subplots(figsize=(12,4))
# for curr in selected_currency:
#     df = filtered_exchange[filtered_exchange["Currency"]==curr]
#     sns.lineplot(data=df, x="Date", y="Rate", ax=ax, label=curr)
# ax.set_xlabel("날짜")
# ax.set_ylabel("환율")
# ax.grid(True, linestyle="--", alpha=0.5)
# ax.legend(title="통화")
# st.pyplot(fig, use_container_width=True)

# # =======================
# # 지수 비교 그래프
# # =======================
# st.header("📈 지수 비교")
# fig, ax = plt.subplots(figsize=(12,4))
# for idx_name in selected_index:
#     df = filtered_index[filtered_index["IndexName"]==idx_name]
#     sns.lineplot(data=df, x="Date", y="Value", ax=ax, label=idx_name)
# ax.set_xlabel("날짜")
# ax.set_ylabel("지수 값")
# ax.grid(True, linestyle="--", alpha=0.5)
# ax.legend(title="지수")
# st.pyplot(fig, use_container_width=True)