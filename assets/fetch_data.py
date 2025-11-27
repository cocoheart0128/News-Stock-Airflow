from airflow.assets import asset
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


@asset(
    name="fetch_stock_data",
    description="Fetch OHLCV stock data using yfinance"
)
def fetch_stock_data() -> pd.DataFrame:
    tickers = ["AAPL", "MSFT", "GOOGL"]  # 원하는 티커 추가
    
    end = datetime.today()
    start = end - timedelta(days=30)

    frames = []
    for t in tickers:
        df = yf.download(t, start=start, end=end)
        df["ticker"] = t
        frames.append(df.reset_index())

    final_df = pd.concat(frames, ignore_index=True)
    return final_df
