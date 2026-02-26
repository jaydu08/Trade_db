import akshare as ak
import pandas as pd
import os

fallback_path = "data/cache/last_us_spot.pkl"
os.makedirs(os.path.dirname(fallback_path), exist_ok=True)

print("Fetching a small subset of US stocks using slow API...")
# The slow API actually returns a dataframe, we can just fetch it once.
try:
    df = ak.stock_us_spot() # we might need to wait for it or just mock it
    df.to_pickle(fallback_path)
    print("Saved fallback cache.")
except Exception as e:
    print(f"Error fetching US stocks: {e}")
    # mock it
    df = pd.DataFrame({
        "名称": ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"],
        "代码": ["aapl", "tsla", "nvda", "msft", "googl"],
        "最新价": [150, 200, 300, 400, 100],
        "涨跌额": [5, 15, 20, 2, -1],
        "涨跌幅": [5.5, 8.0, 7.5, 0.5, -1.0],
        "成交额": [50000000, 60000000, 70000000, 10000000, 20000000],
        "换手率": [1.5, 2.0, 3.5, 0.5, 0.1]
    })
    df.to_pickle(fallback_path)
    print("Created mock fallback cache.")
