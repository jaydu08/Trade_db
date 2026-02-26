import pandas as pd
import akshare as ak
from modules.ingestion.akshare_client import akshare_client
from modules.analysis.heatmap import MarketHeatMap

# 1. 检查 HK 行情
print("Fetching HK spots...")
try:
    df_hk = akshare_client.get_stock_info_hk()
    print("HK Columns:", df_hk.columns)
    if not df_hk.empty:
        print(df_hk.head(1).to_dict('records'))
    
    # test heatmap generator
    hm = MarketHeatMap()
    res = hm._generate_heatmap(df_hk, "HK", 5, 50000000)
    print("HK Heatmap Results len:", len(res))
    if len(res) == 0:
        print("HK Heatmap empty! Why?")
except Exception as e:
    print("HK Error:", e)

# 2. 检查 A 行情
print("\nFetching A spots...")
try:
    df_cn = akshare_client.get_realtime_quotes()
    print("CN Columns:", df_cn.columns)
    if not df_cn.empty:
        print(df_cn.head(1).to_dict('records'))
        
    hm = MarketHeatMap()
    res = hm._generate_heatmap(df_cn, "CN", 5, 50000000)
    print("CN Heatmap Results len:", len(res))
except Exception as e:
    print("CN Error:", e)
