import akshare as ak
import pandas as pd
import time

def test_api(name, func):
    print(f"\n--- Testing {name} ---")
    try:
        df = func()
        print(f"Success! Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()[:10]}")
        return True
    except Exception as e:
        print(f"Failed: {e}")
        return False

# 1. 东财 A股
test_api("stock_zh_a_spot_em", ak.stock_zh_a_spot_em)

# 2. 新浪 A股
test_api("stock_zh_a_spot", ak.stock_zh_a_spot)

# 3. 腾讯 A股? 好像没有直接的全部A股

# 4. 港股东财
test_api("stock_hk_spot_em", ak.stock_hk_spot_em)
