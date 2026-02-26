import logging
from modules.monitor.resolver import SymbolResolver

logging.basicConfig(level=logging.INFO)

test_cases = [
    "中芯国际",
    "688981", # 科创板
    "00700",  # 港股
    "AAPL"    # 美股
]

for tc in test_cases:
    print(f"\n--- Testing: {tc} ---")
    res = SymbolResolver.resolve(tc)
    print(f"Result: {res}")
