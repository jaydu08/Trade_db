import logging
from modules.monitor.scanner import MonitorService

logging.basicConfig(level=logging.INFO)

# 1. 模拟非交易时间
status_cn = MonitorService._is_market_open("CN")
status_us = MonitorService._is_market_open("US")
status_hk = MonitorService._is_market_open("HK")

print(f"Current time is 'open' for CN? {status_cn}")
print(f"Current time is 'open' for HK? {status_hk}")
print(f"Current time is 'open' for US? {status_us}")

print("Test complete.")
