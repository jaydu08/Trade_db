import logging
from modules.monitor.scanner import MonitorService
from modules.monitor.notifier import Notifier
import sys

def mock_send(chat_id, text):
    with open("llm_output.txt", "w") as f:
        f.write(text)
    print("Output written to llm_output.txt")

Notifier.send_telegram = mock_send

logging.getLogger('modules.monitor.scanner').setLevel(logging.WARNING)
logging.getLogger('core.agent').setLevel(logging.WARNING)

item = {'symbol': 'NVDA', 'name': '英伟达', 'market': 'US', 'chat_id': 8246272632}
quote = {'pct_chg': 6.5, 'price': 130.5}
print("Starting analysis...")
MonitorService._analyze_and_report(item, quote, "📈 暴涨")
