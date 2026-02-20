import logging
import json
import requests
from duckduckgo_search import DDGS
from modules.ingestion.akshare_client import akshare_client

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bocha_complex():
    print("\n--- Testing Bocha AI (Complex Query) ---")
    url = "https://api.bochaai.com/v1/web-search"
    headers = {
        "Authorization": "Bearer sk-996761b2cea840f7a68cf72840f1642c",
        "Content-Type": "application/json"
    }
    # 模拟 Agent 会生成的复杂 Query
    queries = [
        "国漫国潮 产业链图谱 2026 独角兽",
        "光线传媒 2025年 财报 营收",
        "Minimax 最新估值 2026"
    ]
    
    for q in queries:
        print(f"\nQuery: {q}")
        payload = {
            "query": q,
            "count": 5
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('code') == 200 and data.get('data'):
                    items = data['data'].get('webPages', {}).get('value', [])
                    print(f"✅ Found {len(items)} results")
                    if not items:
                        print("⚠️ WARNING: Response 200 but NO items found!")
                else:
                    print(f"❌ Bocha Logic Error: {data}")
            else:
                print(f"❌ Bocha HTTP Error: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"❌ Bocha Network Error: {e}")

if __name__ == "__main__":
    test_bocha_complex()