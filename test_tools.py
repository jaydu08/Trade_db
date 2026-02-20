import logging
import json
import requests
from duckduckgo_search import DDGS
from modules.ingestion.akshare_client import akshare_client

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bocha_fixed():
    print("\n--- Testing Bocha AI (Fixed) ---")
    url = "https://api.bochaai.com/v1/web-search"
    headers = {
        "Authorization": "Bearer sk-996761b2cea840f7a68cf72840f1642c",
        "Content-Type": "application/json"
    }
    # Minimal Payload
    payload = {
        "query": "OpenAI Sora",
        "count": 1
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"Data: {str(data)[:200]}...")
            if data.get('code') == 200:
                print("✅ Bocha: Success")
            else:
                print(f"❌ Bocha Logic Error: {data}")
        else:
            print(f"❌ Bocha HTTP Error: {resp.text}")
    except Exception as e:
        print(f"❌ Bocha Network Error: {e}")

def test_akshare_fixed():
    print("\n--- Testing Akshare (Fixed) ---")
    # A股
    try:
        d = akshare_client.get_realtime_quote_sina("600519", "CN")
        print(f"CN (600519): {d.get('price')} @ {d.get('timestamp')}")
    except: pass
    
    # 港股
    try:
        d = akshare_client.get_realtime_quote_sina("00700", "HK")
        print(f"HK (00700): {d.get('price')} @ {d.get('timestamp')}")
    except: pass

    # 美股
    try:
        d = akshare_client.get_realtime_quote_sina("NVDA", "US")
        print(f"US (NVDA): {d.get('price')} @ {d.get('timestamp')}")
    except: pass

if __name__ == "__main__":
    test_bocha_fixed()
    test_akshare_fixed()