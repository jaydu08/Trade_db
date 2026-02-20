import logging
import json
import requests
from modules.ingestion.akshare_client import akshare_client

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_eastmoney_quote():
    print("\n--- Testing Eastmoney Quote ---")
    
    # 300251 光线传媒
    print("Querying 300251 (CN)...")
    try:
        # Explicitly call the NEW method
        d = akshare_client.get_realtime_quote_eastmoney("300251", "CN")
        print(f"Result: {d}")
    except AttributeError:
        print("❌ Error: get_realtime_quote_eastmoney method not found! (Old code loaded?)")
    except Exception as e:
        print(f"❌ Error: {e}")

    # 300043 星辉娱乐
    print("\nQuerying 300043 (CN)...")
    try:
        d = akshare_client.get_realtime_quote_eastmoney("300043", "CN")
        print(f"Result: {d}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_eastmoney_quote()