import logging
import httpx
from config.settings import TELEGRAM_BOT_TOKEN

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_push():
    chat_id = 5573627236 # From logs
    print(f"\n--- Testing Push to Chat ID: {chat_id} ---")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "🚨 **推送测试**\n这是来自 Trae 的手动测试消息。\n如果您收到此消息，说明推送链路畅通。",
        "parse_mode": "Markdown"
    }
    
    try:
        resp = httpx.post(url, json=payload, timeout=10)
        print(f"Response: {resp.status_code}")
        print(f"Body: {resp.text}")
        
        if resp.status_code == 200:
            print("✅ Push SUCCESS!")
        else:
            print("❌ Push FAILED!")
            
    except Exception as e:
        print(f"❌ Push Exception: {e}")

if __name__ == "__main__":
    test_push()