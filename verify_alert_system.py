import logging
import os
import httpx
from core.agent import Tools, agent_executor

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock Data
SYMBOL = "GOOG"
NAME = "Alphabet"
PCT = 4.14
PRICE = 176.5
DIRECTION = "📈 暴涨"

def test_search_and_analysis():
    print("\n=== 1. Testing Search & Analysis ===")
    
    # 1. Search
    query = f"{NAME} {SYMBOL} 今日 重大新闻 股价异动原因 Gemini 3.1"
    print(f"Searching: {query}...")
    try:
        web_news = Tools.web_search(query)
        print(f"\n[Search Result Preview]:\n{web_news[:500]}...\n")
        
        if "Gemini" not in web_news and "3.1" not in web_news:
            print("⚠️ WARNING: Search result does NOT contain 'Gemini' or '3.1'.")
    except Exception as e:
        print(f"❌ Search Failed: {e}")
        return

    # 2. Analysis
    prompt = f"""
    【紧急任务】
    标的：{NAME} ({SYMBOL})
    状态：今日{DIRECTION} {PCT}%。
    
    【已知情报】
    {web_news}
    
    【要求】
    1. 仅根据【已知情报】分析原因。
    2. 如果情报中提到“Gemini 3.1”，必须作为核心原因。
    3. 如果情报中没有相关内容，直接回答“未搜到明确原因”。
    """
    print("Running LLM Analysis...")
    try:
        analysis = agent_executor.run(prompt)
        print(f"\n[LLM Analysis]:\n{analysis}\n")
    except Exception as e:
        print(f"❌ Analysis Failed: {e}")

def test_push():
    print("\n=== 2. Testing Telegram Push ===")
    
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = 5573627236 # Your chat_id
    
    if not token:
        print("❌ TELEGRAM_BOT_TOKEN is missing in env!")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "🚨 **验证测试**\n这是 verify_alert_system.py 发送的消息。\n如果收到，说明推送链路正常。\n(No Markdown Mode)",
        # No parse_mode
    }
    
    print(f"Sending to {url}...")
    try:
        resp = httpx.post(url, json=payload, timeout=10)
        print(f"Response Code: {resp.status_code}")
        print(f"Response Body: {resp.text}")
        
        if resp.status_code == 200:
            print("✅ Push Success!")
        else:
            print("❌ Push Failed!")
    except Exception as e:
        print(f"❌ Push Exception: {e}")

if __name__ == "__main__":
    test_search_and_analysis()
    test_push()