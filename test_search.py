import logging
from core.agent import Tools

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_search():
    query = "Google GOOG 今日 重大新闻 股价异动原因"
    print(f"\n--- Testing Search for: '{query}' ---")
    
    # 1. Test Tools.web_search (Bocha/DDG Hybrid)
    try:
        result = Tools.web_search(query)
        print(f"\n[Tools.web_search Result]:\n{result[:1000]}...") # Print first 1000 chars
        
        if "Gemini" in result or "3.1" in result:
            print("\n✅ FOUND 'Gemini' or '3.1' in result!")
        else:
            print("\n❌ 'Gemini' or '3.1' NOT found in result.")
            
    except Exception as e:
        print(f"\n❌ Tools.web_search Failed: {e}")

if __name__ == "__main__":
    test_search()