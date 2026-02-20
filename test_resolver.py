import logging
from modules.monitor.resolver import SymbolResolver

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_resolver():
    test_cases = [
        "00700",       # HK standard
        "600519",      # CN standard
        "goog",        # US lowercase
        "GOOG",        # US uppercase
        "CRCL",        # US rare
        "腾讯",        # CN/HK name
        "谷歌",        # US name
        "腾讯控股",    # Full name
        "贵州茅台"     # CN name
    ]
    
    print("\n--- Testing SymbolResolver ---")
    for case in test_cases:
        print(f"\nInput: '{case}'")
        try:
            result = SymbolResolver.resolve(case)
            if result:
                print(f"✅ Result: {result}")
            else:
                print("❌ Result: None")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_resolver()