from modules.ingestion.akshare_client import akshare_client
import logging

logging.basicConfig(level=logging.INFO)

def test_crcl():
    print("--- Testing CRCL Quote ---")
    quote = akshare_client.get_realtime_quote_eastmoney("CRCL", "US")
    print(f"Quote Result: {quote}")
    
    if quote:
        price = quote.get('price')
        pct = quote.get('pct_chg')
        print(f"Price: {price}, Pct: {pct}")
        
        if abs(pct) >= 5.0:
            print("✅ Should Trigger Alert!")
        else:
            print(f"❌ No Alert (Threshold 5.0 > {abs(pct)})")

if __name__ == "__main__":
    test_crcl()