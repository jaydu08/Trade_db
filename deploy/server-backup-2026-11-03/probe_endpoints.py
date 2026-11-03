"""Read-only public endpoint preflight. Never trades, logs in or sends TG messages."""
import concurrent.futures
import json
import time
import urllib.request
import urllib.error

CHECKS = {
    'binance_spot': 'https://api.binance.com/api/v3/time',
    'binance_futures': 'https://fapi.binance.com/fapi/v1/time',
    'binance_futures_data': 'https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT',
    'binance_product_directory': 'https://www.binance.com/bapi/asset/v2/public/asset-service/product/get-products',
    'telegram_connectivity_only': 'https://api.telegram.org/',
    'sina_stock_quote': 'https://hq.sinajs.cn/list=sh000001,hk00700,gb_aapl',
    'eastmoney_quote': 'https://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&fields=f43,f58',
    'dexscreener': 'https://api.dexscreener.com/latest/dex/tokens/0x4902C5ebc598265Ed2212b559B042De8a5Eeec3f',
    'github': 'https://api.github.com/repos/jaydu08/Trade_db',
}


def check(pair):
    name, url = pair
    start = time.monotonic()
    result = {'service': name}
    request = urllib.request.Request(url, headers={'User-Agent': 'migration-preflight/1.0',
                                                  'Referer': 'https://finance.sina.com.cn/'})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            data = response.read(8_000_000)
            result.update(http_status=response.status, bytes=len(data), transport_ok=True)
            if name.startswith('binance') or name in ('dexscreener', 'eastmoney_quote', 'github'):
                body = json.loads(data)
                result['data_ok'] = ('serverTime' in body if name in ('binance_spot', 'binance_futures')
                    else 'markPrice' in body if name == 'binance_futures_data'
                    else bool(body.get('success') and body.get('data')) if name == 'binance_product_directory'
                    else isinstance(body, dict) and not body.get('message'))
            else:
                result['data_ok'] = None  # TG root does not prove bot credentials or send rights.
    except urllib.error.HTTPError as error:
        result.update(http_status=error.code, transport_ok=False,
            reason='restricted_location' if error.code == 451 else 'rate_limit' if error.code == 429 else 'HTTP error; inspect provider/network')
    except Exception as error:
        result.update(transport_ok=False, reason=type(error).__name__)
    result['elapsed_ms'] = round((time.monotonic() - start) * 1000)
    return result


if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(check, CHECKS.items()))
    print(json.dumps({'checks': results, 'limitations': [
        'HTTP access does not establish legal/account eligibility.',
        'This checks public REST endpoints, not authenticated APIs, TG delivery or WebSocket stability.',
        'No IP changes, proxy settings or restriction bypasses are performed.']}, indent=2))
