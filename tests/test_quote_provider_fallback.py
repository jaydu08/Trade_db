import asyncio
import datetime as dt
import unittest
from unittest.mock import AsyncMock, Mock, patch

import requests

from modules.ingestion.tencent_quote import quote_code, parse_quotes, fetch_quotes
from modules.ingestion.us_premarket_fallback import parse_chart, parse_nasdaq


def sample(code, stamp='20261105160000', volume='100', amount='200', price='10', previous='8'):
    fields = [''] * 46
    for index, value in {1: '测试股票', 2: '000001', 3: price, 4: previous, 5: '9',
                         6: volume, 30: stamp, 31: '2', 32: '25', 33: '11',
                         34: '8', 37: amount, 38: '1.5', 45: '12'}.items():
        fields[index] = value
    return f'v_{code}="' + '~'.join(fields) + '";'


class TencentQuoteTests(unittest.TestCase):
    def setUp(self):
        from modules.ingestion import tencent_quote
        self.cooldown = patch.object(tencent_quote, '_SINA_RETRY_AFTER', 0)
        self.cooldown.start()
        self.addCleanup(self.cooldown.stop)

    def test_permission_denial_has_bounded_cooldown(self):
        from modules.ingestion.tencent_quote import sina_available, mark_sina_denied
        with patch('modules.ingestion.tencent_quote.time.monotonic', return_value=100):
            self.assertTrue(sina_available())
            mark_sina_denied()
            self.assertFalse(sina_available())
        with patch('modules.ingestion.tencent_quote.time.monotonic', return_value=401):
            self.assertTrue(sina_available())

    def test_symbol_mapping(self):
        self.assertEqual(quote_code('105.MSTR', 'US'), 'usMSTR')
        self.assertEqual(quote_code('BRK.B', 'US'), 'usBRK.B')
        self.assertEqual(quote_code('700', 'HK'), 'hk00700')
        self.assertEqual(quote_code('HSI', 'HK'), 'hkHSI')
        self.assertEqual(quote_code('sh000300', 'CN'), 'sh000300')
        self.assertEqual(quote_code('600519', 'CN'), 'sh600519')
        self.assertEqual(quote_code('510300', 'CN'), 'sh510300')
        self.assertEqual(quote_code('920001', 'CN'), 'bj920001')

    def test_cn_units(self):
        q = parse_quotes(sample('sh600519'), {'sh600519': '600519'}, 'CN')['600519']
        self.assertEqual(q['volume'], 10000)
        self.assertEqual(q['amount'], 2000000)
        self.assertEqual(q['timestamp'], '2026-11-05 16:00:00')
        self.assertEqual(q['currency'], 'CNY')
        self.assertEqual(q['provider'], 'Tencent')

    def test_hk_units(self):
        q = parse_quotes(sample('hk00700', '2026/11/05 16:08:22'), {'hk00700': '00700'}, 'HK')['00700']
        self.assertEqual(q['volume'], 100)
        self.assertEqual(q['amount'], 200)
        self.assertEqual(q['timestamp'], '2026-11-05 16:08:22')
        self.assertEqual(q['currency'], 'HKD')

    def test_us_units_and_completed_session_timestamp(self):
        q = parse_quotes(sample('usAAPL', '2026-11-04 16:00:01'), {'usAAPL': 'AAPL'}, 'US')['AAPL']
        self.assertEqual(q['volume'], 100)
        self.assertEqual(q['amount'], 200)
        self.assertEqual(q['timestamp'], '2026-11-04 16:00:01')
        self.assertEqual(q['timestamp_timezone'], 'America/New_York')
        self.assertEqual(q['market_cap'], 1200000000)
        self.assertEqual(q['session_scope'], 'regular')

    def test_missing_amount_is_not_invented(self):
        q = parse_quotes(sample('usAAPL', amount=''), {'usAAPL': 'AAPL'}, 'US')['AAPL']
        self.assertIsNone(q['amount'])

    def test_invalid_data_is_rejected(self):
        for raw in [sample('usAAPL', price='nan'), sample('usAAPL', price='0'),
                    sample('usAAPL', previous='0'), sample('usAAPL', stamp='bad'),
                    'v_usAAPL="";', 'Forbidden']:
            self.assertEqual(parse_quotes(raw, {'usAAPL': 'AAPL'}, 'US'), {})

    def test_unrequested_symbol_is_not_included(self):
        self.assertEqual(parse_quotes(sample('usTSLA'), {'usAAPL': 'AAPL'}, 'US'), {})

    @patch('modules.ingestion.tencent_quote.requests.get')
    def test_provider_error_is_empty_not_fake_quote(self, get):
        get.side_effect = requests.HTTPError('403')
        self.assertEqual(fetch_quotes(['AAPL'], 'US'), {})

    @patch('modules.ingestion.tencent_quote.fetch_quotes')
    def test_batch_fallback_on_primary_403(self, fetch):
        from modules.ingestion.akshare_client import AkShareClient
        quote = parse_quotes(sample('hk00700'), {'hk00700': '00700'}, 'HK')['00700']
        fetch.return_value = {'00700': quote}
        with patch('modules.ingestion.akshare_client.requests.get') as get:
            get.return_value.status_code = 403
            get.return_value.raise_for_status.side_effect = requests.HTTPError('403')
            result = AkShareClient.get_realtime_quotes_batch(['00700'], 'HK')
        self.assertEqual(result['00700']['provider'], 'Tencent')
        fetch.assert_called_once_with(['00700'], 'HK')

    @patch('modules.ingestion.tencent_quote.fetch_quotes')
    def test_async_probe_uses_same_market_symbol_keys(self, fetch):
        from modules.probing.async_prober import AsyncMarketProber
        quote = parse_quotes(sample('hk00700'), {'hk00700': '00700'}, 'HK')['00700']
        fetch.return_value = {'00700': quote}
        prober = AsyncMarketProber()
        with patch.object(prober, '_fetch_quote_with_retry', new=AsyncMock(return_value=None)):
            results = asyncio.run(prober.get_quotes_async([{'symbol': '00700', 'market': 'HK'}]))
        self.assertIn('HK:00700', results)

    @patch('modules.ingestion.tencent_quote.fetch_quotes')
    def test_benchmark_uses_same_date_tencent_index(self, fetch):
        from modules.analysis.market_relative_strength import fetch_index_return
        fetch.return_value = {'HSI': {'timestamp': '2026-11-05 16:08:22', 'pct_chg': 1.2}}
        with patch('modules.analysis.market_relative_strength._fetch_sina_index_return', side_effect=ValueError()):
            row = fetch_index_return('HK', dt.date(2026, 11, 5))
            self.assertEqual(row['return_pct'], 1.2)
            self.assertEqual(row['source'], 'Tencent index quote')
            with self.assertRaises(ValueError):
                fetch_index_return('HK', dt.date(2026, 11, 6))


class USPremarketFallbackTests(unittest.TestCase):
    def nasdaq(self):
        self.now = dt.datetime(2026, 11, 5, 10, tzinfo=dt.timezone.utc)
        return {'data': {'marketStatus': 'Pre-Market', 'companyName': 'Test',
                        'primaryData': {'lastSalePrice': '$103.00', 'volume': '1,500',
                                        'lastTradeTimestamp': 'Nov 5, 2026 4:59 AM ET'},
                        'secondaryData': {'lastSalePrice': '$100.00',
                                          'lastTradeTimestamp': 'Closed at Nov 4, 2026 4:00 PM ET'}}}

    def test_nasdaq_live_premarket_fields(self):
        data = self.nasdaq()
        row = parse_nasdaq(data, 'AAPL', self.now)
        self.assertEqual(row['volume'], 1500)
        self.assertEqual(row['price'], 103)
        self.assertEqual(row['prev_close'], 100)
        self.assertEqual(row['pct'], 3)
        self.assertEqual(row['provider'], 'Nasdaq premarket')

    def test_nasdaq_regular_session_is_not_premarket(self):
        data = self.nasdaq()
        data['data']['marketStatus'] = 'Market Open'
        self.assertIsNone(parse_nasdaq(data, 'AAPL', self.now))

    def test_nasdaq_previous_premarket_quote_is_rejected(self):
        data = self.nasdaq()
        data['data']['primaryData']['lastTradeTimestamp'] = 'Nov 4, 2026 4:59 AM ET'
        self.assertIsNone(parse_nasdaq(data, 'AAPL', self.now))

    def test_nasdaq_missing_volume_is_rejected(self):
        data = self.nasdaq()
        data['data']['primaryData']['volume'] = 'N/A'
        self.assertIsNone(parse_nasdaq(data, 'AAPL', self.now))

    def payload(self):
        start = int(dt.datetime(2026, 11, 5, 9, tzinfo=dt.timezone.utc).timestamp())
        self.now = dt.datetime(2026, 11, 5, 10, tzinfo=dt.timezone.utc)
        self.start = start
        return {'chart': {'result': [{
            'meta': {'currentTradingPeriod': {'pre': {'start': start, 'end': start + 19800}},
                     'regularMarketTime': start - 43200, 'regularMarketPrice': 100,
                     'previousClose': 90, 'shortName': 'Test'},
            'timestamp': [start - 60, start + 60, start + 120],
            'indicators': {'quote': [{'close': [200, 102, 103], 'volume': [100000, 100, 50]}]},
        }]}}

    def test_only_current_premarket_volume_and_last_completed_close(self):
        payload = self.payload()
        row = parse_chart(payload, 'AAPL', self.now)
        self.assertEqual(row['volume'], 150)
        self.assertEqual(row['price'], 103)
        self.assertEqual(row['prev_close'], 100)
        self.assertEqual(row['pct'], 3)
        self.assertEqual(row['amount'], 15450)
        self.assertEqual(row['session_scope'], 'premarket')

    def test_regular_hours_are_not_premarket(self):
        payload = self.payload()
        self.assertIsNone(parse_chart(payload, 'AAPL', self.now + dt.timedelta(hours=6)))

    def test_no_current_day_bars_no_quote(self):
        payload = self.payload()
        payload['chart']['result'][0]['timestamp'] = [self.start - 60] * 3
        self.assertIsNone(parse_chart(payload, 'AAPL', self.now))

    def test_no_volume_no_quote(self):
        payload = self.payload()
        payload['chart']['result'][0]['indicators']['quote'][0]['volume'] = [0, 0, 0]
        self.assertIsNone(parse_chart(payload, 'AAPL', self.now))

    def test_reference_from_current_regular_session_is_rejected(self):
        payload = self.payload()
        payload['chart']['result'][0]['meta']['regularMarketTime'] = self.start + 60
        self.assertIsNone(parse_chart(payload, 'AAPL', self.now))


if __name__ == '__main__':
    unittest.main()
