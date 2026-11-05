import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modules.ingestion.market_context import breadth_from_rows, full_market_rows, fetch_indices
from modules.ingestion.tencent_quote import fetch_unadjusted_daily


class MarketContextTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 5, 19, tzinfo=ZoneInfo('Asia/Shanghai'))
        self.stamp = int(self.now.replace(hour=16).timestamp())

    def row(self, symbol, pct=1, stamp=None):
        return dict(f12=symbol, f13=1, f2=10, f3=pct, f124=self.stamp if stamp is None else stamp)

    def test_dated_breadth_counts_and_scope_not_candidates(self):
        rows = [self.row('a', 1), self.row('b', -1), self.row('c', 0)]
        result = breadth_from_rows('CN', rows, 3, '2026-11-05', self.now)
        self.assertEqual((result['advancing'], result['declining'], result['unchanged']), (1, 1, 1))
        self.assertEqual(result['unit'], 'securities')
        self.assertIn('not Trend candidates', result['coverage_scope'])

    def test_wrong_date_missing_time_invalid_price_are_not_fabricated(self):
        rows = [self.row('a'), self.row('b', stamp=0), self.row('c', stamp=self.stamp - 86400)]
        result = breadth_from_rows('CN', rows, 3, '2026-11-05', self.now)
        self.assertIsNone(result['advancing'])
        self.assertEqual(result['observed_counts']['advancing'], 1)
        self.assertEqual(result['quality'], 'missing')

    def test_complete_pagination_and_repeated_page_rejected(self):
        def get(url, params, timeout):
            response = Mock()
            first = (params['pn'] - 1) * 100
            response.json.return_value = {'data': {'total': 3000, 'diff': [self.row(str(i)) for i in range(first, first + 100)]}}
            return response
        rows, total = full_market_rows('CN', get=get)
        self.assertEqual((len(rows), total), (3000, 3000))
        response = Mock()
        response.json.return_value = {'data': {'total': 3000, 'diff': [self.row(str(i)) for i in range(100)]}}
        with self.assertRaisesRegex(ValueError, 'repeated'):
            full_market_rows('CN', get=lambda *a, **kw: response)

    @patch('modules.ingestion.tencent_quote.requests.get')
    def test_daily_uses_close_not_open_and_never_adjusted_substitute(self, get):
        get.return_value.json.return_value = {'data': {'sh588000': {'day': [['2026-11-05', '1.3', '1.358', '1.405', '1.299', '100']]}}}
        self.assertEqual(fetch_unadjusted_daily('588000', 'CN')[0][1], 1.358)
        get.return_value.json.return_value = {'data': {'sh588000': {'qfqday': [['2026-11-05', '1', '1', '1', '1', '100']]}}}
        with self.assertRaises(ValueError):
            fetch_unadjusted_daily('588000', 'CN')

    @patch('modules.ingestion.tencent_quote.fetch_quotes')
    def test_stale_index_keeps_null(self, fetch):
        fetch.return_value = {'sh000001': {'price': 3000, 'timestamp': '2026-11-04 15:00:00'}}
        result = fetch_indices('CN', '2026-11-05', self.now)
        self.assertIsNone(result[0]['close'])
        self.assertTrue(result[0]['stale'])

    def test_partial_but_explicit_hk_observed_universe_is_not_all_market_estimate(self):
        from modules.ingestion.market_context import qualify_breadth
        result = qualify_breadth('HK', dict(enumerated_count=4700, covered_count=3100,
                                          observed_counts=dict(advancing=1200, declining=1500, unchanged=400)))
        self.assertEqual(result['advancing'], 1200)
        self.assertEqual(result['quality'], 'partial')
        self.assertFalse(result['is_all_market_total'])

    def test_local_close_refresh_is_not_suppressed_by_six_hour_ttl(self):
        from modules.monitor.macro_dashboard import global_post_close_due
        now = self.now.replace(hour=16, minute=30)
        before = self.now.replace(hour=14).timestamp()
        self.assertTrue(global_post_close_due({'global_assets_checked_at': before}, now.timestamp()))
        self.assertFalse(global_post_close_due({'global_assets_checked_at': now.timestamp()}, now.timestamp()))


if __name__ == '__main__':
    unittest.main()
