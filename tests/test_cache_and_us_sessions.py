import sqlite3
import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from modules.market_sessions import us_last_completed, us_archive_session, verified_us_quote


class SessionTests(unittest.TestCase):
    def test_weekend_monday_and_dst(self):
        for stamp in ('2026-11-07T09:30:00+08:00','2026-11-08T09:30:00+08:00','2026-11-09T19:12:00+08:00'):
            self.assertEqual(str(us_last_completed(datetime.fromisoformat(stamp))), '2026-11-06')
        self.assertEqual(str(us_last_completed(datetime.fromisoformat('2026-11-06T19:12:00+08:00'))), '2026-11-05')
        self.assertEqual(str(us_last_completed(datetime.fromisoformat('2026-10-20T04:30:00+08:00'))), '2026-10-19')

    def test_holidays_early_close_and_bank_holiday(self):
        self.assertEqual(str(us_last_completed(datetime.fromisoformat('2026-11-27T19:00:00+08:00'))), '2026-11-25')
        self.assertEqual(str(us_last_completed(datetime.fromisoformat('2026-11-27T13:30:00-05:00'))), '2026-11-27')
        self.assertEqual(str(us_last_completed(datetime.fromisoformat('2026-11-11T17:00:00-05:00'))), '2026-11-11')

    def test_backfill_date_is_not_next_day_archive(self):
        self.assertEqual(str(us_archive_session('2026-11-06','trend_pool_history_backfill')), '2026-11-06')
        self.assertEqual(str(us_archive_session('2026-11-07','heatmap')), '2026-11-06')

    def test_real_close_required_not_premarket_or_wrong_session(self):
        now = datetime.fromisoformat('2026-11-09T19:12:00+08:00')
        quote = dict(price=342, timestamp='2026-11-06 16:00:01', timestamp_timezone='America/New_York', session_scope='regular')
        self.assertIsNotNone(verified_us_quote(quote, now))
        for stamp in ('2026-11-09 08:00:00','2026-11-05 16:00:01'):
            self.assertIsNone(verified_us_quote(dict(quote,timestamp=stamp),now))


class CacheFailureTests(unittest.TestCase):
    def test_market_fetch_failure_is_not_reported_as_success(self):
        from modules.analysis.heatmap import MarketHeatMap
        worker = object.__new__(MarketHeatMap)
        with patch('modules.ingestion.akshare_client.AkShareClient._fetch_bulk_sina',side_effect=RuntimeError('source failed')):
            with self.assertRaisesRegex(RuntimeError,'quote acquisition failed'):
                worker.process_and_notify('CN')

    def manager(self):
        from core.cache import CacheManager
        manager = object.__new__(CacheManager)
        manager._cache = Mock()
        manager._failure_until = 0
        return manager

    def test_corrupt_read_is_miss_not_source_failure(self):
        manager = self.manager()
        manager._cache.get.side_effect = sqlite3.DatabaseError('database disk image is malformed')
        self.assertIsNone(manager.get('quotes'))

    def test_corrupt_write_does_not_discard_valid_response(self):
        from core.cache import cached
        manager = self.manager()
        manager._cache.get.return_value = None
        manager._cache.set.side_effect = sqlite3.DatabaseError('database disk image is malformed')
        with patch('core.cache.cache_manager', manager):
            self.assertEqual(cached('test')(lambda: {'price': 100})(), {'price': 100})


if __name__ == '__main__':
    unittest.main()
