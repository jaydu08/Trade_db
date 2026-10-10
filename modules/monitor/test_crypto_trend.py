import sqlite3
import unittest
from datetime import datetime, timezone
from modules.monitor.crypto_trend import period_return, effective_security, review_priority, trading_pool_age_hours


class TrendTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.execute('CREATE TABLE bars(id TEXT,ts INTEGER,price REAL,source TEXT)')

    def tearDown(self):
        self.db.close()

    def test_period_return_and_new_listing(self):
        item = {'id': 'spot:X', 'market': 'spot', 'updated': 200000, 'price': 12}
        self.db.execute('INSERT INTO bars VALUES(?,?,?,?)', ('spot:X', 200000-43200, 10, 'binance'))
        self.assertAlmostEqual(period_return(self.db, item, 43200)[0], 20)
        self.assertIsNone(period_return(self.db, item, 30*86400)[0])

    def test_pool_change_cannot_mix_prices(self):
        item = {'id': 'bsc:X', 'market': 'onchain', 'pair': 'new', 'updated': 200000, 'price': 12}
        self.db.execute('INSERT INTO bars VALUES(?,?,?,?)', ('bsc:X', 200000-86400, 10, 'dex:old'))
        self.assertIsNone(period_return(self.db, item, 86400)[0])

    def test_stale_pass_or_changed_pool_becomes_unknown(self):
        payload = {'status': 'pass', 'expires_at': 100, 'pair': 'a'}
        self.assertEqual(effective_security(payload, 'a', 101)['status'], 'unknown')
        self.assertEqual(effective_security(payload, 'b', 99)['status'], 'unknown')

    def test_review_priority_has_quality_floors_and_capped_spikes(self):
        item = {'chain': 'bsc', 'stale': False, 'period_change': 100,
                'volume_24h': 2_000_000, 'liquidity': 500_000, 'market_cap': 3_000_000}
        self.assertEqual(review_priority(item)['score'], 100)
        self.assertEqual(review_priority(dict(item, period_change=10000))['score'], 100)
        self.assertIsNone(review_priority(dict(item, liquidity=100)))
        self.assertIsNone(review_priority(dict(item, chain='solana')))
        self.assertIsNone(review_priority(dict(item, stale=True)))

    def test_trading_pool_age_uses_pair_creation_timestamp(self):
        now = datetime(2026, 10, 10, tzinfo=timezone.utc).timestamp()
        self.assertEqual(trading_pool_age_hours('2026-10-09T00:00:00+00:00', now), 24)
        self.assertIsNone(trading_pool_age_hours('invalid', now))
