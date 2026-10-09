import sqlite3
import unittest
from modules.monitor.crypto_trend import period_return, effective_security


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
