import sqlite3
import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from modules.monitor import crypto_trend
from modules.monitor.crypto_trend import period_return, effective_security, trading_pool_age_hours


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

    def test_trading_pool_age_uses_pair_creation_timestamp(self):
        now = datetime(2026, 10, 10, tzinfo=timezone.utc).timestamp()
        self.assertEqual(trading_pool_age_hours('2026-10-09T00:00:00+00:00', now), 24)
        self.assertIsNone(trading_pool_age_hours('invalid', now))

    def test_chain_options_do_not_disappear_when_risk_filter_is_pass(self):
        now = time.time()
        with tempfile.TemporaryDirectory() as folder:
            path = folder + '/trend.sqlite'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE assets(id TEXT,market TEXT,payload TEXT)')
            db.execute('CREATE TABLE security(id TEXT,payload TEXT)')
            db.execute('CREATE TABLE health(component TEXT,payload TEXT,updated REAL)')
            db.execute('CREATE TABLE bars(id TEXT,ts INTEGER,price REAL,source TEXT)')
            for chain, status in [('ethereum', 'pass'), ('solana', 'unknown'), ('robinhood', 'unknown')]:
                asset_id = chain + ':token'
                payload = {'id': asset_id, 'chain': chain, 'market': 'onchain', 'pair': 'pool',
                           'symbol': chain, 'name': chain, 'updated': now, 'price': 2,
                           'provider_change_24h': 5}
                db.execute('INSERT INTO assets VALUES(?,?,?)', (asset_id, 'onchain', json.dumps(payload)))
                security = {'status': status, 'pair': 'pool', 'expires_at': now + 3600}
                db.execute('INSERT INTO security VALUES(?,?)', (asset_id, json.dumps(security)))
            db.commit()
            db.close()
            with patch.object(crypto_trend, 'DB_PATH', path), patch.object(crypto_trend, 'ALPHA_ROOT', folder):
                result = crypto_trend.get_crypto_trend(market='onchain', period='24h', risk='pass')
            self.assertEqual(result['available_chains'], ['ethereum', 'robinhood', 'solana'])
            self.assertEqual([item['chain'] for item in result['items']], ['ethereum'])
