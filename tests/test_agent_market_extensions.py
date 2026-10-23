import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from agent_market_extensions import build_extensions, write_extensions


class ExtensionsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'crypto.sqlite'
        self.now = datetime(2026, 10, 23, 11, tzinfo=ZoneInfo('Asia/Shanghai'))
        with sqlite3.connect(self.db) as db:
            db.executescript('CREATE TABLE assets(id TEXT,market TEXT,payload TEXT);'
                'CREATE TABLE health(component TEXT,updated REAL,payload TEXT);'
                'CREATE TABLE bars(id TEXT,source TEXT,ts REAL,price REAL);')
            for market in ('spot', 'futures'):
                db.execute('INSERT INTO health VALUES(?,?,?)', (market, self.now.timestamp(),
                    json.dumps({'selected_symbols': ['MSTRBUSDT', 'BTCUSDT', 'OLDUSDT']})))
            for symbol, group, kind in [('MSTRBUSDT', 'securities', 'TOKENIZED_SECURITY'),
                                         ('BTCUSDT', 'crypto', 'CRYPTO_SPOT'),
                                         ('OLDUSDT', 'securities', 'TOKENIZED_SECURITY')]:
                item = dict(symbol=symbol, asset_group=group, instrument_type=kind,
                    updated=self.now.timestamp(), price=110, secret='never export this')
                db.execute('INSERT INTO assets VALUES(?,?,?)', (symbol, 'spot', json.dumps(item)))
            for hours in (12, 168, 720):
                db.execute('INSERT INTO bars VALUES(?,?,?,?)', ('MSTRBUSDT', 'binance',
                    self.now.timestamp() - hours * 3600, 100))
        (self.root / 'data').mkdir()
        (self.root / 'data/macro_dashboard_snapshot.json').write_text(json.dumps({
            'global_assets_checked_at': self.now.timestamp(), 'global_assets': [dict(id='CN.512480',
                kind='ETF', asof='2026-10-22', listing_market='CN', currency='CNY',
                previous_session='2026-10-21', changes={'1d': 2, '7d': 5})]}))

    def test_full_classification_windows_and_allowlist(self):
        result = build_extensions(self.root, self.db, self.now, full=True)
        group = result['securities_and_macro_contracts'][0]
        self.assertEqual(group['universe_count'], 2)
        self.assertEqual(group['items'][0]['windows']['720h']['return_pct'], 10)
        self.assertIsNone(group['items'][1]['windows']['12h']['return_pct'])
        self.assertNotIn('secret', json.dumps(result))
        self.assertNotIn('BTCUSDT', json.dumps(result))
        row = result['global_indices_and_etfs'][0]
        self.assertEqual(row['changes']['1d'], 2)
        self.assertEqual(row['baseline_dates']['1d'], '2026-10-21')
        self.assertIsNone(row['baseline_dates']['7d'])
        self.assertEqual(row['currency'], 'CNY')

    def test_period_uses_period_history_not_daily(self):
        result = build_extensions(self.root, self.db, self.now, days=30)
        items = result['securities_and_macro_contracts'][0]['items']
        self.assertEqual(len(items), 1)
        self.assertEqual(list(items[0]['windows']), ['720h'])

    def test_stale_samples_are_marked(self):
        result = build_extensions(self.root, self.db, self.now.replace(hour=12), full=True)
        self.assertTrue(result['stale'])
        self.assertTrue(result['securities_and_macro_contracts'][0]['items'][0]['stale'])

    def test_missing_sources_remain_missing(self):
        result = build_extensions(self.root / 'missing', self.root / 'none.db', self.now)
        self.assertTrue(result['stale'])
        self.assertEqual(len(result['missing_reason']), 2)

    def test_atomic_standalone(self):
        write_extensions(self.root, self.db, self.root / 'output', self.now)
        result = json.loads((self.root / 'output/market_extensions.json').read_text())
        self.assertEqual(len(result['global_indices_and_etfs']), 1)


if __name__ == '__main__':
    unittest.main()
