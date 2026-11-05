"""2026-10-25 reported snapshot defects: classification, dates, phase, stale rollup."""
import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from agent_market_calendar import market_phase
from generate_agent_snapshot import (classify_stock_asset, verified_hk_product,
    macro_snapshot, stock_anomaly_snapshot, stock_snapshot)


class SnapshotRegressionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'data').mkdir()
        self.now = datetime(2026, 10, 23, 19, 15, tzinfo=ZoneInfo('Asia/Shanghai'))
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            db.executescript('CREATE TABLE trendseedpool(date TEXT,market TEXT,symbol TEXT,name TEXT);'
                'CREATE TABLE trenddailybar(date TEXT,market TEXT,symbol TEXT,name TEXT,close REAL,amount REAL,'
                'turnover_rate REAL,source TEXT,updated_at TEXT,catalyst_tags TEXT);'
                'CREATE TABLE dailyrank(id INTEGER PRIMARY KEY,date TEXT,market TEXT,symbol TEXT,change_pct REAL);')

    def test_inverse_identity_survives_abbreviations_and_bad_catalog(self):
        for symbol in ('07552', '07500', '7552', 'HK.07500'):
            kind, source = classify_stock_asset(symbol, 'FI二南方', 'STOCK')
            self.assertEqual(kind, 'LEVERAGED_ETF')
            self.assertIn('HKEX', source)
            product = verified_hk_product(symbol)
            self.assertEqual(product['product_type'], 'INVERSE_ETF')
            self.assertEqual(product['leverage_factor'], -2)
            self.assertEqual(product['leverage_reset_period'], 'daily')

    def test_inverse_funds_cannot_enter_ordinary_stock_scan(self):
        directory = self.root / 'data/agent_market_scan'
        directory.mkdir()
        (directory / '2026-10-23.json').write_text(json.dumps({'markets': {'HK': {
            'session_date': '2026-10-23', 'source_universe_count': 3,
            'items': [dict(symbol=s, name='FI二南方', price=100, change_pct=change)
                      for s, change in [('07552', 20), ('07500', 15), ('00700', 2)]]}}}))
        up, down, strong, weak, health = stock_anomaly_snapshot(self.root, self.now)
        self.assertEqual({i['symbol'] for i in up + down + strong + weak}, {'00700'})
        self.assertEqual(next(h for h in health if h['market'] == 'HK')[
            'excluded_reason_counts']['non_stock_instrument'], 2)

    def test_us_archive_date_is_not_exchange_session_even_after_delayed_write(self):
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            for archive, price in [('2026-10-23', 100), ('2026-10-24', 110)]:
                db.execute('INSERT INTO trenddailybar VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (archive, 'US', '105.TEST', 'Test', price, 10000, 1, 'heatmap',
                     '2026-10-24 14:00:00', ''))
        sunday = datetime(2026, 10, 25, 10, tzinfo=self.now.tzinfo)
        markets, *_ = stock_snapshot(self.root, sunday)
        self.assertEqual(next(m for m in markets if m['market'] == 'US')['session_date'], '2026-10-23')
        up, _, _, _, health = stock_anomaly_snapshot(self.root, sunday)
        item = next(i for i in up if i['market'] == 'US')
        self.assertEqual(item['session_date'], '2026-10-23')
        self.assertEqual(item['archive_date'], '2026-10-24')
        self.assertEqual(next(h for h in health if h['market'] == 'US')['session_date'], '2026-10-23')

    def test_cash_session_phases_and_timezone(self):
        cases = [('CN', 9, 0, 'pre_open'), ('CN', 9, 20, 'opening_auction'),
            ('CN', 10, 0, 'open'), ('CN', 11, 30, 'lunch_break'),
            ('CN', 14, 58, 'closing_auction'), ('CN', 15, 0, 'post_close'),
            ('HK', 12, 30, 'lunch_break'), ('HK', 13, 0, 'open'),
            ('HK', 16, 5, 'closing_auction'), ('HK', 16, 10, 'post_close')]
        for market, hour, minute, expected in cases:
            t = self.now.replace(hour=hour, minute=minute)
            self.assertEqual(market_phase(market, t), expected)
            self.assertEqual(market_phase(market, t.astimezone(ZoneInfo('UTC'))), expected)
        self.assertEqual(market_phase('HK', self.now.replace(day=25)), 'closed')
        self.assertEqual(market_phase('HK', self.now.replace(month=12, day=24, hour=13)), 'post_close')
        self.assertEqual(market_phase('HK', self.now.replace(day=19)), 'closed')

    def test_us_plain_ticker_without_catalog_inherits_verified_alias_type(self):
        with sqlite3.connect(self.root / 'data/meta.db') as db:
            db.execute('CREATE TABLE asset(symbol TEXT,asset_type TEXT)')
            db.execute('INSERT INTO asset VALUES(?,?)', ('105.SNDK', 'STOCK'))
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            for symbol in ('105.SNDK', 'SNDK'):
                for archive, close in [('2026-10-22', 100), ('2026-10-23', 90)]:
                    db.execute('INSERT INTO trenddailybar VALUES(?,?,?,?,?,?,?,?,?,?)',
                               (archive, 'US', symbol, 'SanDisk', close, 1000, 1, 'heatmap',
                                archive + ' 00:30:00', ''))
        _, down, *_ = stock_anomaly_snapshot(self.root, self.now)
        us = [r for r in down if r['market'] == 'US']
        self.assertEqual(len(us), 1)
        self.assertEqual(us[0]['instrument_type'], 'STOCK')
        self.assertEqual(us[0]['raw_symbols'], ['105.SNDK', 'SNDK'])

    def test_macro_observation_stale_rolls_up_without_stale_collector(self):
        now = self.now
        payload = dict(refreshed_at=now.timestamp(), macro_checked_at=now.timestamp(),
            macro=[dict(id='DGS10', asof='2026-10-01', value=4, source='FRED')],
            crypto_activity={key: dict(value=1, checked_at=now.timestamp())
                             for key in ('spot', 'perp', 'dex', 'sentiment')})
        (self.root / 'data/macro_dashboard_snapshot.json').write_text(json.dumps(payload))
        rows, _, health = macro_snapshot(self.root, now)
        self.assertTrue(rows[0]['stale'])
        self.assertTrue(health['stale'])
        self.assertTrue(health['observation_stale'])
        self.assertFalse(health['collection_stale'])
        self.assertEqual(health['stale_indicator_count'], 1)


if __name__ == '__main__':
    unittest.main()
