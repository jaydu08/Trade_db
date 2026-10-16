"""Offline checks for full-window period snapshots and immutable exports."""

import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import generate_agent_period_snapshot as period


class PeriodTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'trade'
        (self.root / 'data').mkdir(parents=True)
        self.crypto = Path(self.temp.name) / 'crypto.db'
        self.output = Path(self.temp.name) / 'agent/v1'
        self.now = datetime(2026, 10, 14, 18, 35, tzinfo=timezone(timedelta(hours=8)))
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            db.execute('CREATE TABLE trenddailybar (date TEXT, market TEXT, symbol TEXT, name TEXT, close REAL, amount REAL, turnover_rate REAL, source TEXT, updated_at TEXT)')
            db.execute('CREATE TABLE trendseedpool (date TEXT, market TEXT, symbol TEXT)')
            for day, prices in [('2026-09-14', (100, 100)), ('2026-10-07', (100, 100)),
                                ('2026-10-14', (110, 90))]:
                for symbol, price in zip(('UP', 'DOWN'), prices):
                    db.execute('INSERT INTO trenddailybar VALUES (?,?,?,?,?,?,?,?,?)',
                               (day, 'CN', symbol, symbol, price, 1000, 1, 'heatmap', '2026-10-14 09:00:00'))
                    db.execute('INSERT INTO trendseedpool VALUES (?,?,?)', (day, 'CN', symbol))
        with sqlite3.connect(self.crypto) as db:
            db.execute('CREATE TABLE health (component TEXT, updated REAL, payload TEXT)')
            db.execute('CREATE TABLE assets (id TEXT, market TEXT, payload TEXT)')
            db.execute('CREATE TABLE security (id TEXT, payload TEXT)')
            db.execute('CREATE TABLE bars (id TEXT, ts REAL, price REAL, source TEXT)')
            stamp = self.now.timestamp() - 60
            db.execute('INSERT INTO health VALUES (?,?,?)', ('spot', stamp, json.dumps({'selected_symbols': ['UPUSDT', 'DOWNUSDT']})))
            for symbol, price in [('UPUSDT', 110), ('DOWNUSDT', 90)]:
                key = 'spot:' + symbol
                db.execute('INSERT INTO assets VALUES (?,?,?)', (key, 'spot', json.dumps({
                    'id': key, 'market': 'spot', 'symbol': symbol, 'price': price,
                    'updated': stamp, 'volume_24h': 1000000})))
                for days in (7, 30):
                    db.execute('INSERT INTO bars VALUES (?,?,?,?)', (key, stamp - days * 86400, 100, 'binance'))
        (self.root / 'data/macro_dashboard_snapshot.json').write_text(json.dumps({
            'global_assets': [{'id': 'SOX', 'name': '费城半导体', 'kind': '指数', 'value': 120,
                               'asof': '2026-10-13', 'source': 'cache',
                               'changes': {'7d': 5, '30d': -2}}],
            'macro': [{'id': 'DGS10', 'name': '美债10Y', 'value': 4.5,
                       'asof': '2026-10-13', 'source': 'FRED'}]}))

    def test_full_period_up_down_and_null_quality(self):
        for days in (7, 30):
            snapshot, size = period.generate(self.root, self.crypto, self.output, days, self.now, scores={})
            self.assertLess(size, period.MAX_BYTES)
            stocks = snapshot['markets']['equities_and_commodities']
            self.assertEqual({r['direction'] for r in stocks}, {'up', 'down'})
            self.assertEqual({r['window_start_target'] for r in stocks},
                             {'2026-10-07' if days == 7 else '2026-09-14'})
            self.assertTrue(all(r['trend_score'] is None for r in stocks))
            self.assertEqual({r['direction'] for r in snapshot['markets']['crypto']}, {'up', 'down'})
            self.assertIsNone(snapshot['markets']['macro_rates'][0]['period_return_pct'])
            original = (self.output / ('weekly' if days == 7 else 'monthly') / '2026-10-14.json').read_bytes()
            later, _ = period.generate(self.root, self.crypto, self.output, days,
                                       self.now + timedelta(minutes=3), scores={})
            self.assertEqual(later, snapshot)
            self.assertEqual(original, (self.output / ('weekly' if days == 7 else 'monthly') / '2026-10-14.json').read_bytes())

    def test_sparse_baseline_rejected(self):
        points = [(datetime(2026, 9, 1).date(), {'close': 100}),
                  (datetime(2026, 10, 14).date(), {'close': 110})]
        self.assertIsNone(period._return(points, 30)[0])

    def test_month_end_morning_excludes_incomplete_stock_session(self):
        morning = datetime(2026, 11, 30, 9, 35, tzinfo=timezone(timedelta(hours=8)))
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            for day, price in [('2026-10-28', 100), ('2026-11-27', 110),
                               ('2026-11-30', 250)]:
                db.execute('INSERT INTO trenddailybar VALUES (?,?,?,?,?,?,?,?,?)',
                           (day, 'CN', 'UP', 'UP', price, 1000, 1, 'heatmap',
                            '2026-11-30 01:30:00'))
        rows, _ = period.stock_period(self.root, 30, morning, {('CN', 'UP'): 99})
        item = next(row for row in rows if row['symbol'] == 'UP')
        self.assertEqual(item['window_end'], '2026-11-27')
        self.assertEqual(item['price'], 110)
        self.assertIsNone(item['trend_score'])
        self.assertIn('incomplete bars excluded', item['trend_score_missing_reason'])

    def test_month_end_morning_is_explicitly_partial(self):
        morning = datetime(2026, 11, 30, 9, 35, tzinfo=timezone(timedelta(hours=8)))
        with patch.object(period, 'stock_period', return_value=(
                [{'market': 'CN', 'session_date': '2026-11-27'}], [])), \
             patch.object(period, 'crypto_period', return_value=([{'market': 'spot'}], [])), \
             patch.object(period, 'macro_period', return_value=([], [])):
            snapshot = period.build(self.root, self.crypto, 30, morning, scores={})
        self.assertFalse(snapshot['publication_context']['full_calendar_month_complete'])
        self.assertIn('not a full finished calendar month',
                      snapshot['publication_context']['month_end_morning_note'])
        self.assertEqual(snapshot['run_kind'], 'scheduled')


if __name__ == '__main__':
    unittest.main()
