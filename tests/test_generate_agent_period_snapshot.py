"""Offline checks for full-window period snapshots and immutable exports."""

import json
import sqlite3
import sys
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import generate_agent_period_snapshot as period
from generate_agent_snapshot import classify_stock_asset, verified_hk_product


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

    def _bar(self, market, symbol, day, price, amount=0, source='trend_pool_refresh_eod'):
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            db.execute('INSERT INTO trenddailybar VALUES (?,?,?,?,?,?,?,?,?)',
                       (day, market, symbol, symbol, price, amount, 0, source,
                        day + ' 00:12:00'))

    def _weekly_now(self):
        return datetime(2026, 10, 18, 9, 30, tzinfo=timezone(timedelta(hours=8)))

    def test_weekly_rejects_lagged_asset_sessions(self):
        now = self._weekly_now()
        samples = {'CN': ['300981', '601579', '301080', '600707', '001389'],
                   'HK': ['01142'], 'US': ['105.PCRX', '106.BH', 'CBRS', '105.LITE', '106.OKLO']}
        for market, symbols in samples.items():
            end = '2026-10-17' if market == 'US' else '2026-10-16'
            start = '2026-10-10' if market == 'US' else '2026-10-09'
            self._bar(market, 'FRESH_' + market, start, 100)
            self._bar(market, 'FRESH_' + market, end, 110)
            for symbol in symbols:
                self._bar(market, symbol, start, 100)
                self._bar(market, symbol, '2026-10-15' if market != 'US' else '2026-10-16', 150)
        rows, health = period.stock_period(self.root, 7, now, {})
        self.assertTrue(all(r['symbol'].startswith('FRESH_') for r in rows))
        excluded = [r for h in health for r in h['excluded_candidates']]
        self.assertTrue(set(sum(samples.values(), [])).issubset({r['symbol'] for r in excluded}))
        self.assertTrue(all(r['eligibility_status'] == 'lagged' and r['session_gap'] >= 1
                            for r in excluded if r['symbol'] in set(sum(samples.values(), []))))

    def test_us_utc_archive_date_maps_to_ny_session(self):
        self.assertEqual(period.archive_session('US', '2026-10-17').isoformat(), '2026-10-16')
        self.assertEqual(period.archive_session('HK', '2026-10-16').isoformat(), '2026-10-16')

    def test_halted_stock_does_not_become_false_latest(self):
        now = self._weekly_now()
        self._bar('CN', 'LATEST', '2026-10-09', 100)
        self._bar('CN', 'LATEST', '2026-10-16', 110)
        self._bar('CN', 'HALTED', '2026-10-09', 100)
        self._bar('CN', 'HALTED', '2026-10-15', 120)
        self._bar('CN', 'MISSING', '2026-10-09', 100)
        self._bar('CN', 'MISSING', '2026-10-15', 120)
        with sqlite3.connect(self.root / 'data/meta.db') as db:
            db.execute('CREATE TABLE asset(symbol TEXT, asset_type TEXT, listing_status TEXT)')
            db.execute("INSERT INTO asset VALUES('HALTED','STOCK','SUSPENDED')")
        # The minimal fixture has no industry tables; only this test needs a catalog.
        catalog = defaultdict(lambda: {'asset_type': 'STOCK', 'listing_status': 'ACTIVE',
                                       'industries': [], 'concepts': []})
        catalog['HALTED'] = {'asset_type': 'STOCK', 'listing_status': 'SUSPENDED',
                             'industries': [], 'concepts': []}
        with patch.object(period, '_stock_catalog', return_value=catalog):
            rows, health = period.stock_period(self.root, 7, now, {})
        self.assertEqual([r['symbol'] for r in rows], ['LATEST'])
        statuses = {r['symbol']: r['eligibility_status'] for h in health for r in h['excluded_candidates']}
        self.assertEqual(statuses['HALTED'], 'suspended')
        self.assertEqual(statuses['MISSING'], 'lagged')

    def test_relative_strength_recomputed_after_eligibility_filter(self):
        now = self._weekly_now()
        for symbol, close, end in [('A', 110, '2026-10-16'), ('B', 120, '2026-10-16'),
                                   ('EXTREME', 1000, '2026-10-15')]:
            self._bar('CN', symbol, '2026-10-09', 100)
            self._bar('CN', symbol, end, close)
        rows, _ = period.stock_period(self.root, 7, now, {})
        by_symbol = {r['symbol']: r for r in rows}
        self.assertEqual(set(by_symbol), {'A', 'B'})
        self.assertEqual(by_symbol['A']['relative_strength_pct'], -10)

    def test_stock_returns_anchor_to_market_reference_session(self):
        now = self._weekly_now()
        for symbol, end in [('A', '2026-10-16'), ('B', '2026-10-15')]:
            self._bar('HK', symbol, '2026-10-09', 100)
            self._bar('HK', symbol, end, 110)
        rows, _ = period.stock_period(self.root, 7, now, {})
        self.assertEqual({r['window_end'] for r in rows if r['market'] == 'HK'}, {'2026-10-16'})
        self.assertEqual({r['market_reference_session'] for r in rows if r['market'] == 'HK'}, {'2026-10-16'})

    def test_hk_daily_leveraged_etf_classification(self):
        for symbol in ('07709', '07747'):
            self.assertEqual(classify_stock_asset(symbol, 'XL二南方', 'STOCK')[0], 'LEVERAGED_ETF')
            self.assertEqual(verified_hk_product(symbol)['leverage_factor'], 2)
            self.assertEqual(verified_hk_product(symbol)['leverage_reset_period'], 'daily')

    def test_hk_multiple_trading_counters(self):
        hk, usd = verified_hk_product('07747'), verified_hk_product('09747')
        self.assertEqual(hk['product_id'], usd['product_id'])
        self.assertEqual((hk['currency'], usd['currency']), ('HKD', 'USD'))
        self.assertEqual(usd['counter_status'], 'terminated_2026-09-18')

    def test_terminated_hk_counter_cannot_rank_as_active(self):
        now = self._weekly_now()
        self._bar('HK', '09747', '2026-10-09', 100)
        self._bar('HK', '09747', '2026-10-16', 110)
        rows, health = period.stock_period(self.root, 7, now, {})
        self.assertNotIn('09747', {r['symbol'] for r in rows})
        excluded = [r for h in health for r in h['excluded_candidates']]
        self.assertEqual(next(r for r in excluded if r['symbol'] == '09747')['eligibility_status'], 'suspended')

    def test_existing_us_leveraged_etfs_not_regressed(self):
        for ticker in ('SOXS', 'SOXL', 'TQQQ', 'SQQQ'):
            self.assertEqual(classify_stock_asset(ticker, ticker, 'STOCK')[0], 'LEVERAGED_ETF')

    def test_verified_metadata_overrides_stock_default(self):
        self.assertEqual(classify_stock_asset('07747', 'XL二南三星', 'STOCK')[1][:8], 'HKEX IFP')

    def test_missing_amount_is_not_serialized_as_zero(self):
        self.assertEqual(period.amount_measure(0, 'trend_pool_refresh_eod')[:2], (None, 'missing'))
        self.assertEqual(period.amount_measure(None, 'heatmap')[:2], (None, 'missing'))

    def test_verified_zero_turnover_is_preserved(self):
        self.assertEqual(period.amount_measure(0, 'vendor_confirmed', verified_zero=True)[:2],
                         (0, 'verified_zero'))

    def test_amount_change_requires_comparable_bars(self):
        now = self._weekly_now()
        for day, amount in [('2026-10-09', 100), ('2026-10-12', 100),
                            ('2026-10-15', 200), ('2026-10-16', 200)]:
            self._bar('CN', 'A', day, 100 if day != '2026-10-16' else 110, amount,
                      'heatmap' if day != '2026-10-12' else 'daily_hot')
        rows, _ = period.stock_period(self.root, 7, now, {})
        self.assertIsNone(rows[0]['amount_change_pct'])
        self.assertIsNotNone(rows[0]['amount_change_missing_reason'])

    def test_nonzero_amount_change_with_missing_latest_is_explained(self):
        now = self._weekly_now()
        for day, amount in [('2026-10-09', 100), ('2026-10-12', 100),
                            ('2026-10-15', 200), ('2026-10-16', 0)]:
            self._bar('CN', 'BSP', day, 100 if day != '2026-10-16' else 110, amount, 'heatmap')
        rows, _ = period.stock_period(self.root, 7, now, {})
        self.assertIsNone(rows[0]['amount_latest'])
        self.assertIsNone(rows[0]['amount_change_pct'])

    def test_zero_amount_in_commodity_futures(self):
        now = self._weekly_now()
        for i in range(9):
            self._bar('CF', f'CF{i}', '2026-10-09', 100)
            self._bar('CF', f'CF{i}', '2026-10-16', 110)
        rows, _ = period.stock_period(self.root, 7, now, {})
        self.assertTrue(all(r['amount_latest'] is None and r['amount_status'] == 'missing'
                            for r in rows if r['market'] == 'CF'))

    def test_commodity_archive_date_is_not_false_exchange_session(self):
        now = self._weekly_now()
        self._bar('CF', 'FU0', '2026-10-10', 100)
        self._bar('CF', 'FU0', '2026-10-17', 110)
        with patch.object(period, 'crypto_period', return_value=([{'market': 'spot', 'symbol': 'BTCUSDT',
                                                                   'stale': False, 'quality': 'ok'}], [])):
            snapshot = period.build(self.root, self.crypto, 7, now, scores={})
        self.assertIsNone(snapshot['session_date']['CF'])
        item = next(r for r in snapshot['markets']['equities_and_commodities'] if r['market'] == 'CF')
        self.assertIsNone(item['session_date'])
        self.assertEqual(item['archive_reference_date'], '2026-10-17')

    def test_7d_baseline_does_not_silently_become_10d(self):
        now = self._weekly_now()
        for day in ('2026-10-06', '2026-10-16'):
            self._bar('HK', '07747', day, 100 if day.endswith('06') else 110)
        for day in ('2026-10-09', '2026-10-16'):
            self._bar('HK', 'OTHER', day, 100)
        rows, health = period.stock_period(self.root, 7, now, {})
        self.assertNotIn('07747', {r['symbol'] for r in rows})
        excluded = [r for h in health for r in h['excluded_candidates']]
        self.assertEqual(next(r for r in excluded if r['symbol'] == '07747')['baseline_quality'],
                         'missing_asset_bars')

    def test_weekend_holiday_baseline_is_valid(self):
        sessions = {datetime.fromisoformat(d).date() for d in ('2026-10-08', '2026-10-12', '2026-10-16')}
        self.assertEqual(period.baseline_quality(sessions, datetime(2026, 10, 8).date(),
                                                 datetime(2026, 10, 11).date())[0], 'aligned')

    def _quality_build(self, *, stock_stale=False, macro_stale=False):
        now = self._weekly_now()
        payload = {'refreshed_at': now.timestamp(), 'macro_checked_at': now.timestamp()}
        (self.root / 'data/macro_dashboard_snapshot.json').write_text(json.dumps(payload))
        stock = {'market': 'CN', 'symbol': 'A', 'session_date': '2026-10-16',
                 'quality': 'partial', 'stale': stock_stale}
        crypto = {'market': 'spot', 'symbol': 'BTCUSDT', 'quality': 'ok', 'stale': False}
        rates = [{'symbol': 'DGS10', 'quality': 'stale' if macro_stale else 'partial',
                  'stale': macro_stale}]
        stock_health = [{'domain': 'stocks_CN', 'data_asof': '2026-10-16',
                         'quality': 'partial', 'stale': False}]
        crypto_health = [{'domain': 'crypto_spot', 'data_asof': now.isoformat(),
                          'quality': 'partial', 'stale': False}]
        with patch.object(period, 'stock_period', return_value=([stock], stock_health)), \
             patch.object(period, 'crypto_period', return_value=([crypto], crypto_health)), \
             patch.object(period, 'macro_period', return_value=([], rates)):
            return period.build(self.root, self.crypto, 7, now, scores={})

    def test_snapshot_stale_rollup_includes_nested_assets(self):
        snapshot = self._quality_build(stock_stale=True)
        self.assertTrue(snapshot['stale'])
        self.assertIn('stocks_CN:A', snapshot['stale_item_ids'])

    def test_snapshot_stale_rollup_includes_macro_indicators(self):
        snapshot = self._quality_build(macro_stale=True)
        self.assertTrue(snapshot['stale'])
        self.assertEqual(snapshot['stale_item_count'], 1)
        macro = next(h for h in snapshot['source_health'] if h['domain'] == 'macro')
        self.assertEqual(macro['observation_freshness'], 'stale')
        self.assertEqual(macro['collection_freshness'], 'current')

    def test_fresh_collection_does_not_refresh_old_observation(self):
        snapshot = self._quality_build(macro_stale=True)
        self.assertTrue(snapshot['markets']['macro_rates'][0]['stale'])
        self.assertEqual(snapshot['source_health'][-1]['collection_freshness'], 'current')

    def test_all_fresh_rollup_is_false(self):
        snapshot = self._quality_build()
        self.assertFalse(snapshot['stale'])
        self.assertEqual(snapshot['stale_item_count'], 0)

    def test_one_stale_asset_does_not_mark_all_markets_stale(self):
        snapshot = self._quality_build(macro_stale=True)
        self.assertFalse(next(h for h in snapshot['source_health'] if h['domain'] == 'stocks_CN')['stale'])
        self.assertFalse(next(h for h in snapshot['source_health'] if h['domain'] == 'crypto_spot')['stale'])

    def test_daily_snapshot_classification_compatibility(self):
        self.assertEqual(classify_stock_asset('07709', 'XL二南方海力士', 'STOCK')[0], 'LEVERAGED_ETF')
        self.assertEqual(classify_stock_asset('AAPL', 'Apple', 'STOCK')[0], 'STOCK')

    def test_monthly_quality_rules_reused(self):
        now = datetime(2026, 10, 31, 10, 0, tzinfo=timezone(timedelta(hours=8)))
        for day, close in [('2026-09-30', 100), ('2026-10-30', 110)]:
            self._bar('HK', '07709', day, close)
        rows, _ = period.stock_period(self.root, 30, now, {})
        item = next(r for r in rows if r['symbol'] == '07709')
        self.assertEqual(item['asset_type'], 'LEVERAGED_ETF')
        self.assertIsNone(item['amount_latest'])
        self.assertEqual(item['market_reference_session'], '2026-10-30')

    def test_immutable_history_archive_is_not_overwritten(self):
        archive = self.output / 'weekly/2026-10-14.json'
        first, _ = period.generate(self.root, self.crypto, self.output, 7, self.now, scores={})
        before = archive.read_bytes()
        second, _ = period.generate(self.root, self.crypto, self.output, 7,
                                    self.now + timedelta(minutes=10), scores={})
        self.assertEqual(first, second)
        self.assertEqual(before, archive.read_bytes())

    def test_end_to_end_semantic_json_validation(self):
        snapshot, size = period.generate(self.root, self.crypto, self.output, 7, self.now, scores={})
        encoded = (self.output / 'weekly/2026-10-14.json').read_text()
        self.assertEqual(json.loads(encoded), snapshot)
        self.assertLess(size, period.MAX_BYTES)
        self.assertNotIn('NaN', encoded)
        self.assertTrue(all(r['is_latest_eligible'] for r in snapshot['markets']['equities_and_commodities']))
        self.assertEqual(snapshot['stale'], bool(snapshot['stale_item_count']) or any(
            h.get('stale') for h in snapshot['source_health']))


if __name__ == '__main__':
    unittest.main()
