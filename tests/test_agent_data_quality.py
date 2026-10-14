"""Quality rules: independent stock scan, source-vs-observation time, coverage."""

import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from agent_market_scan import scan, validate_quote
from agent_snapshot_health import audit
from generate_agent_snapshot import (classify_stock_asset, ensure_daily_ready,
                                     macro_freshness, stock_anomaly_snapshot)


class AgentDataQualityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'data').mkdir()
        self.now = datetime(2026, 10, 14, 18, 30, tzinfo=timezone(timedelta(hours=8)))
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            db.execute('CREATE TABLE trendseedpool (date TEXT,market TEXT,symbol TEXT,name TEXT)')
            db.execute('CREATE TABLE trenddailybar (date TEXT,market TEXT,symbol TEXT,name TEXT,close REAL,amount REAL,turnover_rate REAL,source TEXT,updated_at TEXT)')
            for market in ('CN', 'HK'):
                for i in range(12):
                    db.execute('INSERT INTO trendseedpool VALUES (?,?,?,?)',
                               ('2026-10-13', market, f'{market}{i}', f'公司{i}'))
                    if i < 3:
                        db.execute('INSERT INTO trenddailybar VALUES (?,?,?,?,?,?,?,?,?)',
                                   ('2026-10-14', market, f'{market}{i}', f'公司{i}',
                                    100, 10000, 1, 'heatmap', '2026-10-14 09:35:00'))

    def test_independent_scan_finds_downs_despite_three_hk_hot_bars(self):
        def quotes(symbol, market):
            index = int(symbol[len(market):])
            return dict(price=90 if index % 2 else 110,
                        pct_chg=-10 if index % 2 else 10,
                        timestamp='2026/10/14 16:03' if market == 'HK' else '2026-10-14 15:35:00',
                        provider='Sina')
        scan(self.root, self.root / 'data/agent_market_scan', self.now, quotes)
        up, down, _, _, coverage = stock_anomaly_snapshot(self.root, self.now)
        hk = next(r for r in coverage if r['market'] == 'HK')
        self.assertEqual(hk['source_universe_count'], 12)
        self.assertEqual(hk['available_universe_count'], 12)
        self.assertEqual(hk['negative_eligible_count'], 6)
        self.assertEqual(len([r for r in down if r['market'] == 'HK']), 6)
        self.assertEqual(hk['negative_coverage_status'], 'complete')
        self.assertEqual(len([r for r in up if r['market'] == 'HK']), 6)

    def test_stale_quote_and_fred_are_not_freshened_by_new_check(self):
        bad, reason = validate_quote('HK', self.now.date(), dict(price=100, pct_chg=-2,
            timestamp='2026/10/13 16:03'), self.now)
        self.assertIsNone(bad)
        self.assertEqual(reason, 'wrong_session_date')
        status, stale, reason, expected = macro_freshness(
            'DGS10', '2026-10-09', self.now.timestamp(), self.now)
        self.assertEqual((status, stale, expected), ('stale', True, '2026-10-13'))
        self.assertIn('2026-10-09', reason)
        self.assertEqual(macro_freshness('DGS10', '2026-10-09', self.now.timestamp(),
                         self.now - timedelta(days=1))[0], 'ok')
        self.assertEqual(macro_freshness('DTWEXBGS', '2026-10-09',
                         self.now.timestamp(), self.now)[0], 'unknown')
        self.assertEqual(macro_freshness('DFEDTARU', '2026-10-09',
                         self.now.timestamp(), self.now)[0], 'unknown')

    def test_coverage_change_requires_comparable_version(self):
        output = self.root / 'public/agent/v1'
        (output / 'history').mkdir(parents=True)
        old = {'market_anomalies': {'coverage': [{'market': 'HK', 'coverage_scope': 'other',
                'selection_version': 'old', 'eligible_universe_count': 100}]}}
        (output / 'history/2026-10-13.json').write_text(json.dumps(old))
        snapshot = {'schema_version': '1.2', 'snapshot_id': 'x', 'generated_at': self.now.isoformat(),
                    'overview': {'macro': []}, 'source_health': [],
                    'market_anomalies': {'stocks_up': [], 'stocks_down': [], 'coverage': [
                        dict(market=m, coverage_scope='current', selection_version='v2',
                             eligible_universe_count=10, negative_coverage_status='complete')
                        for m in ('CN', 'HK', 'US')]}}
        report = audit(snapshot, output, self.now)
        self.assertIsNone(report['coverage_change']['HK']['drop_pct'])
        self.assertEqual(report['coverage_change']['HK']['comparison_status'],
                         'insufficient_comparable_history')

    def test_partial_provider_failure_is_visible_and_blocks_complete_publication(self):
        def quotes(symbol, market):
            if market == 'HK' and int(symbol[2:]) >= 4:
                raise TimeoutError('test provider timeout')
            return dict(price=95, pct_chg=-5, timestamp=(
                '2026-10-14 16:03:00' if market == 'HK' else '2026-10-14 15:35:00'))
        scan(self.root, self.root / 'data/agent_market_scan', self.now, quotes)
        _, _, _, _, coverage = stock_anomaly_snapshot(self.root, self.now)
        hk = next(row for row in coverage if row['market'] == 'HK')
        self.assertEqual(hk['available_universe_count'], 4)
        self.assertEqual(hk['excluded_reason_counts']['provider_error'], 8)
        self.assertEqual(hk['negative_coverage_status'], 'insufficient')
        minimal = dict(overview={'markets': [dict(market=m, session_date='2026-10-14',
            candidate_universe=12, turnover=100, data_asof='2026-10-14T16:05:00+08:00')
            for m in ('CN', 'HK')] + [dict(market='US', session_date='2026-10-13',
            candidate_universe=12, data_asof='2026-10-14T00:20:00+00:00')]},
            source_health=[dict(domain=domain, stale=False, quality='ok')
                for domain in ('crypto_spot', 'crypto_futures', 'crypto_onchain', 'macro')],
            market_anomalies={'coverage': coverage})
        with self.assertRaisesRegex(ValueError, 'HK independent anomaly scan'):
            ensure_daily_ready(minimal, self.now)

    def test_no_negative_is_not_fabricated_and_identity_is_explicit(self):
        def quotes(symbol, market):
            return dict(price=105, pct_chg=5, timestamp=(
                '2026-10-14 16:03:00' if market == 'HK' else '2026-10-14 15:35:00'))
        scan(self.root, self.root / 'data/agent_market_scan', self.now, quotes)
        _, down, _, _, coverage = stock_anomaly_snapshot(self.root, self.now)
        self.assertFalse([row for row in down if row['market'] == 'HK'])
        hk = next(row for row in coverage if row['market'] == 'HK')
        self.assertEqual(hk['negative_coverage_status'], 'none_observed')
        self.assertEqual(hk['negative_eligible_count'], 0)
        self.assertEqual(classify_stock_asset('107.SOXS', '三倍做空半导体ETF')[0], 'LEVERAGED_ETF')


if __name__ == '__main__':
    unittest.main()
