"""Offline contract and failure-mode checks for the public market snapshot."""

import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/generate_agent_snapshot.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("generate_agent_snapshot", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SnapshotTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "trade"
        self.data = self.root / "data"
        self.data.mkdir(parents=True)
        self.crypto = Path(self.temp.name) / "crypto.sqlite"
        self.output = Path(self.temp.name) / "public/agent/v1"
        self.now = datetime(2026, 10, 13, 19, 45, tzinfo=timezone(timedelta(hours=8)))
        with sqlite3.connect(self.data / "ledger.db") as db:
            db.execute("CREATE TABLE trenddailybar (date TEXT, market TEXT, symbol TEXT, name TEXT, close REAL, amount REAL, turnover_rate REAL, source TEXT, catalyst_tags TEXT, updated_at TEXT)")
            db.execute("CREATE TABLE trendseedpool (date TEXT, market TEXT, symbol TEXT, name TEXT)")
            db.execute("CREATE TABLE dailyrank (id INTEGER PRIMARY KEY, date TEXT, market TEXT, symbol TEXT, change_pct REAL)")
            rows = [
                ("2026-10-12", "CN", "AAA", "上涨股", 10, 100, 1, "heatmap", "", "2026-10-12 09:30:00"),
                ("2026-10-13", "CN", "AAA", "上涨股", 11, 200, 2, "heatmap", "", "2026-10-13 09:35:00"),
                ("2026-10-12", "CN", "BBB", "下跌股", 10, 100, 1, "daily_hot", "", "2026-10-12 09:30:00"),
                ("2026-10-13", "CN", "BBB", "下跌股", 9, 300, 3, "daily_hot", "", "2026-10-13 09:35:00"),
                ("2026-10-13", "HK", "00001", "港股", 80, 500, 2, "heatmap", "", "2026-10-13 09:30:00"),
                ("2026-10-12", "HK", "00001", "港股", 70, 400, 2, "heatmap", "", "2026-10-12 09:30:00"),
                ("2026-10-12", "US", "XYZ", "美股", 90, 400, 2, "heatmap", "", "2026-10-12 00:10:00"),
                ("2026-10-13", "US", "XYZ", "美股", 100, 500, 2, "heatmap", "", "2026-10-13 00:10:00"),
            ]
            db.executemany("INSERT INTO trenddailybar VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        with sqlite3.connect(self.crypto) as db:
            db.execute("CREATE TABLE assets (id TEXT PRIMARY KEY, market TEXT, payload TEXT)")
            db.execute("CREATE TABLE bars (id TEXT, ts INTEGER, price REAL, source TEXT)")
            db.execute("CREATE TABLE health (component TEXT, updated REAL, payload TEXT)")
            db.execute("CREATE TABLE security (id TEXT, payload TEXT)")
            stamp = self.now.timestamp() - 60
            for market in ("spot", "futures", "onchain"):
                db.execute("INSERT INTO health VALUES (?,?,?)", (market, stamp, json.dumps({"status": "ok", "count": 2,
                    "selected_symbols": ["UPUSDT", "DOWNUSDT"]})))
            for symbol, current, previous in (("UPUSDT", 120, 100), ("DOWNUSDT", 80, 100)):
                item_id = "spot:" + symbol
                db.execute("INSERT INTO assets VALUES (?,?,?)", (item_id, "spot", json.dumps(dict(
                    id=item_id, market="spot", symbol=symbol, price=current, updated=stamp,
                    volume_24h=1000000, price_source="Binance last trade"))))
                db.execute("INSERT INTO bars VALUES (?,?,?,?)", (item_id, int(stamp - 43200), previous, "binance"))
        (self.data / "macro_dashboard_snapshot.json").write_text(json.dumps({
            "refreshed_at": self.now.timestamp() - 60,
            "macro": [{"id": "DGS10", "name": "美债10Y", "value": 4.5, "unit": "%",
                       "asof": "2026-10-13", "source": "FRED"}],
            "crypto_activity": {"spot": {"value": 100000, "checked_at": self.now.timestamp() - 60}}
        }), encoding="utf-8")
        (self.data / "market_activity_stock.json").write_text(json.dumps([
            {"market": "CN", "day": "2026-10-13", "amount": 1000000},
            {"market": "HK", "day": "2026-10-13", "amount": 2000000},
        ]), encoding="utf-8")
        (self.data / 'agent_market_scan').mkdir()
        (self.data / 'agent_market_scan/2026-10-13.json').write_text(json.dumps({
            'markets': {
                'CN': {'session_date': '2026-10-13', 'source_universe_count': 2,
                       'available_universe_count': 2, 'excluded_reason_counts': {},
                       'items': [dict(symbol='AAA', name='上涨股', price=11, change_pct=10,
                                      observed_at='2026-10-13T15:30:00+08:00',
                                      source_checked_at='2026-10-13T18:20:00+08:00', provider='Sina'),
                                 dict(symbol='BBB', name='下跌股', price=9, change_pct=-10,
                                      observed_at='2026-10-13T15:30:00+08:00',
                                      source_checked_at='2026-10-13T18:20:00+08:00', provider='Sina')]},
                'HK': {'session_date': '2026-10-13', 'source_universe_count': 1,
                       'available_universe_count': 1, 'excluded_reason_counts': {},
                       'items': [dict(symbol='00001', name='港股', price=80, change_pct=14,
                                      observed_at='2026-10-13T16:03:00+08:00',
                                      source_checked_at='2026-10-13T18:20:00+08:00', provider='Sina')]}
            }}), encoding='utf-8')

    def test_real_fields_direction_session_and_immutable_history(self):
        first, size = MODULE.generate(self.root, self.crypto, self.output, self.now)
        self.assertLess(size, 100_000)
        self.assertEqual(first["schema_version"], "1.2")
        self.assertEqual(first["overview"]["markets"][2]["session_date"], "2026-10-12")
        self.assertEqual(first["overview"]["macro"][0]["value"], 4.5)
        self.assertIn("BBB", [x["symbol"] for x in first["anomalies"]["stocks_down"]])
        self.assertIn("UPUSDT", [x["symbol"] for x in first["anomalies"]["crypto"]])
        self.assertIn("DOWNUSDT", [x["symbol"] for x in first["anomalies"]["crypto"]])
        original = (self.output / "history/2026-10-13.json").read_bytes()
        (self.data / "macro_dashboard_snapshot.json").write_text("{}", encoding="utf-8")
        second, _ = MODULE.generate(self.root, self.crypto, self.output, self.now + timedelta(minutes=5))
        self.assertEqual(first, second)
        self.assertEqual((self.output / "latest.json").read_bytes(), original)

    def test_stale_and_missing_are_explicit(self):
        later = self.now + timedelta(days=2)
        snapshot = MODULE.build_snapshot(self.root, self.crypto, later)
        self.assertTrue(next(h for h in snapshot["source_health"] if h["domain"] == "crypto_spot")["stale"])
        self.assertTrue(next(h for h in snapshot["source_health"] if h["domain"] == "crypto_spot")["quality"] == "ok")
        self.assertEqual(snapshot["overview"]["markets"][0]["major_index"], None)
        self.crypto.unlink()
        absent = MODULE.build_snapshot(self.root, self.crypto, later)
        self.assertEqual(next(h for h in absent["source_health"] if h["domain"] == "crypto")["quality"], "missing")

    def test_completed_session_gate_and_asset_types(self):
        snapshot = MODULE.build_snapshot(self.root, self.crypto, self.now)
        MODULE.ensure_daily_ready(snapshot, self.now, require_market_scan=False)
        self.assertEqual(snapshot["anomalies"]["stocks_up"][0]["asset_type"], "UNKNOWN")
        self.assertEqual(MODULE.classify_stock_asset("107.SOXL", "三倍做多半导体ETF")[0], "LEVERAGED_ETF")
        self.assertEqual(MODULE.classify_stock_asset("105.SPY", "标普500ETF")[0], "ETF")
        snapshot["overview"]["markets"][0]["session_date"] = "2026-10-12"
        with self.assertRaisesRegex(ValueError, "CN completed session"):
            MODULE.ensure_daily_ready(snapshot, self.now, require_market_scan=False)

    def test_daily_requires_postclose_trend_pool_refresh(self):
        snapshot = MODULE.build_snapshot(self.root, self.crypto, self.now)
        cn = snapshot['overview']['markets'][0]
        self.assertEqual(cn['archive_source_counts'].get('trend_pool_refresh_eod'), None)
        with self.assertRaisesRegex(ValueError, 'CN current-day trend pool refresh not ready'):
            MODULE.ensure_daily_ready(snapshot, self.now)


if __name__ == "__main__":
    unittest.main()
