import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from modules.analysis import market_relative_strength as strength
from modules.analysis.heatmap import MarketHeatMap


class MarketRelativeStrengthTests(unittest.TestCase):
    def test_shenzhou_qualifies_when_hsi_falls(self):
        self.assertTrue(strength.qualifies("HK", "02313", 2.8, -1.03))
        self.assertFalse(strength.qualifies("HK", "02313", 2.8, 1.0))
        self.assertFalse(strength.qualifies("HK", "02313", -1.0, -5.0))

    def test_board_gates_keep_20cm_and_us_distinct(self):
        self.assertTrue(strength.qualifies("CN", "600000", 3.0, -1.0))
        self.assertFalse(strength.qualifies("CN", "688001", 3.0, -1.0))
        self.assertTrue(strength.qualifies("US", "AAPL", 2.0, -1.0))

    def test_stale_index_uses_liquid_stock_median(self):
        frame = pd.DataFrame([
            {"名称": f"普通股{i}", "最新价": 10, "成交额": 20_000_000, "涨跌幅": -1.0}
            for i in range(100)
        ])
        with patch.object(strength, "fetch_index_return", side_effect=ValueError("stale")):
            result = strength.build_benchmark(frame, "HK", dt.date(2026, 10, 15))
        self.assertEqual(result["return_pct"], -1.0)
        self.assertIn("非指数", result["basis"])
        self.assertEqual(result["sample_size"], 100)

    def test_benchmark_is_date_bound(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(strength, "BENCHMARK_DIR", Path(tmp)):
            strength.save_benchmark({"market": "HK", "date": "2026-10-15", "return_pct": -1.03, "basis": "恒生指数"})
            self.assertEqual(strength.load_benchmark("HK", dt.date(2026, 10, 15))["return_pct"], -1.03)
            self.assertIsNone(strength.load_benchmark("HK", dt.date(2026, 10, 16)))

    def test_hk_heatmap_admits_shenzhou_under_relative_gate(self):
        frame = pd.DataFrame([
            {"代码": "02313", "名称": "申洲国际", "最新价": 37.42, "涨跌幅": 2.8,
             "成交额": 320_000_000, "换手率": 0},
            {"代码": "07552", "名称": "南方两倍做空恒生科技", "最新价": 2.198,
             "涨跌幅": 5.167, "成交额": 402_353_274, "换手率": 0},
        ])
        rows = MarketHeatMap()._generate_heatmap(
            frame, "HK", top_n=7, min_amount=50_000_000, benchmark_pct=-1.03
        )
        self.assertIn("02313", {row["symbol"] for row in rows})


if __name__ == "__main__":
    unittest.main()
