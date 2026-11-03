import datetime as dt
import unittest
from unittest.mock import patch

from interface.api import _refresh_trend_display_prices
from modules.monitor.trend_service import TrendCalculator


class TrendReturnAlignmentTests(unittest.TestCase):
    def test_period_return_requires_a_real_window_base(self):
        prices = {
            dt.date(2026, 6, 15): 100.0,
            dt.date(2026, 7, 20): 120.0,
        }

        ret, current, price_date = TrendCalculator._calculate_period_return(prices, 60)

        self.assertEqual(ret, 0.0)
        self.assertEqual(current, 120.0)
        self.assertEqual(price_date, "")

    def test_period_return_accepts_nearby_trading_day_base(self):
        prices = {
            dt.date(2026, 5, 20): 100.0,
            dt.date(2026, 7, 20): 80.0,
        }

        ret, current, price_date = TrendCalculator._calculate_period_return(prices, 60)

        self.assertEqual(ret, -20.0)
        self.assertEqual(current, 80.0)
        self.assertEqual(price_date, "2026-07-20")

    def test_period_return_rejects_unadjusted_reverse_split(self):
        prices = {
            dt.date(2026, 5, 20): 5.0,
            dt.date(2026, 6, 15): 4.8,
            dt.date(2026, 6, 16): 48.0,
            dt.date(2026, 7, 20): 45.0,
        }

        ret, current, price_date = TrendCalculator._calculate_period_return(prices, 60)

        self.assertEqual(ret, 0.0)
        self.assertEqual(current, 45.0)
        self.assertEqual(price_date, "")

    @patch(
        "modules.ingestion.akshare_client.AkShareClient.get_realtime_quotes_batch",
        return_value={
            "TEST": {
                "symbol": "TEST",
                "price": 80.0,
                "pct_chg": -4.0,
                "amount": 5_000_000.0,
                "timestamp": "2026-07-20 15:00:00",
                "provider": "Sina",
            }
        },
    )
    def test_display_refresh_rebases_price_and_returns_together(self, _mock_quotes):
        payload = {
            "US": [{
                "symbol": "TEST",
                "price": 120.0,
                "return_pct": 20.0,
                "return_20d": 20.0,
                "return_60d": 50.0,
                "trend_score": 30.0,
            }]
        }

        updated = _refresh_trend_display_prices(payload, "US", return_mode="period")
        row = payload["US"][0]

        self.assertEqual(updated, 1)
        self.assertEqual(row["price"], 80.0)
        self.assertEqual(row["return_20d"], -20.0)
        self.assertEqual(row["return_60d"], 0.0)
        self.assertEqual(row["return_pct"], 0.0)
        self.assertEqual(row["price_date"], "2026-07-20")

    @patch(
        "modules.ingestion.akshare_client.AkShareClient.get_realtime_quotes_batch",
        return_value={
            "TEST": {
                "symbol": "TEST",
                "price": 95.0,
                "pct_chg": -5.0,
                "timestamp": "2026-07-20 15:00:00",
                "provider": "Sina",
            }
        },
    )
    def test_daily_refresh_uses_quote_daily_change(self, _mock_quotes):
        payload = {
            "CN": [{
                "symbol": "TEST",
                "price": 100.0,
                "return_pct": 8.0,
                "trend_score": 90.0,
            }]
        }

        _refresh_trend_display_prices(payload, "CN", return_mode="daily")

        self.assertEqual(payload["CN"][0]["price"], 95.0)
        self.assertEqual(payload["CN"][0]["return_pct"], -5.0)


if __name__ == "__main__":
    unittest.main()
