import datetime
import unittest
from unittest.mock import patch

import pandas as pd

from modules.paper_trading.reviewer import PaperTradeReviewer


class _FakeCollection:
    def __init__(self, strict_data, relaxed_data):
        self.strict_data = strict_data
        self.relaxed_data = relaxed_data

    def query(self, query_texts, n_results, where=None):
        if where:
            return self.strict_data
        return self.relaxed_data


class PaperTradeReviewerTests(unittest.TestCase):
    def test_calc_pct_from_df_cn_columns(self):
        df = pd.DataFrame([{"收盘": 10}, {"收盘": 12}])
        self.assertEqual(PaperTradeReviewer._calc_pct_from_df(df), 20.0)

    def test_calc_pct_from_df_close_columns(self):
        df = pd.DataFrame([{"close": 20}, {"close": 18}])
        self.assertEqual(PaperTradeReviewer._calc_pct_from_df(df), -10.0)

    def test_calc_pct_from_df_invalid(self):
        df = pd.DataFrame([{"foo": 1}, {"foo": 2}])
        self.assertIsNone(PaperTradeReviewer._calc_pct_from_df(df))

    def test_is_related_symbol_list(self):
        self.assertTrue(PaperTradeReviewer._is_related_symbol(["00700", "AAPL"], "00700"))

    def test_is_related_symbol_csv(self):
        self.assertTrue(PaperTradeReviewer._is_related_symbol("00700, 000001", "000001"))

    def test_is_related_symbol_miss(self):
        self.assertFalse(PaperTradeReviewer._is_related_symbol("00700,000001", "600519"))

    @patch("modules.paper_trading.reviewer.get_collection")
    def test_fetch_related_events_relaxed_fallback(self, mock_get_collection):
        strict_data = {"documents": [[]], "metadatas": [[]]}
        relaxed_data = {
            "documents": [["腾讯发布新产品"]],
            "metadatas": [[{"event_date": str(datetime.date.today()), "related_symbols": "00700,09988"}]],
        }
        mock_get_collection.return_value = _FakeCollection(strict_data, relaxed_data)

        text = PaperTradeReviewer._fetch_related_events("00700", datetime.date.today() - datetime.timedelta(days=1))
        self.assertIn("腾讯发布新产品", text)


if __name__ == "__main__":
    unittest.main()
