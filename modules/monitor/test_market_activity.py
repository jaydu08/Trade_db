import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from modules.monitor import market_activity as module


class MarketActivityTest(unittest.TestCase):
    def test_only_active_usdt_quote_volume_is_summed(self):
        tickers = [{'symbol': 'BTCUSDT', 'quoteVolume': '120'},
                   {'symbol': 'BTCUSDT', 'quoteVolume': '120'},
                   {'symbol': 'ETHUSDT', 'quoteVolume': '80'},
                   {'symbol': 'BTCUSDC', 'quoteVolume': '500'}]
        self.assertEqual(module.sum_usdt_volume(tickers, {'BTCUSDT', 'ETHUSDT'}),
                         {'value': 200.0, 'pairs': 2})

    def test_stock_history_deduplicates_same_day_and_rejects_tiny_coverage(self):
        frame = pd.DataFrame({'代码': [f'{i:06d}' for i in range(1020)],
                              '成交额': [1_000_000.0] * 1020})
        with tempfile.TemporaryDirectory() as folder, patch.object(module, 'STOCK_PATH', folder + '/stock.json'):
            self.assertIsNotNone(module.record_stock_turnover('CN', frame, now=1791766800))
            self.assertIsNotNone(module.record_stock_turnover('CN', frame, now=1791766800))
            self.assertEqual(len(module.stock_history()), 1)
            self.assertIsNone(module.record_stock_turnover('CN', frame, now=1791853200))
            self.assertIsNone(module.record_stock_turnover('HK', frame.head(20), now=1791766800))
            self.assertEqual(module.stock_dashboard()[0]['baseline_5d'], None)

    def test_crypto_history_preserves_one_sample_per_hour(self):
        snapshot = {'crypto_activity': {'spot': {'value': 100, 'checked_at': 1791766800}}}
        module.update_crypto_history(snapshot, 1791766800)
        snapshot['crypto_activity']['spot']['value'] = 120
        module.update_crypto_history(snapshot, 1791766900)
        self.assertEqual(len(snapshot['crypto_activity_history']), 1)
        self.assertEqual(snapshot['crypto_activity_history'][0]['spot'], 120)


if __name__ == '__main__':
    unittest.main()
