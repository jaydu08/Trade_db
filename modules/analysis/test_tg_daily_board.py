import tempfile
import unittest
from unittest.mock import patch

from modules.analysis import tg_daily_board as module


class DailyBoardTest(unittest.TestCase):
    def test_date_stable_snapshot(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(module, 'BOARD_DIR', folder):
            row = {'symbol': '000001', 'change_pct': 5.0}
            module.save_board('CN', '2026-10-12', [row])
            self.assertEqual(module.read_board('CN', '2026-10-12')['items'], [row])
            self.assertIsNone(module.read_board('HK', '2026-10-12'))
            with self.assertRaises(ValueError):
                module.save_board('../CN', '2026-10-12', [row])


if __name__ == '__main__':
    unittest.main()
