import unittest
from unittest.mock import patch

from modules.monitor.macro_dashboard import parse_fred_csv, parse_fred_response, parse_futures_frame, refresh_snapshot


class FakeFrame:
    def __init__(self, rows):
        self.rows = rows

    def iterrows(self):
        return enumerate(self.rows)


class MacroDashboardTest(unittest.TestCase):
    def test_fred_uses_latest_nonmissing_and_its_previous_observation(self):
        raw = ('observation_date,DFEDTARU,DGS2,DGS10,DFII10,T10YIE,DTWEXBGS\n'
               '2026-10-08,5,4.1,4.5,2.1,2.4,120\n'
               '2026-10-09,5,.,4.6,2.2,2.4,121\n')
        values = {x['id']: x for x in parse_fred_csv(raw)}
        self.assertEqual(values['DGS2']['asof'], '2026-10-08')
        self.assertAlmostEqual(values['DGS10']['change'], 0.1)
        self.assertEqual(values['DTWEXBGS']['value'], 121)

    def test_futures_rejects_missing_price_and_keeps_source_timestamp(self):
        rows = [{'名称': 'NYMEX原油', '最新价': 91.7, '涨跌幅': 1.2,
                 '日期': '2026-10-10', '行情时间': '04:59:58'},
                {'名称': 'COMEX黄金', '最新价': None, '涨跌幅': 2,
                 '日期': '2026-10-10', '行情时间': '04:59:58'}]
        values = parse_futures_frame(FakeFrame(rows))
        self.assertEqual([x['id'] for x in values], ['CL'])
        self.assertIn('2026-10-10T04:59:58+08:00', values[0]['asof'])

    def test_fred_zipped_split_calendars(self):
        import io
        from zipfile import ZipFile
        buffer = io.BytesIO()
        with ZipFile(buffer, 'w') as archive:
            archive.writestr('daily,_7-day.csv', 'observation_date,DFEDTARU\n2026-10-09,5\n')
            archive.writestr('daily.csv', 'observation_date,DGS2,DGS10\n2026-10-09,4,4.5\n')
        self.assertEqual({row['id'] for row in parse_fred_response(buffer.getvalue())},
                         {'DFEDTARU', 'DGS2', 'DGS10'})

    @patch('modules.monitor.macro_dashboard.read_snapshot')
    @patch('modules.monitor.macro_dashboard.fetch_fred')
    @patch('modules.monitor.macro_dashboard.fetch_futures')
    def test_refresh_keeps_successful_old_feed_when_one_source_fails(self, futures, fred, old):
        import tempfile
        from modules.monitor import macro_dashboard as module
        old.return_value = {'macro': [{'id': 'DGS10'}], 'macro_checked_at': 10}
        fred.side_effect = RuntimeError('upstream unavailable')
        futures.return_value = [{'id': 'CL'}]
        with tempfile.TemporaryDirectory() as folder, patch.object(module, 'SNAPSHOT_PATH', folder + '/snapshot.json'), \
                patch('modules.monitor.market_activity.fetch_binance', return_value={'value': 1, 'pairs': 1}), \
                patch('modules.monitor.market_activity.fetch_dex', return_value={'value': 1}), \
                patch('modules.monitor.market_activity.fetch_sentiment', return_value={'value': 50}):
            result = refresh_snapshot(force=True, now=20)
        self.assertEqual(result['macro'], [{'id': 'DGS10'}])
        self.assertEqual(result['futures'], [{'id': 'CL'}])
        self.assertIn('macro', result['errors'])


if __name__ == '__main__':
    unittest.main()
