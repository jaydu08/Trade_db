"""Publishing must timestamp the snapshot after its quote scan completes."""

import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import publish_agent_snapshot as publisher


class PublishClockTest(unittest.TestCase):
    def test_snapshot_clock_is_after_scan(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'trade'
            repo = Path(temp) / 'public'
            (root / 'data').mkdir(parents=True)
            (repo / '.git').mkdir(parents=True)
            observed = {}
            def fake_git(_repo, *args):
                return 'agent-data' if args == ('branch', '--show-current') else 'commit-id' if args == ('rev-parse', 'HEAD') else ''

            def fake_generate(_root, _crypto, _output, snapshot_time, require_daily_ready):
                self.assertTrue(require_daily_ready)
                self.assertGreaterEqual(snapshot_time, observed['scan_finished'])
                observed['generated'] = snapshot_time
                return {'snapshot_id': 'test'}, 100

            # A weekday is required; this test is independent of the machine's
            # actual day and does not open a network connection or write Git.
            class FakeDateTime:
                calls = 0

                @classmethod
                def now(cls, tz):
                    cls.calls += 1
                    return datetime(2026, 10, 15, 18, 10, tzinfo=tz) + timedelta(seconds=cls.calls)

            def fake_scan_with_virtual_clock(_root, _output, timer_time):
                observed['scan_started'] = timer_time
                observed['scan_finished'] = datetime(2026, 10, 15, 18, 10, 2,
                                                     tzinfo=ZoneInfo('Asia/Shanghai'))

            with patch.object(publisher, 'git', side_effect=fake_git), \
                 patch.object(publisher, 'scan_agent_market', side_effect=fake_scan_with_virtual_clock), \
                 patch.object(publisher, 'generate', side_effect=fake_generate), \
                 patch.object(publisher, 'write_smoke'), \
                 patch.object(publisher, 'datetime', FakeDateTime):
                published, _ = publisher.publish(root, root / 'crypto.sqlite', repo)
            self.assertEqual(published, [('test', 100)])
            self.assertEqual(observed['generated'].second, 2)

    def test_period_mode_never_runs_daily_scan(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp) / 'public'
            (repo / '.git').mkdir(parents=True)

            class SundayClock:
                @staticmethod
                def now(tz):
                    return datetime(2026, 10, 18, 18, 30, tzinfo=tz)

            def fake_git(_repo, *args):
                return 'agent-data' if args == ('branch', '--show-current') else ''

            with patch.object(publisher, 'git', side_effect=fake_git), \
                 patch.object(publisher, 'scan_agent_market') as scan, \
                 patch.object(publisher, 'generate') as daily, \
                 patch.object(publisher, 'generate_period', return_value=({'snapshot_id': 'week'}, 50)) as period, \
                 patch.object(publisher, 'write_smoke'), \
                 patch.object(publisher, 'datetime', SundayClock):
                published, _ = publisher.publish(Path(temp), Path(temp) / 'crypto.sqlite',
                                                 repo, period_only=True)
            self.assertEqual(published, [('week', 50)])
            self.assertEqual(period.call_args.args[3], 7)
            scan.assert_not_called()
            daily.assert_not_called()

    def test_daily_mode_does_not_publish_monthly_on_month_end(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'trade'
            repo = Path(temp) / 'public'
            (root / 'data').mkdir(parents=True)
            (repo / '.git').mkdir(parents=True)

            class MonthEndClock:
                @staticmethod
                def now(tz):
                    return datetime(2026, 11, 30, 19, 25, tzinfo=tz)

            def fake_git(_repo, *args):
                return 'agent-data' if args == ('branch', '--show-current') else ''

            with patch.object(publisher, 'git', side_effect=fake_git), \
                 patch.object(publisher, 'scan_agent_market'), \
                 patch.object(publisher, 'generate', return_value=({'snapshot_id': 'day'}, 50)), \
                 patch.object(publisher, 'generate_period') as period, \
                 patch.object(publisher, 'write_smoke'), \
                 patch.object(publisher, 'datetime', MonthEndClock):
                published, _ = publisher.publish(root, root / 'crypto.sqlite', repo,
                                                 daily_only=True)
            self.assertEqual(published, [('day', 50)])
            period.assert_not_called()


if __name__ == '__main__':
    unittest.main()
