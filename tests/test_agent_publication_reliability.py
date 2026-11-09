import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import publish_agent_snapshot as publisher
from agent_publication_status import classify_failure, receipt, safe_error, systemd_finalize
from agent_security_identity import deduplicate, canonical_symbol


class IdentityTest(unittest.TestCase):
    def item(self, symbol, market='US', kind='STOCK', session='2026-11-04'):
        return dict(symbol=symbol, raw_symbol=symbol, market=market, instrument_type=kind,
                    session_date=session, price=10, change_pct=-5, source='archive', data_asof='2026-11-04T21:01:00Z')

    def test_aliases_merge_stably_and_preserve_sources(self):
        inputs = [self.item(s) for s in ('INTC', '105.INTC', 'SNDK', '105.SNDK')]
        result = deduplicate(inputs)
        self.assertEqual(len(result), 2)
        self.assertEqual(result, deduplicate(list(reversed(inputs))))
        self.assertEqual(result[0]['raw_symbols'], ['105.INTC', 'INTC'])
        self.assertEqual(len(result[0]['source_records']), 2)

    def test_instrument_market_session_and_share_class_are_separate(self):
        inputs = [self.item('BABA'), self.item('09988', 'HK'), self.item('SOXL', kind='LEVERAGED_ETF'),
                  self.item('BABA', kind='ADR'), self.item('BRK.A'), self.item('BRK.B'),
                  self.item('BABA', session='2026-11-03')]
        self.assertEqual(len(deduplicate(inputs)), 7)
        self.assertEqual(canonical_symbol('US', '999.INTC'), '999.INTC')


class PublicationReliabilityTest(unittest.TestCase):
    def test_independent_attempts_deduplicate_and_recover_once(self):
        from agent_publication_status import alert, recovery
        with tempfile.TemporaryDirectory() as temp, \
             patch('agent_publication_status._send_operational', return_value=True) as send:
            root = Path(temp)
            result = dict(archive_date='2026-11-09', mode='daily', stage='generate',
                          category='data_source_unavailable', diagnostic='US close not ready: 0/100')
            self.assertTrue(alert(root, result))
            self.assertFalse(alert(root, dict(result, diagnostic='US close not ready: 2/101')))
            self.assertFalse(alert(root, result))
            self.assertTrue(recovery(root, result))
            self.assertFalse(recovery(root, result))
            self.assertEqual(send.call_count, 2)
            self.assertTrue(alert(root, result))  # A new failure after recovery must alert.
            self.assertTrue(alert(root, dict(result, stage='upload', category='upload_failed')))

    def test_failed_notification_is_retried(self):
        from agent_publication_status import alert
        result = dict(archive_date='2026-11-09', mode='daily', stage='upload', category='upload_failed')
        with tempfile.TemporaryDirectory() as temp, \
             patch('agent_publication_status._send_operational', side_effect=[False, True]) as send:
            self.assertFalse(alert(Path(temp), result))
            self.assertTrue(alert(Path(temp), result))
            self.assertEqual(send.call_count, 2)

    def test_service_timeout_writes_failure_and_alerts_once(self):
        with tempfile.TemporaryDirectory() as temp, \
             patch.dict(os.environ, {'SERVICE_RESULT': 'timeout', 'INVOCATION_ID': 'test-timeout'}), \
             patch('agent_publication_status.alert') as notify:
            result = systemd_finalize(Path(temp))
            self.assertEqual(result['service_result'], 'timeout')
            self.assertEqual(result['status'], 'failed')
            systemd_finalize(Path(temp))
            notify.assert_called_once()

    def test_failure_categories_and_private_receipts(self):
        exc = subprocess.CalledProcessError(128, ['git'], stderr='Permission denied (publickey).')
        self.assertEqual(classify_failure(exc, 'upload'), 'permission_error')
        self.assertEqual(classify_failure(RuntimeError('timeout'), 'upload'), 'upload_failed')
        self.assertEqual(classify_failure(ValueError('CN source not ready'), 'generate'), 'data_source_unavailable')
        self.assertEqual(classify_failure(ValueError('bad JSON'), 'generate'), 'generation_failed')
        self.assertNotIn('sk-secret', safe_error(ValueError('https://u:secret@host sk-secret'))['diagnostic'])
        with tempfile.TemporaryDirectory() as temp:
            now = datetime(2026, 11, 5, 19, 12, tzinfo=ZoneInfo('Asia/Shanghai'))
            receipt(Path(temp), 'daily', now, 'failed', 'upload', category='upload_failed')
            self.assertEqual(json.loads((Path(temp) / 'data/agent_publication_status/daily-latest.json').read_text())['status'], 'failed')

    def test_first_push_failure_retries_and_reads_explicit_remote_branch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote, repo = root / 'remote.git', root / 'public'
            subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
            subprocess.run(['git', 'init', '-b', 'agent-data', str(repo)], check=True, capture_output=True)
            real_git = publisher.git
            real_git(repo, 'config', 'user.name', 'Test')
            real_git(repo, 'config', 'user.email', 'test@localhost')
            (repo / 'README.md').write_text('public data test')
            real_git(repo, 'add', 'README.md'); real_git(repo, 'commit', '-m', 'initial')
            real_git(repo, 'remote', 'add', 'origin', str(remote))
            real_git(repo, 'push', '-u', 'origin', 'agent-data')
            now = datetime(2026, 11, 5, 19, 12, tzinfo=ZoneInfo('Asia/Shanghai'))
            class Clock:
                @staticmethod
                def now(tz):
                    return now
            payload = dict(archive_date='2026-11-05', snapshot_id='frozen', generated_at=now.isoformat(),
                           overview={'markets': []}, anomalies={}, market_anomalies={'coverage': []}, source_health=[], session_date={})
            def generate(_root, _crypto, output, _now, require_daily_ready):
                for relative in ('history/2026-11-05.json', 'latest.json', 'health/2026-11-05.json'):
                    path = output / relative; path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(payload))
                return payload, 100
            failed = False
            def flaky_git(_repo, *args):
                nonlocal failed
                if args[:1] == ('push',) and not failed:
                    failed = True
                    raise subprocess.CalledProcessError(128, ['git'], stderr='simulated connection reset')
                return real_git(_repo, *args)
            with patch.object(publisher, 'datetime', Clock), patch.object(publisher, 'git', side_effect=flaky_git), \
                 patch.object(publisher, 'collect_market_context', return_value={}), \
                 patch.object(publisher, 'scan_agent_market'), patch.object(publisher, 'generate', side_effect=generate), \
                 patch.object(publisher, 'write_focus', return_value=None), \
                 patch.object(publisher, 'write_extensions', return_value=None), patch.object(publisher, 'write_smoke'):
                # Existing smoke artifact is optional; create one so staging is real Git, not mocked.
                smoke = repo / 'agent/v1/smoke.json'; smoke.parent.mkdir(parents=True, exist_ok=True); smoke.write_text('{}')
                with self.assertRaises(subprocess.CalledProcessError):
                    publisher.publish(root, root / 'crypto', repo, daily_only=True)
                self.assertEqual(publisher.STAGE, 'upload')
                local_commit = real_git(repo, 'rev-parse', 'HEAD')
                publisher.publish(root, root / 'crypto', repo, daily_only=True)
            self.assertEqual(real_git(repo, 'rev-parse', 'HEAD'), local_commit)
            self.assertEqual(publisher.STAGE, 'verify')
            self.assertEqual(json.loads(real_git(repo, 'show', 'origin/agent-data:agent/v1/history/2026-11-05.json')), payload)
            with self.assertRaises(ValueError):
                publisher.verify_remote(repo, {'agent/v1/history/2026-11-05.json'}, '2026-11-04')


if __name__ == '__main__':
    unittest.main()
