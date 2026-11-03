"""Privacy boundary, owner isolation and stable Agent focus publication."""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from agent_research_focus import build_focus, write_focus, _owner_chat_id
import publish_agent_snapshot as publisher


class FocusTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / 'data'
        self.data.mkdir()
        self.now = datetime(2026, 10, 21, 12, tzinfo=timezone(timedelta(hours=8)))
        self.owner = patch('agent_research_focus._owner_chat_id', return_value=11)
        self.owner.start()
        self.addCleanup(self.owner.stop)
        (self.data / 'agent_focus_config.json').write_text(json.dumps(dict(enabled=True, username='game2du')))
        (self.data / 'watchlist.json').write_text(json.dumps({
            'US:105.BNC': dict(market='US', symbol='105.BNC', name='BNC',
                               entry_price=987654, tags='PRIVATE_NOTE_SENTINEL'),
            'HK:700': dict(market='HK', symbol='700', name='腾讯'),
            'CN:000001': dict(market='CN', symbol='000001', name='平安'),
            'HK:00001': dict(market='HK', symbol='00001', name='长和'),
            'US:INACTIVE': dict(market='US', symbol='INACTIVE', is_active=False)}))
        (self.data / 'watchlists').mkdir()
        (self.data / 'watchlists/other.json').write_text(json.dumps({
            'US:OTHERUSER': dict(market='US', symbol='OTHERUSER')}))
        with sqlite3.connect(self.data / 'ledger.db') as db:
            db.execute('CREATE TABLE papertrade (symbol TEXT,name TEXT,market TEXT,status TEXT,chat_id INT,entry_reason TEXT)')
            db.executemany('INSERT INTO papertrade VALUES (?,?,?,?,?,?)', [
                ('BNC', 'BNC', 'US', 'ACTIVE', 11, 'SECRET_HOLD_REASON'),
                ('105.MSTR', 'Strategy', 'US', 'ACTIVE', 11, 'SECRET_HOLD_REASON'),
                ('CLOSED', 'Closed', 'US', 'CLOSED', 11, ''),
                ('OTHERUSER', 'Other', 'US', 'ACTIVE', 22, '')])
        with sqlite3.connect(self.data / 'meta.db') as db:
            db.execute('CREATE TABLE asset (symbol TEXT,name TEXT,market TEXT,asset_type TEXT)')
            db.execute('CREATE TABLE asset_profile (symbol TEXT,main_business TEXT,products TEXT,company_profile TEXT,updated_at TEXT)')
            db.execute('CREATE TABLE industry (code TEXT,name TEXT,level INT)')
            db.execute('CREATE TABLE asset_industry_link (symbol TEXT,industry_code TEXT,is_primary INT)')
            db.execute('INSERT INTO asset VALUES (?,?,?,?)', ('105.BNC', 'CEA', 'US', 'STOCK'))
            db.execute('INSERT INTO asset_profile VALUES (?,?,?,?,?)',
                       ('105.BNC', '公司业务', '产品', '简介', '2026-10-20 12:00:00'))
            db.execute('INSERT INTO industry VALUES (?,?,?)', ('001', '行业', 2))
            db.execute('INSERT INTO asset_industry_link VALUES (?,?,?)', ('105.BNC', '001', 1))

    def test_combined_allowlist_owner_isolation_and_public_company_context(self):
        focus = build_focus(self.root, self.now)
        identities = {(i['market'], i['symbol']) for i in focus['items']}
        self.assertEqual(identities, {('US', 'BNC'), ('US', 'MSTR'), ('HK', '00700'),
                                      ('CN', '000001'), ('HK', '00001')})
        bnc = next(i for i in focus['items'] if i['symbol'] == 'BNC')
        self.assertEqual((bnc['main_business'], bnc['industry']), ('公司业务', '行业'))
        serialized = json.dumps(focus)
        for private in ('entry_price', '987654', 'PRIVATE_NOTE_SENTINEL', 'SECRET_HOLD_REASON',
                        'entry_reason', 'chat_id', 'game2du', 'OTHERUSER', 'CLOSED', 'INACTIVE'):
            self.assertNotIn(private, serialized)
        self.assertTrue(all('source_kind' not in i and 'is_hold' not in i for i in focus['items']))
        self.assertEqual(focus['recent_news_dedup']['status'], 'not_available')

    def test_empty_is_distinct_from_unavailable_and_disabled(self):
        (self.data / 'watchlist.json').write_text('{}')
        with sqlite3.connect(self.data / 'ledger.db') as db:
            db.execute('DELETE FROM papertrade')
        focus = build_focus(self.root, self.now)
        self.assertEqual(focus['count'], 0)
        self.assertIsNotNone(focus['empty_reason'])
        self.assertIsNone(focus['missing_reason'])
        (self.data / 'watchlist.json').write_text('invalid json')
        focus = build_focus(self.root, self.now)
        self.assertEqual(focus['quality'], 'partial')
        self.assertIsNone(focus['empty_reason'])
        (self.data / 'agent_focus_config.json').unlink()
        self.assertIsNone(build_focus(self.root, self.now))

    def test_focus_is_stable_until_public_content_changes(self):
        output = self.root / 'public/agent/v1'
        one = write_focus(self.root, output, self.now)
        two = write_focus(self.root, output, self.now + timedelta(hours=1))
        self.assertEqual(one, two)
        (self.data / 'watchlist.json').write_text('{}')
        three = write_focus(self.root, output, self.now + timedelta(hours=2))
        self.assertNotEqual(one['generated_at'], three['generated_at'])
        self.assertEqual(three['count'], 2)

    def test_focus_only_does_not_scan_or_publish_daily_or_period(self):
        repo = self.root / 'public'
        (repo / '.git').mkdir(parents=True)
        def fake_git(_repo, *args):
            return 'agent-data' if args == ('branch', '--show-current') else 'test-id' if args == ('rev-parse', 'HEAD') else ''
        with patch.object(publisher, 'git', side_effect=fake_git), \
             patch.object(publisher, 'scan_agent_market') as scan, \
             patch.object(publisher, 'generate') as daily, \
             patch.object(publisher, 'generate_period') as period, \
             patch.object(publisher, 'write_smoke'):
            published, _ = publisher.publish(self.root, self.root / 'crypto', repo, focus_only=True)
        self.assertEqual(published, [('research_focus', 5)])
        self.assertTrue((repo / 'agent/v1/focus.json').exists())
        scan.assert_not_called()
        daily.assert_not_called()
        period.assert_not_called()

    def test_owner_mapping_matches_primary_and_explicit_ui_override(self):
        with patch.dict(os.environ, {'WEB_USERNAME': 'game2du', 'ALLOWED_USER_IDS': '77,88',
                                     'WEB_USERS_JSON': '{"game2du":{"chat_id":99},"other":{}}'}):
            self.assertEqual(_owner_chat_id(self.root, 'game2du'), 99)
            self.assertLess(_owner_chat_id(self.root, 'other'), 0)
            with self.assertRaisesRegex(ValueError, 'not configured'):
                _owner_chat_id(self.root, 'unconfigured')
        with patch.dict(os.environ, {'WEB_USERNAME': 'game2du', 'ALLOWED_USER_IDS': '77,88',
                                     'WEB_USERS_JSON': '{}'}):
            self.assertEqual(_owner_chat_id(self.root, 'game2du'), 77)


if __name__ == '__main__':
    unittest.main()
