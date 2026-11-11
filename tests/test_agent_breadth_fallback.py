import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from modules.ingestion import breadth_fallback as backup


class BreadthFallbackTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        self.now=datetime.fromisoformat('2026-11-11T19:12:00+08:00')
        p=patch.dict(backup.MINIMUM,dict(CN=3,HK=3,US=3))
        p.start();self.addCleanup(p.stop)

    def row(self,symbol,pct=1,stamp='2026-11-11 15:00:00',source='Tencent'):
        return dict(代码=symbol,最新价=100,涨跌幅=pct,行情时间=stamp,行情来源=source)

    def records(self):
        return [self.row('600001',1),self.row('600002',-1),self.row('600003',0)]

    def test_exact_counts_and_explicit_non_trend_scope(self):
        result=backup.frame_breadth('CN',self.records(),'2026-11-11',self.now)
        self.assertEqual([result[k] for k in ('advancing','declining','unchanged')],[1,1,1])
        self.assertFalse(result['is_all_market_total'])
        self.assertFalse(result['stale'])
        self.assertIn('not Trend',result['coverage_scope'])

    def test_intraday_wrong_date_and_missing_timestamp_are_excluded(self):
        rows=self.records()+[self.row('600004',stamp='2026-11-10 15:00:00'),self.row('600005',stamp='2026-11-11 10:00:00'),self.row('600006',stamp='')]
        result=backup.frame_breadth('CN',rows,'2026-11-11',self.now)
        self.assertEqual(result['covered_count'],3)
        self.assertEqual(result['excluded_reason_counts'],dict(wrong_session_date=1,incomplete_session_quote=1,missing_quote_time=1))

    def test_alias_dedup_does_not_merge_different_tickers(self):
        rows=[self.row(s,stamp='2026-11-10 16:00:01') for s in ('INTC','105.INTC','BRK.A','BRK.B')]
        result=backup.frame_breadth('US',rows,'2026-11-10',self.now)
        self.assertEqual(result['covered_count'],3)
        self.assertEqual(result['duplicate_alias_count'],1)

    def test_primary_502_automatically_uses_saved_independent_source(self):
        backup.save_frame(self.root,'CN',pd.DataFrame(self.records()),self.now)
        response=requests.Response();response.status_code=502
        error=requests.HTTPError('502',response=response)
        with patch('modules.ingestion.market_context.full_market_rows',side_effect=error), \
             patch.object(backup,'live_backup') as live:
            result=backup.fetch_breadth(self.root,'CN','2026-11-11',self.now)
        live.assert_not_called()
        self.assertTrue(result['fallback_used'])
        self.assertEqual(result['session_date'],'2026-11-11')
        self.assertEqual(result['source_attempts'][0]['http_status'],502)
        self.assertIn('Tencent',result['source'])

    def test_primary_failure_uses_live_when_no_saved_session(self):
        record=backup.frame_breadth('CN',self.records(),'2026-11-11',self.now)
        with patch('modules.ingestion.market_context.full_market_rows',side_effect=TimeoutError()), \
             patch.object(backup,'live_backup',return_value=record) as live:
            result=backup.fetch_breadth(self.root,'CN','2026-11-11',self.now)
        live.assert_called_once()
        self.assertEqual(result['source_attempts'][-1]['mode'],'independent_live_catalog')

    def test_wrong_day_cache_not_reused_and_all_failures_return_null(self):
        backup.save_frame(self.root,'CN',pd.DataFrame(self.records()),self.now)
        with patch('modules.ingestion.market_context.full_market_rows',side_effect=TimeoutError()), \
             patch.object(backup,'live_backup',side_effect=ValueError('no valid session')):
            result=backup.fetch_breadth(self.root,'CN','2026-11-10',self.now)
        self.assertTrue(result['stale'])
        self.assertIsNone(result['advancing'])
        self.assertEqual(len(result['source_attempts']),2)

    def test_low_coverage_is_not_published_as_complete(self):
        record=backup.frame_breadth('CN',self.records()[:2],'2026-11-11',self.now,100)
        self.assertTrue(record['stale'])
        self.assertIsNone(record['advancing'])

    def test_live_us_current_session_cannot_replace_yesterdays_close(self):
        now=datetime.fromisoformat('2026-11-11T23:45:00+08:00')
        with self.assertRaisesRegex(ValueError,'live session advanced'):
            backup.live_backup(self.root,'US','2026-11-10',now)

    def test_weekend_is_not_misidentified_as_a_new_live_us_session(self):
        now=datetime.fromisoformat('2026-11-14T23:45:00+08:00')
        with patch('modules.ingestion.breadth_fallback.sqlite3.connect',side_effect=ValueError('catalog reached')):
            with self.assertRaisesRegex(ValueError,'catalog reached'):
                backup.live_backup(self.root,'US','2026-11-13',now)

    def test_saved_record_keeps_original_collection_time(self):
        checked=datetime.fromisoformat('2026-11-11T07:00:00+08:00')
        rows=[self.row(s,stamp='2026-11-10 16:00:01') for s in ('AAPL','INTC','MSFT')]
        backup.save_frame(self.root,'US',pd.DataFrame(rows),self.now,checked_at=checked)
        record=backup.load_saved(self.root,'US','2026-11-10',self.now)
        self.assertEqual(record['source_checked_at'],checked.isoformat())

    def test_good_primary_does_not_call_backup(self):
        with patch('modules.ingestion.market_context.full_market_rows',return_value=([],3)), \
             patch('modules.ingestion.market_context.breadth_from_rows',return_value=dict(source='Eastmoney',stale=False)), \
             patch.object(backup,'live_backup') as live:
            record=backup.fetch_breadth(self.root,'CN','2026-11-11',self.now)
        self.assertFalse(record['fallback_used']);live.assert_not_called()

    def test_unqualified_primary_also_switches_source(self):
        expected=backup.frame_breadth('CN',self.records(),'2026-11-11',self.now)
        with patch('modules.ingestion.market_context.full_market_rows',return_value=([],3)), \
             patch('modules.ingestion.market_context.breadth_from_rows',return_value=dict(stale=True,missing_reason='coverage insufficient')), \
             patch.object(backup,'live_backup',return_value=expected):
            record=backup.fetch_breadth(self.root,'CN','2026-11-11',self.now)
        self.assertTrue(record['fallback_used'])

    def test_revision_preserves_all_other_market_fields_and_original(self):
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
        from repair_agent_breadth_archive import repair
        repo=self.root/'public';output=repo/'agent/v1'
        (output/'history').mkdir(parents=True);(output/'health').mkdir()
        original=dict(snapshot_id='old',archive_date='2026-11-11',generated_at=self.now.isoformat(),
                      overview=dict(markets=[dict(market='CN',session_date='2026-11-11',price=99,breadth=None,
                                                  major_indices=[dict(close=3000,stale=False)])],macro=[dict(value=4,stale=False)]),
                      anomalies={'stocks_up':[dict(price=100)]},source_health=[dict(domain='market_context_CN',stale=True)])
        raw=json.dumps(original).encode()
        (output/'history/2026-11-11.json').write_bytes(raw)
        (output/'latest.json').write_bytes(raw)
        (output/'health/2026-11-11.json').write_text('{}')
        record=backup.frame_breadth('CN',self.records(),'2026-11-11',self.now)
        with patch('repair_agent_breadth_archive.fetch_breadth',return_value=record), \
             patch('repair_agent_breadth_archive.audit',return_value={'failures':[]}):
            repair(self.root,repo,'2026-11-11',self.now)
            self.assertEqual(repair(self.root,repo,'2026-11-11',self.now),[])
        new=json.loads((output/'latest.json').read_bytes())
        self.assertEqual(new['anomalies'],original['anomalies'])
        self.assertEqual(new['overview']['macro'],original['overview']['macro'])
        self.assertEqual(new['overview']['markets'][0]['price'],99)
        self.assertEqual(new['overview']['markets'][0]['major_indices'],original['overview']['markets'][0]['major_indices'])
        self.assertEqual((repo/new['revision']['original_path']).read_bytes(),raw)


if __name__=='__main__':unittest.main()
