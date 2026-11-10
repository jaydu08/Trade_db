import json
import sys
import tempfile
import unittest
from datetime import datetime,timedelta
from pathlib import Path
from unittest.mock import Mock,patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from modules.ingestion.commodity_quote import parse_quotes,fetch_quotes,PRODUCTS
from modules.monitor.macro_dashboard import refresh_futures_snapshot,store_snapshot
from generate_agent_snapshot import macro_snapshot,macro_freshness


class CommodityTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.fromisoformat('2026-11-10T19:55:00+08:00')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root/'data').mkdir()
        self.path = self.root/'data/macro_dashboard_snapshot.json'

    def sample(self,code='HG',price='630',settlement='634.45',day='2026-11-10',clock='19:53:06'):
        return f'v_hf_{code}="{price},-0.70,630,630,640,629,{clock},{settlement},638,0,7,5,{day},name";'

    def test_prices_units_and_settlement_basis(self):
        rows = parse_quotes(''.join(self.sample(c) for c in PRODUCTS),self.now)
        self.assertEqual(len(rows),5)
        copper = next(r for r in rows if r['id']=='HG')
        self.assertEqual(copper['value'],630)
        self.assertEqual(copper['unit'],'美分/磅')
        self.assertAlmostEqual(copper['change_pct'],(630/634.45-1)*100)
        self.assertEqual(copper['asof'],'2026-11-10T19:53:06+08:00')
        self.assertEqual(copper['source_checked_at'],self.now.isoformat())

    def test_invalid_and_future_quotes_are_rejected(self):
        for text in ('forbidden',self.sample(price='nan'),self.sample(settlement='0'),
                     self.sample(day='2026-11-11'),self.sample(clock='bad')):
            self.assertEqual(parse_quotes(text,self.now),[])

    @patch('modules.ingestion.commodity_quote.requests.get')
    def test_incomplete_response_fails_not_partial_overwrite(self,get):
        get.return_value.text = self.sample()
        with self.assertRaisesRegex(ValueError,'incomplete'):
            fetch_quotes()

    def test_stale_return_is_masked_not_hidden_only_by_flag(self):
        rows = parse_quotes(self.sample(day='2026-11-05'),self.now)
        self.path.write_text(json.dumps(dict(futures=rows,futures_checked_at=self.now.timestamp())))
        exported,_,_ = macro_snapshot(self.root,self.now)
        row = exported[0]
        self.assertTrue(row['stale'])
        self.assertFalse(row['eligible_for_daily_change'])
        self.assertIsNone(row['change_pct'])
        self.assertIsNotNone(row['reported_change_pct'])
        self.assertEqual(row['observed_at'],'2026-11-05T19:53:06+08:00')

    def test_fresh_quote_has_actual_distinct_check_and_observation(self):
        rows = parse_quotes(self.sample(),self.now)
        self.path.write_text(json.dumps(dict(futures=rows,futures_checked_at=self.now.timestamp())))
        row = macro_snapshot(self.root,self.now)[0][0]
        self.assertFalse(row['stale'])
        self.assertTrue(row['eligible_for_daily_change'])
        self.assertNotEqual(row['observed_at'],row['source_checked_at'])
        self.assertEqual(datetime.fromisoformat(row['source_checked_at']).timestamp(),self.now.timestamp())

    def test_previous_day_or_old_intraday_not_daily_conclusion(self):
        for day,clock in [('2026-11-09','23:53:00'),('2026-11-10','10:00:00')]:
            observed=day+'T'+clock+'+08:00'
            self.assertTrue(macro_freshness('CL',observed,None,self.now,'futures')[1])

    def test_failed_refresh_preserves_success_timestamp_and_price(self):
        original=dict(futures=parse_quotes(self.sample(day='2026-11-05'),self.now),futures_checked_at=100)
        self.path.write_text(json.dumps(original))
        with patch('modules.monitor.macro_dashboard.fetch_futures',side_effect=TimeoutError('timeout')):
            result=refresh_futures_snapshot(self.path)
        self.assertEqual(result['futures'],original['futures'])
        self.assertEqual(result['futures_checked_at'],100)
        self.assertIn('futures',result['errors'])

    def test_slow_writer_does_not_revert_new_successful_quote(self):
        newer=dict(futures=[dict(id='HG',value=630)],futures_checked_at=200,errors={})
        self.path.write_text(json.dumps(newer))
        result=store_snapshot(dict(futures=[dict(id='HG',value=638)],futures_checked_at=100,errors={'futures':'old failure'}),str(self.path))
        self.assertEqual(result['futures'],newer['futures'])
        self.assertEqual(result['futures_checked_at'],200)
        self.assertNotIn('futures',result['errors'])

    def test_versioned_repair_preserves_non_commodity_prices(self):
        from repair_agent_commodity_archive import repair
        repo=self.root/'public'
        output=repo/'agent/v1'
        (output/'history').mkdir(parents=True)
        (output/'health').mkdir()
        original=dict(archive_date='2026-11-10',snapshot_id='original',generated_at='2026-11-10T19:12:00+08:00',
                      overview=dict(macro=[dict(category='futures',id='CL',value=88,stale=True),
                                           dict(category='macro',id='DGS10',value=4,stale=False)],crypto_activity={}),
                      anomalies={'stocks_up':[dict(price=100)]},
                      source_health=[dict(domain='macro',stale=True,collection_stale=False)])
        raw=json.dumps(original).encode()
        (output/'history/2026-11-10.json').write_bytes(raw)
        (output/'health/2026-11-10.json').write_text('{}')
        (output/'latest.json').write_bytes(raw)
        rows=[dict(category='futures',id=c,value=630,stale=False,source_checked_at=self.now.isoformat()) for c in PRODUCTS]
        with patch('repair_agent_commodity_archive.macro_snapshot',return_value=(rows,{},{})), \
             patch('repair_agent_commodity_archive.audit',return_value={'failures':[]}):
            repair(self.root,repo,'2026-11-10',self.now)
            self.assertEqual(repair(self.root,repo,'2026-11-10',self.now),[])
        new=json.loads((output/'history/2026-11-10.json').read_bytes())
        self.assertEqual(new['anomalies'],original['anomalies'])
        self.assertEqual(new['overview']['macro'][0],original['overview']['macro'][1])
        self.assertEqual((repo/new['revision']['original_path']).read_bytes(),raw)

    def test_old_quote_check_cannot_borrow_new_cache_refresh_time(self):
        # Construct an old per-item check with a current outer cache timestamp.
        rows=parse_quotes(self.sample(),self.now)
        rows[0]['source_checked_at']=(self.now-timedelta(hours=3)).isoformat()
        self.path.write_text(json.dumps(dict(futures=rows,futures_checked_at=self.now.timestamp())))
        row=macro_snapshot(self.root,self.now)[0][0]
        self.assertTrue(row['stale'])
        self.assertIsNone(row['change_pct'])

    def test_quality_audit_rejects_stale_daily_return(self):
        from agent_snapshot_health import audit
        payload=dict(schema_version='1.2',snapshot_id='test',generated_at=self.now.isoformat(),
                     overview={'macro':[dict(id='CL',category='futures',stale=True,change_pct=5)]},
                     market_anomalies={'coverage':[]},source_health=[])
        report=audit(payload,self.root/'public',self.now)
        self.assertFalse(report['suitable_for_automated_research'])
        self.assertTrue(any('ineligible observation exposes daily change' in s for s in report['failures']))


if __name__=='__main__':
    unittest.main()
