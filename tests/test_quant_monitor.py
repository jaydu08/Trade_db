import copy,hashlib,json,sys,tempfile,unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from modules.quant.engine import compute,observations,edges,outcome,expected_session,bar_end,FORMULA_VERSION,validate_prices
from modules.quant.store import Store
from modules.quant.sources import normalize_prices
from modules.quant.runtime import atomic
from modules.quant.export import write,summary,snapshot


def prices(n=500,end='2026-11-23',business=False):
    index=pd.bdate_range(end=end,periods=n) if business else pd.date_range(end=end,periods=n)
    close=100+np.arange(n)*.12+np.sin(np.arange(n)/7)*5
    return pd.DataFrame(dict(open=close,high=close*1.02,low=close*.97,close=close),index=index)


class FeatureTests(unittest.TestCase):
    def test_future_changes_do_not_change_past_features(self):
        p=prices();iv=pd.Series(25+np.sin(np.arange(len(p))),index=p.index)
        first=compute('BTC',p,iv)[0];changed=p.copy();changed.iloc[-10:]*=2
        iv2=iv.copy();iv2.iloc[-10:]*=3
        second=compute('BTC',changed,iv2)[0]
        pd.testing.assert_frame_equal(first.iloc[:-10],second.iloc[:-10])

    def test_quantiles_exclude_today(self):
        p=prices();iv=pd.Series(np.linspace(15,35,len(p)),index=p.index)
        before=compute('BTC',p,iv)[0];iv.iloc[-1]=500
        after=compute('BTC',p,iv)[0]
        self.assertEqual(before.iv_q90_1y.iloc[-1],after.iv_q90_1y.iloc[-1])
        self.assertGreater(after.iv.iloc[-1],before.iv.iloc[-1])

    def test_95_percent_is_ceil_and_full_window(self):
        p=prices(business=True);iv=pd.Series(25.,index=p.index)
        iv.iloc[-253:-240]=np.nan # 13 missing of 252 -> 239, below actual 95%.
        f,_=compute('CYB',p,iv)
        self.assertTrue(pd.isna(f.iv_q90_1y.iloc[-1]))
        iv.iloc[-253]=25
        f,_=compute('CYB',p,iv)
        self.assertFalse(pd.isna(f.iv_q90_1y.iloc[-1]))

    def test_iv_needs_full_year_since_first_value(self):
        p=prices();iv=pd.Series(np.nan,index=p.index);iv.iloc[-347:]=25
        f,_=compute('BTC',p,iv)
        self.assertTrue(pd.isna(f.iv_q90_1y.iloc[-1]))
        self.assertTrue(pd.isna(f.iv_percentile_1y.iloc[-1]))

    def test_prior_high_excludes_current_and_ma_includes_current(self):
        p=prices();p.iloc[-1]=dict(open=1000,high=1010,low=990,close=1000)
        f,_=compute('BTC',p)
        self.assertEqual(f.prior_20_close_high.iloc[-1],p.close.iloc[-21:-1].max())
        self.assertEqual(f.ma_20.iloc[-1],p.close.iloc[-20:].mean())

    def test_rv_sample_std_annualization(self):
        p=prices();f,_=compute('BTC',p)
        expected=np.log(p.close).diff().iloc[-20:].std(ddof=1)*np.sqrt(365)*100
        self.assertAlmostEqual(f.rv_20.iloc[-1],expected)

    def test_missing_iv_is_unknown_not_false(self):
        p=prices();f,rules=compute('BTC',p)
        row=next(r for r in observations('BTC',f,rules,datetime.now(timezone.utc)) if r['signal_id']=='IV90_DOWN')
        self.assertIsNone(row['state']);self.assertIsNone(row['new_event'])
        self.assertIsNotNone(next(r for r in observations('BTC',f,rules,datetime.now(timezone.utc)) if r['signal_id']=='RV90_DOWN')['state'])

    def test_gap_recovery_does_not_fabricate_first_event(self):
        p=prices().drop(pd.Timestamp('2026-11-22'));f,_=compute('BTC',p)
        cond=pd.Series(False,index=f.index);cond.iloc[-1]=True;valid=pd.Series(True,index=f.index)
        row=observations('BTC',f,{'BREAK20_ABOVE_MA200':(cond,valid)},datetime.now(timezone.utc))[0]
        self.assertTrue(row['state']);self.assertIsNone(row['new_event']);self.assertTrue(row['event_start_uncertain'])

    def test_cross_uses_both_days_own_ma(self):
        p=prices();f,rules=compute('BTC',p)
        pd.testing.assert_series_equal(rules['RECOVER_MA200'][0],(p.close>f.ma_200)&(p.close.shift(1)<=f.ma_200.shift(1)),check_names=False)

    def test_continuous_state_only_one_edge(self):
        cond=pd.Series([False,True,True,True,False,True]);valid=pd.Series(True,index=cond.index)
        self.assertEqual(list(np.flatnonzero(edges(cond,valid))),[1,5])

    def test_invalid_ohlc_rejected_rounding_tolerated(self):
        p=prices();p.iloc[-1,p.columns.get_loc('high')]=1
        with self.assertRaises(ValueError):validate_prices(p)
        p=prices();p.iloc[-1,p.columns.get_loc('high')]=p.close.iloc[-1]-.000001
        validate_prices(p)


class TimeAndOutcomeTests(unittest.TestCase):
    def test_weekends_and_stock_before_close(self):
        self.assertEqual(str(expected_session('CYB',datetime.fromisoformat('2026-11-22T19:00:00+08:00'))),'2026-11-20')
        self.assertEqual(str(expected_session('HSI',datetime.fromisoformat('2026-11-24T14:00:00+08:00'))),'2026-11-23')
        self.assertEqual(str(expected_session('HSI',datetime.fromisoformat('2026-11-24T16:30:00+08:00'))),'2026-11-24')

    def test_crypto_utc_boundary(self):
        self.assertEqual(str(expected_session('BTC',datetime.fromisoformat('2026-11-24T07:59:00+08:00'))),'2026-11-22')
        self.assertEqual(str(expected_session('BTC',datetime.fromisoformat('2026-11-24T08:00:00+08:00'))),'2026-11-23')

    def test_hk_2027_calendar_and_unknown_cn_year_fail_closed(self):
        self.assertEqual(str(expected_session('HSI',datetime.fromisoformat('2027-02-09T19:00:00+08:00'))),'2027-02-05')
        self.assertEqual(bar_end('HSI','2027-02-05').hour,12)
        with self.assertRaisesRegex(ValueError,'日历尚未核验'):
            expected_session('CYB',datetime.fromisoformat('2027-02-09T19:00:00+08:00'))

    def test_stock_holiday_extra_bar_is_not_imported(self):
        now=datetime.fromisoformat('2026-11-24T14:00:00+08:00');bar=dict(open=100,high=101,low=99,close=100)
        rows=normalize_prices({'2026-11-20':bar,'2026-11-21':bar},'CYB',now)
        self.assertEqual(set(rows),{'2026-11-20'})

    def test_unfinished_bar_not_imported(self):
        now=datetime.fromisoformat('2026-11-24T14:00:00+08:00')
        bar=dict(open=100,high=101,low=99,close=100)
        got=normalize_prices({'2026-11-23':bar,'2026-11-24':bar},'BTC',now)
        self.assertEqual(set(got),{'2026-11-23'})

    def test_next_open_exact_holding_and_mae(self):
        p=prices(40);out=outcome(p,0,30,.004)
        self.assertEqual(out['entry_date'],str(p.index[1].date()));self.assertEqual(out['exit_date'],str(p.index[30].date()))
        self.assertAlmostEqual(out['net_return'],p.close.iloc[30]/p.open.iloc[1]-1-.004)
        self.assertAlmostEqual(out['mae'],min(0,p.low.iloc[1:31].min()/p.open.iloc[1]-1))
        self.assertIsNone(outcome(p,20,30,.004))

    def test_mae_is_never_positive(self):
        p=prices(40);p.open=100.;p.low=100.;p.high=105.;p.close=102.
        self.assertEqual(outcome(p,0,30,0)['mae'],0)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.root=Path(t.name);self.store=Store(self.root);self.addCleanup(self.store.db.close)
        self.p=prices();self.f,_=compute('BTC',self.p)
        cond=pd.Series(False,index=self.p.index);cond.iloc[-1]=True
        self.rules={'BREAK20_ABOVE_MA200':(cond,pd.Series(True,index=cond.index))}
        self.now=datetime.fromisoformat('2026-11-24T06:00:00+00:00')
        previous=str(self.p.index[-2].date())
        self.store.db.execute('INSERT INTO observations VALUES(?,?)',(f'BTC:BREAK20_ABOVE_MA200:{previous}:{FORMULA_VERSION}',json.dumps(dict(state=False,event_start_uncertain=False))))
        self.store.db.commit()

    def test_repeated_run_does_not_duplicate_events(self):
        launch=self.now-timedelta(days=3)
        self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',launch)
        self.assertEqual(self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',launch),0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM events').fetchone()[0],1)

    def test_backfill_is_not_prospective(self):
        self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',self.now)
        self.assertEqual(self.store.events('BTC')[0]['origin'],'historical_backfill')
        self.assertEqual(self.store.forward_summary()['completed_evaluations'],0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM forward').fetchone()[0],0)

    def test_immature_prospective_not_counted_as_win(self):
        self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',self.now-timedelta(days=3))
        summary=self.store.forward_summary()
        self.assertEqual(summary['completed_evaluations'],0);self.assertEqual(summary['evaluating_count'],3)
        self.assertTrue(all(g['wins']==0 for g in summary['groups']))

    def test_disappearing_event_is_revised_not_deleted(self):
        self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',self.now)
        false=pd.Series(False,index=self.p.index)
        self.store.update_events('BTC',self.p,self.f,{'BREAK20_ABOVE_MA200':(false,~false)},self.now,'v2',self.now)
        row=self.store.events('BTC')[0]
        self.assertEqual(row['status'],'revised_out');self.assertEqual(row['revision_count'],1)

    def test_source_price_revision_preserves_old_version(self):
        meta=dict(source='test',data_hash='a',fetched_at=self.now.isoformat())
        self.store.merge('BTC','iv',{'2026-11-23':dict(value=25)},meta)
        result=self.store.merge('BTC','iv',{'2026-11-23':dict(value=26)},dict(meta,data_hash='b'))
        self.assertEqual(result['changed_rows'],1)
        old=json.loads(self.store.db.execute('SELECT previous FROM revisions').fetchone()[0]);self.assertEqual(old['value'],25)

    def test_same_source_repeat_preserves_first_availability(self):
        meta=dict(source='test',available_at=self.now.isoformat(),fetched_at=self.now.isoformat())
        self.store.merge('BTC','iv',{'2026-11-23':dict(value=25)},meta)
        later=(self.now+timedelta(hours=1)).isoformat()
        self.store.merge('BTC','iv',{'2026-11-23':dict(value=25)},dict(meta,available_at=later,fetched_at=later))
        row=self.store.series('BTC','iv')['2026-11-23']
        self.assertEqual(row['available_at'],meta['available_at']);self.assertEqual(row['fetched_at'],later)
        self.store.merge('BTC','iv',{'2026-11-23':dict(value=26)},dict(meta,available_at=later,fetched_at=later))
        self.assertEqual(self.store.series('BTC','iv')['2026-11-23']['available_at'],later)

    def test_prospective_matures_at_exact_horizon_and_fixes_entry(self):
        self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',self.now-timedelta(days=3))
        extended=prices(530,end='2026-12-23')
        self.store.evaluate_forward('BTC',extended.iloc[:501],self.now+timedelta(days=1))
        event=self.store.events('BTC')[0];entry=event['forward'][0]['entry_price']
        extended.iloc[500,extended.columns.get_loc('open')]*=2
        self.store.evaluate_forward('BTC',extended,self.now+timedelta(days=30))
        finished=self.store.events('BTC')[0]['forward'][0]
        self.assertEqual(finished['status'],'completed');self.assertEqual(finished['entry_price'],entry)
        self.assertAlmostEqual(finished['net_return'],extended.close.iloc[529]/entry-1-.004)
        self.assertEqual(self.store.forward_summary()['completed_evaluations'],1)
        preserved=json.dumps(finished,sort_keys=True)
        extended.close*=2
        self.store.evaluate_forward('BTC',extended,self.now+timedelta(days=31))
        self.assertEqual(json.dumps(self.store.events('BTC')[0]['forward'][0],sort_keys=True),preserved)

    def test_source_switch_history_is_not_lost_on_following_poll(self):
        self.store.feed('price:BTC',dict(source='B',previous_source='A',source_switch=True,fetched_at=self.now.isoformat()))
        self.store.feed('price:BTC',dict(source='B',source_switch=False))
        self.assertEqual(self.store.feed('price:BTC')['source_switch_history'][0]['previous_source'],'A')

    def test_missing_old_holding_bar_cannot_mature_by_counting_later_bars(self):
        self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',self.now-timedelta(days=3))
        extended=prices(540,end='2027-01-02').drop(pd.Timestamp('2026-11-28'))
        self.store.evaluate_forward('BTC',extended,self.now+timedelta(days=40))
        row=self.store.events('BTC')[0]['forward'][0]
        self.assertEqual(row['status'],'data_gap');self.assertEqual(self.store.forward_summary()['completed_evaluations'],0)

    def test_post_launch_historical_backfill_not_prospective(self):
        cond=pd.Series(False,index=self.p.index);cond.iloc[-2]=True
        self.store.update_events('BTC',self.p,self.f,{'BREAK20_ABOVE_MA200':(cond,~pd.Series(False,index=cond.index))},self.now,'v1',self.now-timedelta(days=10))
        self.assertEqual(self.store.events('BTC')[0]['origin'],'historical_backfill')

    def test_recovered_unknown_previous_observation_not_new_live_event(self):
        previous=str(self.p.index[-2].date())
        self.store.db.execute('UPDATE observations SET payload=? WHERE id=?',(json.dumps(dict(state=None)),f'BTC:BREAK20_ABOVE_MA200:{previous}:{FORMULA_VERSION}'));self.store.db.commit()
        self.store.update_events('BTC',self.p,self.f,self.rules,self.now,'v1',self.now-timedelta(days=10))
        self.assertEqual(self.store.events('BTC')[0]['origin'],'historical_backfill')
        self.assertFalse(self.store.inserted_event_ids)


class ExportTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.root=Path(t.name)/'root';self.output=Path(t.name)/'repo/agent/v1'
        self.now=datetime.fromisoformat('2026-11-24T19:12:00+08:00')
        self.data=dict(schema_version='1.0',snapshot_id='test',generated_at=self.now.isoformat(),formula_version=FORMULA_VERSION,
                       research_version='test-r1',quality='partial',markets=[dict(market='BTC',name='BTC',session_date='2026-11-23',quality='ok',iv_quality='ok')],signals=[],forward={})
        atomic(self.root/'data/quant/latest.json',self.data)
        atomic(self.root/'data/quant/evidence.json',dict(research_version='test-r1',records=[],primary=[]))
        atomic(self.root/'data/quant/backtest_events.json',dict(research_version='test-r1',events=[]))

    def test_manifest_hashes_and_immutable_daily(self):
        allowed=write(self.root,self.output,self.now)
        self.assertIn('agent/v1/quant/history/2026-11-24.json',allowed)
        manifest=json.loads((self.output/'quant/manifest.json').read_text())
        for path,digest in manifest['files'].items():self.assertEqual(hashlib.sha256((self.output.parent.parent/path).read_bytes()).hexdigest(),digest)
        before=(self.output/'quant/history/2026-11-24.json').read_bytes()
        atomic(self.root/'data/quant/latest.json',dict(self.data,snapshot_id='changed'))
        self.assertIn('agent/v1/quant/history/2026-11-24.json',write(self.root,self.output,self.now))
        self.assertEqual((self.output/'quant/history/2026-11-24.json').read_bytes(),before)

    def test_period_summary_includes_period_events_not_just_current_state(self):
        atomic(self.root/'data/quant/events/BTC.json',dict(events=[dict(id='earlier',market='BTC',signal_id='RV90_DOWN',session_date='2026-11-21',origin='historical_backfill')]))
        result=summary(self.root,self.now,7)
        self.assertEqual(result['period_events'][0]['id'],'earlier');self.assertIn('calendar',result['window']['basis'])

    def test_outdated_collector_does_not_claim_current_signals(self):
        atomic(self.root/'data/quant/latest.json',dict(self.data,generated_at=(self.now-timedelta(days=2)).isoformat(),signals=[dict(market='BTC',state=True,new_event=True)]))
        record=snapshot(self.root,self.now)
        self.assertTrue(record['collector_stale']);self.assertIsNone(record['signals'][0]['state'])

    def test_preview_does_not_freeze_daily_archive(self):
        allowed=write(self.root,self.output,self.now,preview=True)
        self.assertTrue(any('/previews/' in p for p in allowed));self.assertFalse((self.output/'quant/history/2026-11-24.json').exists())

    def test_failed_manifest_write_recovers_without_rewriting_frozen_history(self):
        from modules.quant import export
        real=export.atomic
        def fail(path,payload):
            if Path(path).name=='manifest.json':raise OSError('simulated full filesystem')
            return real(path,payload)
        with patch.object(export,'atomic',side_effect=fail):
            with self.assertRaises(OSError):write(self.root,self.output,self.now)
        frozen=(self.output/'quant/history/2026-11-24.json').read_bytes()
        allowed=write(self.root,self.output,self.now)
        self.assertEqual((self.output/'quant/history/2026-11-24.json').read_bytes(),frozen)
        self.assertTrue(any('/evidence/' in p for p in allowed));self.assertTrue(any('/event_versions/' in p for p in allowed))

    def test_frozen_dependency_tampering_is_rejected(self):
        write(self.root,self.output,self.now)
        frozen=json.loads((self.output/'quant/history/2026-11-24.json').read_text())
        ref=frozen['event_files']['BTC']['path'];(self.output.parent.parent/ref).write_text('{}')
        with self.assertRaisesRegex(ValueError,'hash mismatch'):write(self.root,self.output,self.now)

    def test_period_budget_does_not_remove_existing_market_content(self):
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
        import generate_agent_period_snapshot as period
        old=dict(snapshot_id='large',markets={'original_data':'X'*500},quant_signals=dict(history_path='agent/v1/quant/weekly/2026-11-24.json',quality='partial',excess='Y'*1500,rule_count=46))
        with patch.object(period,'build',return_value=copy.deepcopy(old)),patch.object(period,'MAX_BYTES',1400):
            result,size=period.generate(self.root,self.root/'crypto',self.output,7,self.now)
        self.assertLess(size,1400);self.assertEqual(result['markets'],old['markets'])
        self.assertEqual(result['quant_signals']['history_path'],old['quant_signals']['history_path'])


if __name__=='__main__':unittest.main()
