import copy,json,sys,tempfile,unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from modules.quant.engine import MARKETS,FROZEN_MARKETS,US_MARKETS,CATALOG_VERSION,FORMULA_VERSION,signal_names,compute,observations,expected_session,bar_end,contiguous
from modules.quant.runtime import unavailable_signals,atomic
from modules.quant.sources import normalize_prices,cboe_iv,yahoo
from modules.quant.export import write
from modules.quant.us_research import extend,evaluate,verify_raw,BASE_VERSION,CUTOFF


def prices(n=1200):
    days=pd.bdate_range(end='2025-11-20',periods=n)
    c=100+np.arange(n)*.03+np.sin(np.arange(n)/20)*12
    return pd.DataFrame(dict(open=c,high=c*1.02,low=c*.98,close=c),index=days)


class USCalendarTests(unittest.TestCase):
    def test_us_ny_session_and_thanksgiving(self):
        for market in US_MARKETS:
            self.assertEqual(str(expected_session(market,datetime.fromisoformat('2026-11-24T20:00:00+08:00'))),'2026-11-23')
            self.assertEqual(str(expected_session(market,datetime.fromisoformat('2026-11-26T17:00:00-05:00'))),'2026-11-25')

    def test_us_half_day_availability_and_continuity(self):
        self.assertEqual(str(expected_session('SPX',datetime.fromisoformat('2026-11-27T13:19:00-05:00'))),'2026-11-25')
        self.assertEqual(str(expected_session('SPX',datetime.fromisoformat('2026-11-27T13:20:00-05:00'))),'2026-11-27')
        self.assertEqual(bar_end('SPX','2026-11-27').hour,13)
        self.assertTrue(contiguous('SPX','2026-11-25','2026-11-27'))
        self.assertFalse(contiguous('SPX','2026-11-24','2026-11-27'))

    def test_dst_changes_utc_close_without_changing_ny_session(self):
        from datetime import timezone
        self.assertEqual(bar_end('NDX','2026-07-06').astimezone(timezone.utc).hour,20)
        self.assertEqual(bar_end('NDX','2026-11-23').astimezone(timezone.utc).hour,21)

    def test_unknown_us_calendar_fails_closed(self):
        with self.assertRaisesRegex(ValueError,'NYSE calendar not verified'):
            expected_session('SOX',datetime.fromisoformat('2029-01-03T20:00:00+08:00'))

    def test_holiday_and_unfinished_price_bars_excluded(self):
        bar=dict(open=100,high=101,low=99,close=100)
        rows=normalize_prices({d:bar for d in ('2026-11-25','2026-11-26','2026-11-27')},'SOX',datetime.fromisoformat('2026-11-27T11:00:00-05:00'))
        self.assertEqual(set(rows),{'2026-11-25'})


class USRulesTests(unittest.TestCase):
    def test_registry_is_66_rules_without_changing_original_46(self):
        self.assertEqual(sum(len(signal_names(m)) for m in FROZEN_MARKETS),46)
        self.assertEqual(sum(len(signal_names(m)) for m in MARKETS),66)
        self.assertEqual(MARKETS['SPX']['symbol'],'^GSPC');self.assertEqual(MARKETS['NDX']['symbol'],'^NDX')

    def test_sox_has_own_rv_and_no_iv_rule_even_if_iv_is_supplied(self):
        p=prices();iv=pd.Series(50.,index=p.index)
        f,rules=compute('SOX',p,iv)
        self.assertNotIn('IV90_DOWN',rules);self.assertEqual(len(rules),6)
        self.assertTrue(f.iv.isna().all())
        self.assertAlmostEqual(f.rv_20.iloc[-1],np.log(p.close).diff().iloc[-20:].std(ddof=1)*np.sqrt(252)*100)
        self.assertEqual(len(unavailable_signals('SOX','missing','source down',{})),6)

    def test_spx_ndx_iv_missing_only_blocks_iv_rules(self):
        for m in ('SPX','NDX'):
            f,r=compute(m,prices());rows=observations(m,f,r,datetime.fromisoformat('2026-11-24T00:00:00+00:00'))
            self.assertIsNone(next(x for x in rows if x['signal_id']=='IV90_DOWN')['state'])
            self.assertIsNotNone(next(x for x in rows if x['signal_id']=='RV90_DOWN')['state'])

    def test_zero_sample_remains_null_and_is_not_fabricated_win(self):
        p=prices();p[['open','high','low','close']]=100.
        records,_=evaluate('SOX',p,pd.Series(dtype=float),'test')
        row=next(r for r in records if r['signal_id']=='RV90_DOWN' and r['period']=='all')
        self.assertEqual(row['completed_events'],0);self.assertIsNone(row['win_rate_pct']);self.assertEqual(row['confidence_interval'],[None,None])

    def test_extension_preserves_base_and_does_not_share_market_statistics(self):
        p=prices();ps={str(d.date()):r.to_dict() for d,r in p.iterrows()}
        iv={d:dict(value=25+np.sin(i/7)*10) for i,d in enumerate(ps)}
        inputs=dict(cutoff=CUTOFF,markets={m:dict(prices=ps,iv={} if m=='SOX' else iv,price_source={},iv_source={}) for m in US_MARKETS})
        base=dict(research_version=BASE_VERSION,data_version='base',records=[dict(market='BTC',wins=99)],primary=[],event_count_across_variants=0,audit={},limitations=[])
        old=copy.deepcopy(base);result,events=extend(base,[],inputs)
        self.assertEqual(base,old);self.assertEqual(result['records'][0],old['records'][0])
        primary=[r for r in result['primary'] if r['market'] in US_MARKETS]
        self.assertEqual(len(primary),20);self.assertTrue(all(r['data_version']!='base' for r in primary))
        self.assertFalse(any(r['market']=='SOX' and r['signal_id']=='IV90_DOWN' for r in result['records']))
        inputs['markets']['SPX']['prices']=dict(ps,**{'2026-11-21':next(iter(ps.values()))})
        with self.assertRaisesRegex(ValueError,'future/empty'):extend(base,[],inputs)


class USAdapterAndExportTests(unittest.TestCase):
    def test_frozen_raw_integrity_refuses_tampered_source(self):
        import hashlib
        with tempfile.TemporaryDirectory() as d:
            content=b'original';sha=hashlib.sha256(content).hexdigest();path=Path(d)/'data/quant/raw/Cboe-VIX'/(sha+'.bin')
            path.parent.mkdir(parents=True);path.write_bytes(content)
            inputs=dict(markets=dict(SPX=dict(price_source={},iv_source=dict(source='Cboe-VIX',data_hash=sha))))
            verify_raw(d,inputs);path.write_bytes(b'tampered')
            with self.assertRaisesRegex(ValueError,'hash mismatch'):verify_raw(d,inputs)

    def test_cboe_date_close_identity_and_no_future_iv(self):
        class Response:content=b'DATE,OPEN,HIGH,LOW,CLOSE\n11/20/2026,20,30,19,25\n11/23/2026,25,31,22,27\n11/24/2026,27,40,24,35\n'
        now=datetime.fromisoformat('2026-11-24T20:00:00+08:00')
        with patch('modules.quant.sources.response',return_value=(Response(),{})) as mocked:
            rows,meta=cboe_iv(None,'NDX',now)
            self.assertEqual(max(rows),'2026-11-23');self.assertEqual(rows['2026-11-23']['value'],27)
            self.assertEqual(meta['underlying'],'^NDX');self.assertIn('VXN_History.csv',mocked.call_args.args[2])

    def test_yahoo_rejects_wrong_index_or_timezone(self):
        class Response:
            def json(self):return {'chart':{'result':[{'meta':dict(symbol='QQQ',currency='USD',exchangeTimezoneName='America/New_York')} ]}}
        class Store:
            def series(self,*a):return {}
        with patch('modules.quant.sources.response',return_value=(Response(),{})):
            with self.assertRaisesRegex(ValueError,'symbol/timezone conflict'):yahoo(Store(),'NDX',datetime.fromisoformat('2026-11-24T20:00:00+08:00'))

    def test_old_catalog_unchanged_and_new_catalog_version_immutable(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)/'root';output=Path(d)/'repo/agent/v1';now=datetime.fromisoformat('2026-11-24T20:00:00+08:00')
            old=output/'quant/catalog_versions'/f'{FORMULA_VERSION}.json';atomic(old,dict(original='preserve'))
            atomic(root/'data/quant/latest.json',dict(snapshot_id='us-test',generated_at=now.isoformat(),markets=[],signals=[]))
            write(root,output,now,preview=True)
            self.assertEqual(json.loads(old.read_text()),dict(original='preserve'))
            catalog=output/'quant/catalog_versions'/f'{CATALOG_VERSION}.json'
            self.assertNotIn('IV90_DOWN',json.loads(catalog.read_text())['rule_applicability']['SOX'])
            atomic(catalog,dict(tampered=True));atomic(root/'data/quant/latest.json',dict(snapshot_id='new-preview',generated_at=now.isoformat(),markets=[],signals=[]))
            with self.assertRaisesRegex(ValueError,'immutable quant catalog'):write(root,output,now,preview=True)


if __name__=='__main__':unittest.main()
