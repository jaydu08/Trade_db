import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from collect_agent_us_closes import collect
from repair_agent_period_us import repair


class USRecoveryTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / 'data').mkdir()
        self.now = datetime.fromisoformat('2026-11-09T20:00:00+08:00')
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            db.execute('CREATE TABLE trendseedpool (date TEXT,market TEXT,symbol TEXT,name TEXT)')
            db.execute('CREATE TABLE trenddailybar (date TEXT,market TEXT,symbol TEXT,name TEXT,open REAL,high REAL,low REAL,close REAL,amount REAL,turnover_rate REAL,source TEXT,catalyst_tags TEXT,updated_at TEXT)')
            for n in range(12):
                db.execute('INSERT INTO trendseedpool VALUES (?,?,?,?)', ('2026-11-05','US',f'TEST{n}',f'Test {n}'))

    def quotes(self, symbols, market):
        return {s:dict(name=s,price=110,prev_close=100,pct_chg=10,amount=10000,
                       volume=100,provider='test',timestamp='2026-11-06 16:00:01',
                       timestamp_timezone='America/New_York',session_scope='regular') for s in symbols}

    def test_real_close_is_idempotent_and_preserves_existing_bars(self):
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            db.execute("INSERT INTO trenddailybar(date,market,symbol,name,close,source) VALUES ('2026-11-07','US','TEST0','Existing',99,'heatmap')")
        result = collect(self.root,self.now,self.quotes)
        self.assertEqual(result['session_date'],'2026-11-06')
        self.assertEqual(result['verified_count'],12)
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            self.assertEqual(db.execute("SELECT close FROM trenddailybar WHERE symbol='TEST0'").fetchone()[0],99)
            self.assertEqual(db.execute('SELECT count(*) FROM trenddailybar').fetchone()[0],12)
        self.assertEqual(result,collect(self.root,self.now,lambda *_:self.fail('immutable same-session cache should be reused')))

    def test_wrong_date_or_low_coverage_never_writes(self):
        def wrong(symbols,market):
            return {s:dict(q,timestamp='2026-11-05 16:00:01') for s,q in self.quotes(symbols,market).items()}
        for getter in (wrong, lambda symbols,market:self.quotes(symbols[:2],market)):
            with self.assertRaisesRegex(ValueError,'coverage insufficient'):
                collect(self.root,self.now,getter)
        self.assertFalse((self.root / 'data/agent_us_closes/2026-11-06.json').exists())
        with sqlite3.connect(self.root / 'data/ledger.db') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM trenddailybar').fetchone()[0],0)

    def test_versioned_repair_preserves_original_and_other_markets(self):
        path = self.root / 'public/agent/v1/weekly/2026-11-08.json'
        path.parent.mkdir(parents=True)
        original = dict(snapshot_id='old', period='weekly',generated_at='2026-11-08T09:30:00+08:00',
                        publication_context={'report_cutoff':'2026-11-08T09:30:00+08:00'},
                        markets={'equities_and_commodities':[dict(market='CN',price=100),dict(market='US',price=200)],
                                 'crypto':[dict(price=123)],'macro_rates':[dict(value=4)]},
                        source_health=[dict(domain='stocks_US',stale=False)],
                        session_date={'US':'2026-11-05'}, data_asof={'stocks_US':'2026-11-05'})
        raw = json.dumps(original).encode()
        path.write_bytes(raw)
        health = dict(domain='stocks_US',stale=False,latest_session_security_count=12,data_asof='2026-11-06')
        with patch('repair_agent_period_us.stock_period',return_value=([dict(market='US',price=210)], [health])):
            new, paths = repair(self.root,path,self.now,apply=True)
            again,_ = repair(self.root,path,self.now,apply=True)
        self.assertEqual(new,again)
        self.assertEqual(paths[1].read_bytes(),raw)
        self.assertEqual(new['markets']['crypto'],original['markets']['crypto'])
        self.assertEqual(new['markets']['macro_rates'],original['markets']['macro_rates'])
        self.assertEqual(new['markets']['equities_and_commodities'][0],original['markets']['equities_and_commodities'][0])
        self.assertEqual(new['generated_at'],original['generated_at'])
        self.assertEqual(new['session_date']['US'],'2026-11-06')

    def test_metadata_repair_never_changes_prices(self):
        from repair_agent_metadata_archive import repair as repair_metadata
        repo = self.root / 'public'
        output = repo / 'agent/v1'
        (output / 'history').mkdir(parents=True)
        (output / 'health').mkdir()
        quote = dict(market='US',price=110,source_checked_at=self.now.isoformat())
        original = dict(archive_date='2026-11-09',snapshot_id='old',generated_at=self.now.isoformat(),
                        anomalies=dict(stocks_up=[quote],stocks_down=[]),
                        market_anomalies={'coverage':[dict(market='US')]},
                        overview={'markets':[dict(market='US',major_index=dict(market='US',close=7000,turnover=25e12))]})
        raw = json.dumps(original).encode()
        (output / 'history/2026-11-09.json').write_bytes(raw)
        (output / 'latest.json').write_bytes(raw)
        (output / 'health/2026-11-09.json').write_text('{}')
        with patch('repair_agent_metadata_archive.audit',return_value={'failures':[]}):
            paths = repair_metadata(repo,'2026-11-09',self.now)
            self.assertEqual(repair_metadata(repo,'2026-11-09',self.now),[])
        new = json.loads((output / 'latest.json').read_bytes())
        self.assertEqual(new['anomalies'],original['anomalies'])
        self.assertEqual(new['overview']['markets'][0]['major_index']['close'],7000)
        self.assertIsNone(new['overview']['markets'][0]['major_index']['turnover'])
        self.assertEqual((repo / new['revision']['original_path']).read_bytes(),raw)


if __name__ == '__main__':
    unittest.main()
