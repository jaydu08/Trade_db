"""Private end-to-end artifact check; never publishes or sends TG messages."""
import argparse,hashlib,json,sys,tempfile
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from modules.quant.runtime import run
from modules.quant.export import write
from modules.quant.engine import MARKETS,signal_names
from generate_agent_snapshot import generate as daily
from generate_agent_period_snapshot import generate as period


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--crypto-db',type=Path,required=True);a=p.parse_args()
    first=run(a.root,refresh=False);second=run(a.root,refresh=False)
    assert second['event_changes']==0,'identical full-series replay changed events'
    now=datetime.now(ZoneInfo('Asia/Shanghai'));base=Path(tempfile.mkdtemp(prefix='quant-e2e-'))
    checks=[]
    for days in (None,7,30):
        output=base/str(days)/'agent/v1'
        result,size=daily(a.root,a.crypto_db,output,now,require_daily_ready=False) if days is None else period(a.root,a.crypto_db,output,days,now)
        # Before cash close the parent market preview may legitimately be partial.
        assert result['quant_signals'].get('rule_count',len(result['quant_signals'].get('signals',[])))==sum(len(signal_names(m)) for m in MARKETS)
        files=write(a.root,output,now,days)
        ref=output.parent.parent/result['quant_signals']['history_path']
        assert ref.exists(),'parent quant reference missing'
        manifest=json.loads((output/'quant/manifest.json').read_text())
        for path,digest in manifest['files'].items():
            assert hashlib.sha256((output.parent.parent/path).read_bytes()).hexdigest()==digest,path
        checks.append(dict(period='daily' if days is None else days,parent_bytes=size,quant_files=len(files),references_verified=True))
    print(json.dumps(dict(status='passed',second_run_event_changes=second['event_changes'],outputs=str(base),checks=checks),ensure_ascii=False))
