import argparse,json,logging,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from modules.quant.runtime import run
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--force',action='store_true');p.add_argument('--offline',action='store_true');a=p.parse_args()
    logging.basicConfig(level=logging.INFO)
    result=run(a.root,force=a.force,refresh=not a.offline)
    print(json.dumps(dict(snapshot_id=result.get('snapshot_id'),quality=result.get('quality'),
                          markets=[{k:r.get(k) for k in ('market','session_date','quality','iv_last_session','iv_quality')} for r in result.get('markets',[])],
                          source_errors=result.get('source_errors'),duration=result.get('duration_seconds')),ensure_ascii=False))
