"""Freeze and independently evaluate the prespecified US extension (no TG)."""
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from modules.quant.us_research import install
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);a=p.parse_args()
    result=install(a.root)
    print(json.dumps(dict(research_version=result['research_version'],records=len(result['records']),primary=len(result['primary']),
                          us_audit=result['audit']['us_extension'],us_primary=[r for r in result['primary'] if r['market'] in ('SPX','NDX','SOX')]),ensure_ascii=False))
