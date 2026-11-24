"""Audit frozen study before enabling production quantitative monitoring."""
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from modules.quant.research import evidence
from modules.quant.runtime import atomic
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,required=True);p.add_argument('--replay',type=Path,required=True);p.add_argument('--root',type=Path,required=True)
    a=p.parse_args();result,events=evidence(a.input,a.replay)
    atomic(a.root/'data/quant/evidence.json',result)
    atomic(a.root/'data/quant/backtest_events.json',dict(research_version=result['research_version'],events=events))
    print(json.dumps(dict(research_version=result['research_version'],original_replay_verified=result['audit']['original_replay_verified'],
                          feature_differences=result['audit']['formula_differences'],primary_differences=result['audit']['primary_result_differences'],
                          records=len(result['records']),primary=len(result['primary']),events=len(events)),ensure_ascii=False))
