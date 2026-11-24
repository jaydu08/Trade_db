"""Publish an explicitly labelled quant preview through the existing data branch.

Does not freeze or rewrite the established daily/weekly/monthly market report.
"""
import argparse,fcntl,json,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from publish_agent_snapshot import git,verify_remote
from modules.quant.runtime import run
from modules.quant.export import write


def publish(root,repo,refresh=True):
    if git(repo,'branch','--show-current')!='agent-data':raise ValueError('requires isolated agent-data branch')
    with (repo/'.git/trend-agent-publish.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        git(repo,'fetch','origin','agent-data');git(repo,'merge','--ff-only','FETCH_HEAD')
        if git(repo,'diff','--cached','--name-only'):raise ValueError('unrelated staged files')
        run(root,refresh=refresh)
        with (root/'data/quant/run.lock').open('a') as qlock:
            fcntl.flock(qlock,fcntl.LOCK_SH)
            now=datetime.now(ZoneInfo('Asia/Shanghai'))
            paths=write(root,repo/'agent/v1',now,preview=True)
        if not paths:return dict(status='not_due')
        git(repo,'add','--',*sorted(paths))
        if set(git(repo,'diff','--cached','--name-only').splitlines())-paths:raise ValueError('unexpected staged paths')
        git(repo,'diff','--cached','--check')
        if git(repo,'diff','--cached','--name-only'):
            git(repo,'-c','user.name=trend-agent-snapshot','-c','user.email=trend-agent@localhost',
                'commit','-m','Publish versioned quantitative research preview; preserve market report schedules')
        git(repo,'push','origin','HEAD:agent-data');verify_remote(repo,paths,str(now.date()))
        return dict(status='verified',commit=git(repo,'rev-parse','HEAD'),manifest='agent/v1/quant/manifest.json',
                    preview_paths=[p for p in sorted(paths) if '/previews/' in p],files=len(paths))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--repo',type=Path,required=True);p.add_argument('--offline',action='store_true');a=p.parse_args()
    print(json.dumps(publish(a.root,a.repo,refresh=not a.offline),ensure_ascii=False))
