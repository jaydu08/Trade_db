"""Explicit, versioned identity-only replay from unchanged historical inputs.

Never fetch live quotes or regenerate crypto/macro. Preserve the original public
artifact before replacing the known-bad stock selection. Refuse post-snapshot
source writes to prevent look-ahead. Normal scheduled exports remain immutable.
"""
import argparse
import fcntl
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from generate_agent_snapshot import (stock_anomaly_snapshot, readonly_db, utc_naive_iso,
                                     atomically_write, immutably_create)
from agent_snapshot_health import audit
from publish_agent_snapshot import git, verify_remote


def repair(root, repo, day):
    if git(repo, 'branch', '--show-current') != 'agent-data':
        raise ValueError('repair requires isolated agent-data branch')
    with (repo / '.git/trend-agent-publish.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        git(repo, 'fetch', 'origin', 'agent-data')
        git(repo, 'merge', '--ff-only', 'FETCH_HEAD')
        relative = f'agent/v1/history/{day}.json'
        path = repo / relative
        original = path.read_bytes()
        snapshot = json.loads(original)
        revision = snapshot.get('revision') or {}
        health_path = f'agent/v1/health/{day}.json'
        if revision.get('kind') != 'verified_security_identity_replay':
            generated = datetime.fromisoformat(snapshot['generated_at'])
            if generated.date().isoformat() != day:
                raise ValueError('archive date mismatch')
            scan = root / f'data/agent_market_scan/{day}.json'
            scan_payload = json.loads(scan.read_text())
            if datetime.fromisoformat(scan_payload['generated_at']) > generated:
                raise ValueError('quote scan updated after original snapshot; refusing look-ahead replay')
            with readonly_db(root / 'data/ledger.db') as db:
                row = db.execute('SELECT MAX(updated_at) AS stamp FROM trenddailybar WHERE market=?', ('US',)).fetchone()
            if row['stamp'] and datetime.fromisoformat(utc_naive_iso(row['stamp'])) > generated:
                raise ValueError('US source changed after original snapshot; refusing look-ahead replay')
            up, down, strong, weak, coverage = stock_anomaly_snapshot(root, generated)
            old_prices = {(r['market'], r['symbol'], r['session_date']): r['price']
                          for r in snapshot['anomalies']['stocks_up'] + snapshot['anomalies']['stocks_down']}
            for item in up + down:
                key = (item['market'], item['symbol'], item['session_date'])
                if key in old_prices and item['price'] != old_prices[key]:
                    raise ValueError('historical selected price changed; refusing repair')
            stock_fields = dict(stocks_up=up, stocks_down=down, relative_strong=strong, relative_weak=weak)
            for name in ('anomalies', 'market_anomalies'):
                snapshot[name].update(stock_fields)
            snapshot['market_anomalies']['coverage'] = coverage
            for market in snapshot['overview']['markets']:
                new = next(r for r in coverage if r['market'] == market['market'])
                for key in ('eligible_universe_count', 'selected_count', 'selection_version', 'excluded_reason_counts'):
                    market[key] = new[key]
            for source in snapshot['source_health']:
                if source['domain'].startswith('agent_anomaly_'):
                    market = source['domain'].removeprefix('agent_anomaly_')
                    new = next(r for r in coverage if r['market'] == market)
                    source.update(quality=new['quality'], stale=new['stale'], missing_reason=new['missing_reason'])
            original_id = snapshot['snapshot_id']
            original_path = f'agent/v1/revisions/{day}/{original_id}.original.json'
            original_health = f'agent/v1/revisions/{day}/{original_id}.original-health.json'
            revision = dict(kind='verified_security_identity_replay', version=1,
                            corrected_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
                            original_snapshot_id=original_id, original_path=original_path,
                            original_health_path=original_health, original_sha256=hashlib.sha256(original).hexdigest(),
                            prices_modified=False, historical_input_guard='US bars and A/H scan not updated after original generated_at',
                            reason='plain ticker lacked catalog type; resolve verified provider-alias metadata before identity deduplication')
            snapshot.update(revision=revision, archive_date=day, snapshot_id=original_id + '-identity-r1')
            report = audit(snapshot, repo / 'agent/v1', generated)
            if report['failures']:
                raise ValueError('corrected archive failed quality audit: ' + str(report['failures']))
            immutably_create(repo / original_path, original)
            immutably_create(repo / original_health, (repo / health_path).read_bytes())
            encoded = (json.dumps(snapshot, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()
            atomically_write(path, encoded)
            atomically_write(repo / 'agent/v1/latest.json', encoded)
            atomically_write(repo / health_path, (json.dumps(report, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode())
        allowed = {relative, health_path, 'agent/v1/latest.json', revision['original_path'], revision['original_health_path']}
        git(repo, 'add', '--', *sorted(allowed))
        staged = set(git(repo, 'diff', '--cached', '--name-only').splitlines())
        if staged - allowed:
            raise ValueError('unexpected staged files in repair')
        if staged:
            git(repo, '-c', 'user.name=trend-agent-snapshot', '-c', 'user.email=trend-agent@localhost',
                'commit', '-m', f'Correct verified security identities {day}; preserve original prices and artifact')
        git(repo, 'push', 'origin', 'HEAD:agent-data')
        verify_remote(repo, allowed, day)
        print(json.dumps(dict(status='verified', archive_date=day, revision=revision,
                              commit=git(repo, 'rev-parse', 'HEAD')), ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--day', required=True)
    args = parser.parse_args()
    repair(args.root, args.repo, args.day)
