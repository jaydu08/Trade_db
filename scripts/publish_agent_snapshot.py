"""Publish today's frozen public-only snapshot to an isolated Git data branch."""

from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from generate_agent_smoke import write_smoke
from generate_agent_snapshot import generate
from generate_agent_period_snapshot import generate as generate_period
from agent_market_scan import scan as scan_agent_market
from agent_research_focus import write_focus
from agent_market_extensions import write_extensions
from collect_agent_market_context import collect as collect_market_context

STAGE = 'sync'


def verify_remote(repo, paths, archive_date):
    """Read back from the explicitly fetched origin/agent-data, not local HEAD."""
    git(repo, 'fetch', 'origin', 'refs/heads/agent-data:refs/remotes/origin/agent-data')
    verified = []
    for path in sorted(paths):
        if not path.endswith('.json'):
            continue
        raw = git(repo, 'show', 'refs/remotes/origin/agent-data:' + path)
        data = json.loads(raw)
        if path.startswith('agent/v1/history/'):
            if data.get('archive_date') != archive_date or path != f'agent/v1/history/{archive_date}.json':
                raise ValueError('remote archive_date/path mismatch')
            for key in ('snapshot_id', 'generated_at', 'overview', 'anomalies', 'source_health', 'session_date'):
                if key not in data:
                    raise ValueError('remote archive missing structure: ' + key)
            if not isinstance(data['overview'].get('markets'), list) or not isinstance(data['anomalies'], dict):
                raise ValueError('remote archive invalid market structure')
        local = json.loads((repo / path).read_text(encoding='utf-8'))
        if data != local:
            raise ValueError('remote JSON differs from committed local artifact: ' + path)
        verified.append(path)
    return verified


def git(repo, *arguments):
    return subprocess.run(["git", "-C", str(repo), *arguments], check=True,
                          text=True, capture_output=True, timeout=45).stdout.strip()


def publish(root, crypto_db, repo, preview_periods=False, daily_only=False, period_only=False, focus_only=False, extensions_only=False):
    global STAGE
    STAGE = 'sync'
    if sum((preview_periods, daily_only, period_only, focus_only, extensions_only)) > 1:
        raise ValueError('choose only one publication mode')
    if git(repo, "branch", "--show-current") != "agent-data":
        raise RuntimeError("refusing to publish outside isolated agent-data branch")
    lock_path = repo / ".git/trend-agent-publish.lock"
    with lock_path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        git(repo, "fetch", "origin", "agent-data")
        git(repo, "merge", "--ff-only", "FETCH_HEAD")
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        STAGE = 'generate'
        output = repo / "agent/v1"
        allowed = set()
        published = []
        focus = write_focus(root, output, now)
        if focus is not None:
            allowed.add('agent/v1/focus.json')
            published.append(('research_focus', focus['count']))
        if now.weekday() < 5 and not (preview_periods or period_only or focus_only or extensions_only):
            today_archive = output / 'history' / f'{now.date().isoformat()}.json'
            legacy_archive = (today_archive.exists() and
                              not json.loads(today_archive.read_text(encoding='utf-8')).get('market_anomalies'))
            if not today_archive.exists():
                collect_market_context(root, now)
                scan_agent_market(root, root / 'data/agent_market_scan', now)
                # The quote scan runs after the timer fires. Audit against its
                # completion time, not the earlier timer-entry timestamp.
                now = datetime.now(ZoneInfo("Asia/Shanghai"))
            if not legacy_archive:
                snapshot, size = generate(root, crypto_db, output, now, require_daily_ready=True)
                allowed.update({"agent/v1/latest.json", f"agent/v1/history/{now.date().isoformat()}.json",
                                f"agent/v1/health/{now.date().isoformat()}.json"})
                published.append((snapshot["snapshot_id"], size))
        due = [(7, now.weekday() == 6),
               (30, (now + timedelta(days=1)).month != now.month)]
        for days, scheduled in due:
            if not (daily_only or focus_only or extensions_only) and (scheduled or preview_periods):
                folder = "weekly" if days == 7 else "monthly"
                if ((root / 'data/ledger.db').exists() and
                        not (output / folder / f'{now.date().isoformat()}.json').exists()):
                    from collect_agent_us_closes import collect as collect_us
                    collect_us(root, now)
                snapshot, size = generate_period(root, crypto_db, output, days, now)
                folder = "weekly" if days == 7 else "monthly"
                allowed.add(f"agent/v1/{folder}/{now.date().isoformat()}.json")
                published.append((snapshot["snapshot_id"], size))
        if not focus_only:
            extensions = write_extensions(root, crypto_db, output, now)
            if extensions is not None:
                allowed.add('agent/v1/market_extensions.json')
                published.append((extensions['snapshot_id'], 'supplemental'))
        if not published:
            return [], git(repo, "rev-parse", "HEAD")
        if not (output / "smoke.json").exists():
            write_smoke(output / "smoke.json", now)
        allowed.add("agent/v1/smoke.json")
        git(repo, "add", "--", *sorted(allowed))
        staged = set(git(repo, "diff", "--cached", "--name-only").splitlines())
        if staged - allowed:
            raise RuntimeError(f"unexpected staged files: {sorted(staged - allowed)}")
        git(repo, "diff", "--cached", "--check")
        if staged:
            git(repo, "-c", "user.name=trend-agent-snapshot",
                "-c", "user.email=trend-agent@localhost",
                "commit", "-m", f"Publish Trend snapshot {now.date().isoformat()}")
        STAGE = 'upload'
        git(repo, "push", "origin", "HEAD:agent-data")
        STAGE = 'verify'
        verify_remote(repo, allowed, now.date().isoformat())
        return published, git(repo, "rev-parse", "HEAD")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--crypto-db", type=Path, default=Path("/root/alpha_news/data/crypto_trend.sqlite"))
    parser.add_argument("--repo", type=Path, required=True, help="isolated agent-data Git checkout")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preview-periods", action="store_true",
                      help="publish today's 7d/30d initial previews only, without daily")
    mode.add_argument("--daily-only", action="store_true", help="publish weekday daily only")
    mode.add_argument("--period-only", action="store_true", help="publish due weekly/monthly only")
    mode.add_argument("--focus-only", action="store_true", help="publish research universe only, without market scans")
    mode.add_argument("--extensions-only", action="store_true", help="publish classified securities and global ETF supplement without freezing daily/period archives")
    args = parser.parse_args()
    from agent_publication_status import receipt, classify_failure, safe_error, alert, recovery
    started = datetime.now(ZoneInfo('Asia/Shanghai'))
    mode_name = ('daily' if args.daily_only else 'period' if args.period_only else
                 'focus' if args.focus_only else 'extensions' if args.extensions_only else 'combined')
    receipt(args.root, mode_name, started, 'running', 'sync')
    import signal
    def terminated(_signum, _frame):
        raise TimeoutError('publication terminated before verification; independent retries remain scheduled')
    signal.signal(signal.SIGTERM, terminated)
    try:
        published, commit = publish(args.root, args.crypto_db, args.repo,
                                    args.preview_periods, args.daily_only, args.period_only, args.focus_only, args.extensions_only)
    except Exception as exc:
        result = receipt(args.root, mode_name, started, 'failed', STAGE,
                         category=classify_failure(exc, STAGE), **safe_error(exc))
        sent = alert(args.root, result)
        print(json.dumps({'operational_alert_sent': sent}), flush=True)
        raise SystemExit(1)
    result = receipt(args.root, mode_name, started, 'verified' if published else 'not_due', STAGE,
                     commit=commit, published=published)
    if published:
        recovery(args.root, result)
    print(f"Published {published}, commit {commit}")


if __name__ == "__main__":
    main()
