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


def git(repo, *arguments):
    return subprocess.run(["git", "-C", str(repo), *arguments], check=True,
                          text=True, capture_output=True, timeout=45).stdout.strip()


def publish(root, crypto_db, repo, preview_periods=False):
    if git(repo, "branch", "--show-current") != "agent-data":
        raise RuntimeError("refusing to publish outside isolated agent-data branch")
    lock_path = repo / ".git/trend-agent-publish.lock"
    with lock_path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        git(repo, "fetch", "origin", "agent-data")
        git(repo, "merge", "--ff-only", "FETCH_HEAD")
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        output = repo / "agent/v1"
        allowed = set()
        published = []
        if now.weekday() < 5 and not preview_periods:
            today_archive = output / 'history' / f'{now.date().isoformat()}.json'
            legacy_archive = (today_archive.exists() and
                              not json.loads(today_archive.read_text(encoding='utf-8')).get('market_anomalies'))
            if not today_archive.exists():
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
            if scheduled or preview_periods:
                snapshot, size = generate_period(root, crypto_db, output, days, now)
                folder = "weekly" if days == 7 else "monthly"
                allowed.add(f"agent/v1/{folder}/{now.date().isoformat()}.json")
                published.append((snapshot["snapshot_id"], size))
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
        git(repo, "push", "origin", "HEAD:agent-data")
        return published, git(repo, "rev-parse", "HEAD")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--crypto-db", type=Path, default=Path("/root/alpha_news/data/crypto_trend.sqlite"))
    parser.add_argument("--repo", type=Path, required=True, help="isolated agent-data Git checkout")
    parser.add_argument("--preview-periods", action="store_true",
                        help="publish today's 7d/30d initial previews only, without daily")
    args = parser.parse_args()
    published, commit = publish(args.root, args.crypto_db, args.repo, args.preview_periods)
    print(f"Published {published}, commit {commit}")


if __name__ == "__main__":
    main()
