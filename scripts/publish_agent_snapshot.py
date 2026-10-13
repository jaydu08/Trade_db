"""Publish today's frozen public-only snapshot to an isolated Git data branch."""

from __future__ import annotations

import argparse
import fcntl
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from generate_agent_smoke import write_smoke
from generate_agent_snapshot import generate


def git(repo, *arguments):
    return subprocess.run(["git", "-C", str(repo), *arguments], check=True,
                          text=True, capture_output=True, timeout=45).stdout.strip()


def publish(root, crypto_db, repo):
    if git(repo, "branch", "--show-current") != "agent-data":
        raise RuntimeError("refusing to publish outside isolated agent-data branch")
    lock_path = repo / ".git/trend-agent-publish.lock"
    with lock_path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        git(repo, "fetch", "origin", "agent-data")
        git(repo, "merge", "--ff-only", "FETCH_HEAD")
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        output = repo / "agent/v1"
        snapshot, size = generate(root, crypto_db, output, now)
        write_smoke(output / "smoke.json", now)
        history_path = f"agent/v1/history/{now.date().isoformat()}.json"
        allowed = {"agent/v1/latest.json", "agent/v1/smoke.json", history_path}
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
        return snapshot["snapshot_id"], size, git(repo, "rev-parse", "HEAD")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--crypto-db", type=Path, default=Path("/root/alpha_news/data/crypto_trend.sqlite"))
    parser.add_argument("--repo", type=Path, required=True, help="isolated agent-data Git checkout")
    args = parser.parse_args()
    snapshot_id, size, commit = publish(args.root, args.crypto_db, args.repo)
    print(f"Published {snapshot_id}, {size} bytes, commit {commit}")


if __name__ == "__main__":
    main()
