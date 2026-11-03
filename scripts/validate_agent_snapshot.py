"""Compare a public snapshot against at least five read-only source records."""

from __future__ import annotations

import argparse
import json
from contextlib import closing
from pathlib import Path

from generate_agent_snapshot import epoch_iso, readonly_db, read_json


def validate(root, crypto_db, latest):
    data = latest.read_bytes()
    snapshot = json.loads(data)
    day = snapshot["generated_at"][:10]
    history = latest.parent / "history" / f"{day}.json"
    if history.read_bytes() != data:
        raise AssertionError("latest and immutable history differ")
    checks = []
    markets = {m["market"]: m for m in snapshot["overview"]["markets"]}
    with closing(readonly_db(root / "data/ledger.db")) as db:
        for direction in ("stocks_up", "stocks_down"):
            for market in ("CN", "HK", "US"):
                sample = next((s for s in snapshot["anomalies"][direction] if s["market"] == market), None)
                if not sample:
                    continue
                rows = db.execute("SELECT close,name,updated_at FROM trenddailybar WHERE market=? AND date=? AND symbol=?",
                                  (market, markets[market]["archive_date"], sample["symbol"])).fetchall()
                matched = any(float(row["close"]) == sample["price"] and row["name"] == sample["name"]
                              for row in rows)
                checks.append((f"{direction}:{market}:{sample['symbol']} price/name/session", matched and
                               sample["session_date"] == markets[market]["session_date"]))
    with closing(readonly_db(crypto_db)) as db:
        for market in ("spot", "futures"):
            sample = next((c for c in snapshot["anomalies"]["crypto"] if c["market"] == market), None)
            if not sample:
                continue
            row = db.execute("SELECT payload FROM assets WHERE id=?", (market + ":" + sample["symbol"],)).fetchone()
            source = json.loads(row["payload"]) if row else {}
            if epoch_iso(source.get("updated")) == sample["data_asof"]:
                checks.append((f"{market}:{sample['symbol']} price/volume",
                               source.get("price") == sample["price"] and source.get("volume_24h") == sample["volume_24h"]))
    macro = snapshot["overview"]["macro"]
    if macro:
        sample = macro[0]
        raw = read_json(root / "data/macro_dashboard_snapshot.json", {})
        source = next((m for m in raw.get(sample["category"], []) if m.get("id") == sample["id"]), {})
        checks.append((f"macro:{sample['id']} value/asof",
                       source.get("value") == sample["value"] and source.get("asof") == sample["data_asof"]))
    if len(checks) < 5 or not all(result for _, result in checks):
        raise AssertionError(f"source comparison failed: {checks}")
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--crypto-db", type=Path, default=Path("/root/alpha_news/data/crypto_trend.sqlite"))
    parser.add_argument("--latest", type=Path, required=True)
    args = parser.parse_args()
    for label, result in validate(args.root, args.crypto_db, args.latest):
        print(f"{'PASS' if result else 'FAIL'} {label}")


if __name__ == "__main__":
    main()
