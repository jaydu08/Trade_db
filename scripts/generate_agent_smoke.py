"""Generate the public, data-free Trend Agent connectivity probe."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "public/agent/v1/smoke.json"


def build_smoke(now: datetime | None = None) -> dict:
    now = now or datetime.now(ZoneInfo("Asia/Shanghai"))
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("generated_at must include a timezone")
    return {
        "schema_version": "1.0",
        "service": "trend-market-agent",
        "test_type": "connectivity",
        "message": "trend-agent-link-ok",
        "generated_at": now.isoformat(timespec="seconds"),
        "status": "ok",
        "markets": ["CN", "HK", "US", "CRYPTO", "MACRO"],
    }


def write_smoke(output: Path, now: datetime | None = None) -> dict:
    payload = build_smoke(now)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=output.parent,
            prefix=".smoke-", suffix=".json", delete=False,
        ) as temporary:
            temporary_name = temporary.name
            json.dump(payload, temporary, ensure_ascii=False, separators=(",", ":"))
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, output)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    payload = write_smoke(args.output)
    print(f"Wrote {args.output}: {payload['generated_at']}")


if __name__ == "__main__":
    main()
