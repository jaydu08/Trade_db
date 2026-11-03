"""Contract checks for the public Trend Agent connectivity probe."""

import importlib.util
import json
import mimetypes
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/generate_agent_smoke.py"
SPEC = importlib.util.spec_from_file_location("generate_agent_smoke", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SmokeContractTest(unittest.TestCase):
    def test_payload_is_public_and_timestamped(self):
        when = datetime(2026, 10, 13, 19, 45, tzinfo=timezone(timedelta(hours=8)))
        payload = MODULE.build_smoke(when)
        self.assertEqual(payload["generated_at"], "2026-10-13T19:45:00+08:00")
        self.assertEqual(payload["message"], "trend-agent-link-ok")
        self.assertEqual(payload["markets"], ["CN", "HK", "US", "CRYPTO", "MACRO"])
        self.assertEqual(
            set(payload),
            {"schema_version", "service", "test_type", "message", "generated_at", "status", "markets"},
        )
        with self.assertRaises(ValueError):
            MODULE.build_smoke(datetime(2026, 10, 13))

    def test_write_valid_json_and_replace(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "agent/v1/smoke.json"
            first = datetime(2026, 10, 13, tzinfo=timezone.utc)
            second = datetime(2026, 10, 14, tzinfo=timezone.utc)
            MODULE.write_smoke(output, first)
            MODULE.write_smoke(output, second)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["generated_at"], second.isoformat(timespec="seconds"))
            self.assertEqual(mimetypes.guess_type(output.name)[0], "application/json")
            self.assertEqual(list(output.parent.glob(".smoke-*.json")), [])


if __name__ == "__main__":
    unittest.main()
