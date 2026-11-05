# New-host deployment files

See `docs/migration-2026-11-05.md` for scope, validation and source compatibility.
These are public-safe templates, not an archive of private production settings.
Never commit `.env`, real YAML credentials, databases, deploy private keys or
private migration reports.

- `systemd/`: existing Trade_db/collector units and export timers, plus managed
  replacements for the three previously detached alpha push workers.
- `small-host.conf`: install in each relevant `.service.d/` as a drop-in.
- `Caddyfile`: port 80 reverse proxy to the unchanged application on port 9800.
- `logrotate.conf`: bounded application stdout/file logs; do not copy old logs.

Retain Asia/Shanghai timezone. Weekday first attempt: 19:12, timeout 600 seconds
(before the 19:25 target). Independently enable the two timers from `deploy/`:
`trend-agent-snapshot-retry-1940.timer` and `trend-agent-snapshot-retry-1955.timer`.
Weekly/monthly eligibility checks remain 09:30/09:40/09:50.
The exporter decides whether the date is Sunday/month-end. Do not replace frozen
archives or publish incomplete staging data simply to test migration.

New snapshots authenticate with a dedicated, write-enabled Trade_db deploy key.
The private key stays on the new host. Source code remains on the independent
`migration/tencent-2gb-20261105` branch until explicitly merged.

Never enable both old and new push workers. Preserve all final cooldowns, Flap
block cursors, candidate queues, stock ledgers, List/Holds and narratives first.
During cutover, verify checksums before starting writers; after writers restart,
DB/state hashes naturally change and should not be compared to the stopped copy.

Hermes installation/configuration is retained; `hermes-gateway` user service is
temporarily stopped/disabled to reserve memory. Restore later with the ubuntu
user command `systemctl --user enable --now hermes-gateway`, only after checking
the combined workload fits this 2-GB host.
