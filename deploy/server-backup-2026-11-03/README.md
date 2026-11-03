# 2026-11-03 live-server migration backup

This branch is an isolated, sanitized **full source snapshot**, not a replacement
for main/master. It includes dirty and untracked application code at capture time.
Production branches, Git staging, running processes and Telegram settings were not
changed. Source provenance and exclusions are in `source-manifest.json` beside this
document. `REDACTED_MIGRATION_SECRET` means the original is in the private offline
backup, NOT that the production value was changed.

## Workloads and versions observed

| Workload | Runtime | Launch method on old host |
| --- | --- | --- |
| Trade_db API/UI + stock/TG schedulers | Python 3.12.12 | trade_db.service; TCP 9800 |
| Binance/DEX Trend collector | Python 3.8.5 | crypto-trend.service |
| Binance signals | Python 3.8.5 | `python -m bn_monitor_v2.main --push --settings config/bn_monitor_v2.yaml`, detached |
| DEX alerts | Python 3.8.5 | `python tools/dex_monitor.py`, detached |
| Flap/Four BNCB alerts | Python 3.8.5 | `python tools/flap_bncb_monitor.py`, detached |
| Agent exports | Python 3.12.12 | two oneshot services + timers |
| poly-sdk | Node/TypeScript | repository present; no running SDK daemon found |

Old host: x86_64, 2 vCPU, ~4GB RAM, 60GB system disk (~35GB used), OpenCloudOS.
Do not blindly downsize to 1GB RAM or an IPv6-only machine. Recommended migration
floor: x86_64 2 vCPU / 4GB RAM / 60–80GB SSD / public IPv4. 40GB requires reviewing
disk use first; 35GB disk is insufficient for a literal restore of the whole OS.

There is also a long-running, stdin-launched Python helper under Trade_db whose
entry source was not identifiable from `/proc`. It exposed no listening TCP port
at inventory time and is NOT reproduced by these units. No BT panel, Tencent
agents, SSH private keys or full OS image are included or needed by these apps.

## Restore order — do not start push workers automatically

1. Provision a supported Linux x86_64 host; configure Asia/Shanghai timezone, NTP,
   SSH firewall and updates. Run `probe_endpoints.py` on the actual assigned IP.
   A non-US country label is NOT a guarantee of Binance eligibility or API access.
   Check permitted use; do not use proxies to bypass restrictions. Reject a host
   if required public endpoints return 451; distinguish 403/WAF and 429 limits.
2. Clone the backup branch of each repository to `/root/Trade_db`,
   `/root/alpha_news`, `/root/poly-sdk`. Clone `Trade_db`'s **agent-data branch**
   separately to `/root/trend_agent_public`, not to an app checkout.
3. Restore PRIVATE config and data from the offline backup. Source files under
   `private/source/<repo>` preserve unsanitized originals when required. Runtime
   directories are under `private/runtime/<repo>/data`. Keep permissions private.
   Do not commit `.env`, actual YAML config, databases, user lists or Git bundles.
   GitHub alone is insufficient to restore keys, history, holdings and cooldowns.
4. Recreate isolated Python runtimes. `pip-freeze.txt` records old installed
   packages, not a promise those builds work on a new OS. Match the two versions
   first; Python 3.8 is legacy/EOL and may need compatible OpenSSL/build tooling.
   Upgrade it only as a separately tested migration. Check `.env.example` and
   private configs, especially TG credentials, allowed IDs, LLM endpoints, RPC
   providers, website users, news providers and account mappings.
5. Install frontend dependencies from the lockfile, run the existing web build;
   Node v20.19.6 existed on the old host. poly-sdk is optional unless used. Run
   available tests without triggering TG messages. Online SQLite copies have
   passed `PRAGMA quick_check`; validate row counts and monitor checkpoint times.
6. `systemd-observed/` contains exact existing app units and timers. The optional
   `systemd-proposed/` units convert the three detached workers into managed
   services; they were NOT installed on the old host. Review paths and configs
   before copying selected units to `/etc/systemd/system` and daemon-reload.
7. Restore GitHub authentication on the new machine using an appropriately scoped
   deploy key or token; credentials are deliberately NOT in this public branch.
   Permit writes to `agent-data`. Set `Asia/Shanghai` before enabling timers:
   daily retries 19:12/19:18/19:23; period checks 09:30/09:40/09:50. Period script
   itself checks Sunday / month end. Preserve frozen archives and List/Holds.
8. Test API/UI and data collection with TG push workers disabled. Restrict TCP9800
   to intended clients or place it behind a secured reverse proxy; do not copy
   the old BT panel's open port 8888. Reconfigure DNS/bookmarks/IP allowlists.
9. At agreed cutover: stop OLD push workers first, take a FINAL incremental backup
   of DBs + JSON checkpoints, restore on new host, then start NEW workers once.
   Never run two polling TG bots or two notification workers concurrently.
   Check pending candidate queues, block cursor, dedup/cooldowns and emitted TG
   counts. Do not leave old host expiring before new exports and alerts validate.

## Private backup limits

SQLite databases are individually consistent online backups; this is not a single
global transaction across all files. JSON is copied over the capture interval.
The locked, rebuildable `Trade_db/data/vector_store` Chroma embeddings index was
excluded rather than claiming a raw live copy was consistent; rebuild it if needed.
Authoritative `ledger.db`, `meta.db`, crypto trend history, narratives, watchlists,
Holds and monitor dedup/checkpoint JSON are captured separately. Final cutover
still requires copying changes since this backup; this snapshot is not live sync.

## Validation evidence

Source and deployment files were allowlisted, scrubbed for known config secrets
and common credential patterns, and checked again before push. No automatic
cloud purchase, service shutdown, application upgrade or TG test send is part of
this backup. Remote branch hashes and local archive checksum are recorded in the
handoff, not in production configuration.
