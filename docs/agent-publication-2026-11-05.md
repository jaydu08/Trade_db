# Agent publication and market-data repair (2026-11-05)

## Incident and immutability

Old-host journal for Nov 4: 19:12 and 19:18 failed during Git fetch; 19:23
generated a local commit but Git push failed (exit 128). The old wrapper swallowed
stderr, so the exact historical network/authentication sub-cause cannot be proven.
The previously unpushed Nov 4 commit is now on `origin/agent-data`; its history
artifact is readable and its original prices/content are not rewritten. It has no
top-level `archive_date` because it predates this schema addition.

Nov 5's first post-migration scheduled run correctly rejected missing CN turnover.
An Agent-only full-catalog, current-date quote preparation now restores that input;
it does not invoke TG selection or send market signals. The first enhanced run
hit the previous 350 KB transport bound. The bound is now 500 KB to accommodate
lineage and dated context, without relaxing source/readiness checks. The original
365,512-byte daily artifact was remotely verified at **19:26:30 Beijing**, commit
`bdc36ba784495da7dab16941a28eed5758b639c1`. This repair-day run missed 19:25;
it must not be reported as meeting that deadline.

Final live-data inspection found the plain SNDK row had no catalog type, whereas
105.SNDK was STOCK. Resolve non-conflicting verified alias metadata before the
typed identity key; a real-data regression covers this missing-metadata case.
At19:43 the day's stock-only ranking was explicitly replayed from its unchanged
historical A/H scan and US bars (guarded against post-snapshot writes), preserving
crypto/macro and all existing prices. `revision` records the original SHA256/path,
correction timestamp and identity-r1 ID. The original artifact and health are
retained in `agent/v1/revisions/2026-11-05/`, not silently discarded.
Verified correction commit: `9af15074cabbc5bd51a2dd62d9cf5171989da519`.
This is an explicit versioned repair, not a change to normal immutable generation.

## Schedule and recovery

- Timezone Asia/Shanghai; first weekday attempt 19:12, service timeout 600 s.
- Separate, independently enabled retry timers at 19:40 and 19:55; neither depends
  on first-attempt success/failure. Weekly/monthly schedules are unchanged.
- Preserve `agent/v1/history/YYYY-MM-DD.json`, frozen once generated.
- One Git commit/push exposes the batch atomically; use an exclusive publication
  lock. A failed push retains the existing local commit for a subsequent retry.
- Read back JSON with an explicit fetch/show from `origin/agent-data`, verify
  daily `archive_date`, required structures and equality to local committed data.
- Private atomic receipts: `data/agent_publication_status/daily-latest.json` and
  date/attempt files; also machine-readable journal events. Categories distinguish
  generation, upload, permission, source-unavailable and readback failures.
- Failed attempts send a separate operational alert through the existing admin
  destination. No existing market-signal template is modified.
- An ExecStopPost watchdog records/alerts timeouts or abnormal termination that
  cannot reach Python's normal error handler; invocation IDs prevent double alerts.
- Real19:40 independent retry ran automatically and verified at19:40:09, retaining
  the frozen daily blob (before the explicit identity-r1 repair).
- Public paths: `agent/v1/history/2026-11-04.json` and
  `agent/v1/history/2026-11-05.json` on Trade_db **agent-data**, not the code branch.

## Security identity

Agent anomaly rows deduplicate by `(market, canonical_symbol, session_date,
instrument_type)` before median/ranking/quotas. Only known Eastmoney US prefixes
105/106/107 normalize to the same exact ticker; unknown prefixes, share classes,
ADRs vs other instruments, and different listing markets are not company-name
merged. Preserve `raw_symbol`, `raw_symbols`, `source_records`, identity basis and
conflict flags. Latest observation wins, with deterministic tie-breaks; no price
averaging. Version the identity-adjusted coverage to avoid false comparisons to
old duplicate-counted universes. UI and TG selection are unchanged.

## Index and breadth contract

- CN: SSE Composite, Shenzhen Component, ChiNext, CSI300.
- HK: Hang Seng, HSCEI, Hang Seng Tech.
- US: S&P500, Dow Jones Industrial, Nasdaq Composite (not Nasdaq100).
- Index records include explicit identity, market/index publisher, session date,
  observation timezone/time, collection time, source, units and coverage.
- Full Eastmoney market-screen pagination is independent of Trend candidates.
  Validate reported total, uniqueness, all pages and each row's session timestamp.
- Breadth counts only verified same-session issues. Report enumerated/observed
  counts, excluded reasons, exchange breakdown and coverage ratio. These screens
  may include ADRs/funds/preferred issues; **not exchange-certified all-common-stock
  totals**. No extrapolation of missing or inactive issues.
- Meaningful observed coverage is at least half the universe and a bounded
  market-specific minimum (CN/US3000, HK1000, capped by universe size). Incomplete
  coverage is partial with exact scope; too little data gives null headline counts.
- Price/amount/volume and source failures remain null with reasons. Candidate-pool
  counts stay separate. Missing context propagates into source-health flags.
- Independent Tencent/Eastmoney close reconciliation uses dated public references;
  Eastmoney's `100.NDX` is its Nasdaq Composite alias, `100.NDX100` Nasdaq100;
  Hang Seng Tech is `124.HSTECH`. Never infer identity from the abbreviation alone.

## Frontend data freshness

The old global ETF collector treated a successful HTTP request as fresh even when
Sina daily bars stopped on yesterday's session. Compare local completed daily bars
to a dated quote, fall back to Tencent **unadjusted `day`** bars only, and cross-check
latest close. Do not silently use qfq/hfq. Failed checks retain the old record marked
stale. Refresh once after CN/HK/US cash closes even inside the six-hour cache TTL;
stale collections retry at a bounded 15-minute cadence.

Verified all 10 CN ETFs now show Nov 5 in the unchanged frontend. Example: 588000
close1.358, +1.34%, replacing yesterday's1.340/+10.29% displayed as current. Some
30-calendar-day baselines legitimately remain null when the existing five-day
anchor-gap guard intersects a long holiday; this patch does not invent a baseline.

## Model clarification

`alpha_news` uses DashScope `qwen3.8-flash`; a minimal completion returned HTTP200
with nonempty content. The separate Trade_db configuration points to a third-party
Gemini gateway. Its prior quota error is not evidence of exhausted Qwen tokens.
No model routing, credentials or paid infrastructure are changed.

## Verification and changed files

New tests include a real local bare Git remote: first push deliberately fails,
retry pushes the unchanged frozen commit, explicit remote branch/date readback
succeeds. Test permission/error categories, conservative aliases, source lineage,
pagination repeats, quote-date exclusions, unadjusted close parsing, partial scope
and post-close cache refresh. Run all Agent tests, macro and quote regressions.
Final targeted suites: 77 Agent +9 macro +21 quote tests pass (107 total).
All10 index closes match a second public source with the same session date.
Nov4/Nov5 raw GitHub HTTPS paths return200; Nov4's Git blob is unchanged.
The real Nov5 archive has correct archive_date, unique typed security identities
and non-null, explicitly partial observed-universe breadth in all three markets.
No credentials, private configuration, databases or receipts belong in GitHub.

Changed production scripts: `publish_agent_snapshot.py`, `generate_agent_snapshot.py`,
`agent_snapshot_health.py`; new `agent_publication_status.py`,
`agent_security_identity.py`, `collect_agent_market_context.py`,
`repair_agent_identity_archive.py` (all under scripts/).
Adapters: new `modules/ingestion/market_context.py`, updated `tencent_quote.py`.
Collectors: `modules/monitor/macro_dashboard.py`, `market_activity.py`.
Tests: new `test_agent_publication_reliability.py`, `test_agent_market_context.py`;
updated `test_publish_agent_snapshot.py`, `test_generate_agent_snapshot.py`,
`test_agent_research_focus.py`, `test_agent_snapshot_regressions.py`.
Deployment: daily service/timer plus two new retry timers under deploy/; update the
corresponding migration service/timer templates and migration README. This document
is the public-safe implementation/acceptance handoff.
