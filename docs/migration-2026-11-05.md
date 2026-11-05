# Tencent 2-core / 2-GB migration

New server: Ubuntu 24.04 x86_64, 43.155.210.91, SSH user `ubuntu` with sudo.
Application paths and Python 3.12.12 / 3.8.5 are retained to avoid dependency
upgrades. Private config, data, credentials and server archives are NOT committed.
No Trend selection thresholds, quotas, Telegram templates or frontend files are
changed by this patch.

## Quote-source compatibility

The new assigned IP receives 403 from Sina real-time quotes. The old server still
receives 200. No proxy or regional-block bypass is installed.

- Keep Sina as the first provider; fill missing ordinary stock/index quotes from
  Tencent public quotes. Preserve the provider name, actual timestamp and currency.
- Convert Mainland volume from hands to shares, amount from CNY 10,000 to CNY.
  HK and US volume/amount already use shares/local currency.
- Missing/invalid fields remain unavailable, not fabricated. Regular US quotes
  are never passed off as premarket quotes.
- Relative-strength benchmarks keep the same index and same-date requirement.
- US premarket fallback uses Nasdaq's explicitly labelled Pre-Market primary
  price/volume and previous regular close, with New York session/date validation.
  Yahoo minute bars are a secondary source only when positive, current premarket
  volume actually exists. A source 403/429 stops further requests to that source.
- US market-cap light lookup can use Tencent's USD market cap when Sina fails.

## Runtime / safety

Existing scheduled daily and weekly/monthly exports are retained. Do not loosen
the publication quality checks merely because a staging DB becomes stale. Refresh
the collector before generating previews. Preview files are private, not published.

Use managed services for the previously detached Binance, DEX and Flap workers;
stop old push processes before enabling new ones and copy final dedup/cursor state.
Set OPENBLAS/OMP/MKL threads to 1 and NUMEXPR threads to 2 on this small host.
Retain swap as a peak buffer. Hermes is paused, not removed; keep its configuration.

GitHub snapshots use a NEW repository-scoped deploy key with write access, not a
copy of the old server's general SSH private key. Caddy serves the existing app
over port 80; the previous Caddy configuration is privately backed up for rollback.

## Verification

- 20 new normalization, fallback, benchmark and premarket tests pass.
- Existing Trade_db baseline: 81 tests, 4 pre-existing errors in
  `test_paper_trading_reviewer.py`, reproduced on the old server.
- 47 crypto/security/DEX/Flap tests pass; Binance push regression set passes too.
- Real spot/futures REST depth, OI, funding and current WebSocket streams pass.
- CN/HK/US live quotes, benchmarks, async probes, watchlist/holds and stock/CEX/DEX
  Trend APIs pass on staging. Real AAPL/MSTR/NVDA Nasdaq premarket quotes pass.
- Daily, 7-day and 30-day private previews generate after collector refresh.
- Four authoritative SQLite backups passed quick_check before migration.

An existing external LLM provider reports insufficient quota; moving servers does
not replenish that balance. No replacement provider or paid service is purchased.
