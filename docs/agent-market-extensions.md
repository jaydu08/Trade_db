# Agent securities and global ETF supplement

Branch: `agent-data`. Read `agent/v1/market_extensions.json` in addition to the
existing daily/weekly/monthly archive. Its generation time can differ from the
frozen report; do not describe live supplemental data as historic report data.

New reports embed `market_extensions`: independently classified Binance securities
spot/perpetual and macro contracts (positive Top5 / negative Bottom5 per group),
plus the full cached global index/ETF universe. The standalone file includes the
full collector-selected securities/macro universe and 12h/168h/720h return windows.
The existing pure-crypto Top150 pool and legacy snapshot fields are unchanged.

Global assets retain all 1d/3d/7d/30d moves, listing markets, currencies and source
dates. 1d means the previous completed available trading session, not intraday;
other windows target calendar days. Exact multi-day baseline dates and unverified
leverage factors remain null, with quality explanations. Binance returns use real
sample time and hourly baselines, never a single-day ranking as a monthly return.
Partial new listings remain visible in the standalone file with null returns.

Normal daily/period publication updates the supplement atomically in the same Git
commit. `--extensions-only` updates it immediately without generating an unfinished
daily report or rewriting frozen historical reports. There are no TG, UI, selection
algorithm, source-database, credential or position/cost exports in this change.
