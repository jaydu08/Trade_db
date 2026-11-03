# Agent export quality corrections — 2026-10-25

Scope: export-only. No Telegram, UI, source-data or Trend selection changes.

* HK 07552/07500 are issuer-verified daily -2x inverse ETF products. Existing
  `LEVERAGED_ETF` enum is retained for compatibility; `product_type=INVERSE_ETF`
  and negative leverage identify direction. Known ETFs do not enter the daily
  ordinary-stock anomaly rank; the two inverse products are also excluded from
  multi-period stock candidates, with reasons retained in excluded candidates.
* Daily US overview and anomaly coverage/items share the existing US EOD archive
  convention: archive date is next UTC ingestion day, so session is archive minus
  one calendar day. Never substitute current Beijing date or delayed write time.
* A/H `market_status` describes the scheduled cash-equity phase in local time:
  pre_open/opening_auction/open/lunch_break/closing_auction/post_close/closed.
  Full-day closures stay `closed` for existing holiday readiness handling.
  HK's random closing auction is conservatively treated as active through 16:10
  (12:10 half-day); this is not a live exchange halt feed. Calendar coverage is
  the existing explicit 2026 exchange calendar, not a guarantee for future years.
* Macro `stale` is collection OR observation stale, including crypto activity.
  Separate `collection_stale` and `observation_stale` prevent an old individual
  indicator from masquerading as current or suppressing otherwise usable daily
  markets. A stale collector or missing macro source still fails readiness.

Frozen history/weekly/monthly archives are not overwritten. These corrections
apply to newly generated exports; previously archived reports retain their
original contents and should not be mistaken for corrected exports.

Identity sources:
- https://www.hkexnews.hk/listedco/listconews/sehk/2025/0829/2025082901858_c.pdf
- https://www1.hkexnews.hk/listedco/listconews/sehk/2025/0430/2025043000857.pdf
- https://www.hkex.com.hk/Global/Exchange/FAQ/Securities-Market?sc_lang=en
- https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml
