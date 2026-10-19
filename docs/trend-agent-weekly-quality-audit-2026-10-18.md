# 2026-10-18 Agent 7D 快照 P0 审计与隔离回放

## 范围、证据与结论

正式 `agent-data:agent/v1/weekly/2026-10-18.json` 是不可变归档。修复前 Git blob `06c2c62d0f668c3712f4f984226ea6e0b64a57ab`，SHA-256 `f73ef8d2d8ef321cac8fb11cd97846a34d3492ba78ab8a017ead5e0902d1b32a`；本次未覆盖。服务日志确认 10 月 18 日 09:40 发布，commit `195de0f6a1a61fd5380eadd0fa639c09578f5264`。隔离回放以原始 `generated_at=2026-10-18T09:30:03+08:00` 为截止时间，只从当前 SQLite 中选当时已写入的股票/商品归档行；Crypto 与宏观缓存没有完整 point-in-time 历史，因此逐项沿用原归档，不把 10 月 19 日的现值伪装成 10 月 18 日数据。回放不是替换正式周报。

根因在 `generate_agent_period_snapshot.py` 的原 `stock_period()`：每只标的用自己的最新收盘作 7D 终点，最多允许落后市场 4 日历日；随后用混入旧窗口的候选池计算中位数及相对强弱。原 `_baseline()` 对 7D 允许向前找 4 天，未判断期间市场是否实际开盘。`amount_latest` 直接输出库中 0，`amount_change_pct` 却跳过零值，形成自相矛盾。顶层 `stale` 只汇总 source_health，遗漏内部股票和宏观项。`classify_stock_asset()` 未识别香港简称 `XL二...`，且 `meta.asset` 错标 `STOCK`。

## 11 条旧行情逐项核查

本表中的“最后交易日”是**从归档日期推断的有效收盘所属日**，不是供应商明确提供的交易所时间戳。US 入库日期为 UTC 归档日，常在纽约交易日次日 00:12 UTC 写入；`updated_at` 是数据库写入时间，不能当成交时间。所有 11 条在市场基准 10 月 16 日均无数据库有效收盘行，也无当日其它来源的该标的行。`meta.asset.listing_status` 全部为 `ACTIVE`，这并不能证明没有停牌；当前证据不足以在“源头未抓到、候选池退出、实际停牌、代码映射”之间进一步定性。因此统一标 `lagged`、保留最后真实日期和来源，而**不宣称全部是采集故障**。

| 市场 | 标的 | 最后归档日 → 推断交易日 | 来源 | `updated_at` UTC-naive | 10/16 数据库收盘 |
|---|---|---|---|---|---|
| CN | 001389 | 10/12 → 10/12 | trend_pool_refresh_eod | 10/12 11:10:00 | 无 |
| CN | 300981 | 10/14 → 10/14 | daily_hot | 10/14 09:36:27 | 无 |
| CN | 301080 | 10/14 → 10/14 | trend_pool_refresh_eod | 10/14 11:10:00 | 无 |
| CN | 600707 | 10/14 → 10/14 | trend_pool_refresh_eod | 10/14 11:10:00 | 无 |
| CN | 601579 | 10/15 → 10/15 | trend_pool_refresh_eod | 10/15 11:10:00 | 无 |
| HK | 01142 | 10/13 → 10/13 | trend_pool_history_backfill | 10/14 11:10:33 | 无 |
| US | 105.LITE | 10/15 → 10/14 | trend_pool_refresh_eod | 10/15 00:12:29 | 10/17 归档无 |
| US | 105.PCRX | 10/14 → 10/13 | trend_pool_refresh_eod | 10/14 00:12:28 | 10/17 归档无 |
| US | 106.BH | 10/16 → 10/15 | trend_pool_refresh_eod | 10/16 00:12:28 | 10/17 归档无 |
| US | 106.OKLO | 10/14 → 10/13 | trend_pool_refresh_eod | 10/14 00:12:28 | 10/17 归档无 |
| US | CBRS | 10/13 → 10/12 | trend_pool_refresh_eod | 10/13 00:12:26 | 10/17 归档无 |

核查 SQL（`data/ledger.db`；此处筛选保持原归档截止）：

```sql
SELECT b.market,b.symbol,b.date,b.source,b.updated_at,b.close,b.amount
FROM trenddailybar b JOIN (
  SELECT market,symbol,MAX(date) AS d FROM trenddailybar
  WHERE date<='2026-10-18' AND symbol IN
  ('105.PCRX','106.BH','CBRS','105.LITE','106.OKLO','01142',
   '300981','601579','301080','600707','001389')
  GROUP BY market,symbol
) x ON b.market=x.market AND b.symbol=x.symbol AND b.date=x.d;
```

`trendseedpool` 对这些代码没有可用的保留记录，因此不能仅凭该表判断缺口原因。新榜以市场最新**已观测**归档会话为统一终点；落后的行只进入有上限的 `source_health.*.excluded_candidates` 诊断，正式榜不借旧价。`session_gap` 按市场归档中观察到的交易会话数计，而非日历日；此会话日历不是完整官方交易所日历，若全市场采集缺日，仍可能无法识别，属于剩余风险。当前样本 10/16 覆盖充分，结论可核验。

## 资产主数据与成交额

`data/meta.db` 的 07709、07747、09747 均为 `STOCK`、`ACTIVE`、`extra=null`。原分类依赖英文 ETF/杠杆关键词和少数美股代码，香港短名未命中。现以港交所 [07709 基金资料](https://ifp.hkex.com.hk/fund-repository/zh-HK/fund/BWT200)、[07747/09747 基金资料](https://ifp.hkex.com.hk/fund-repository/zh-HK/fund/BVZ635) 的产品 ID 覆盖该错误默认类型：07709/07747 为每日正向最多 2 倍产品，标的是 SK hynix `000660.KS` / Samsung Electronics `005930.KS`。港交所 [多柜台通告](https://www.hkex.com.hk/-/media/HKEX-Market/Services/Circulars-and-Notices/Participant-and-Members-Circulars/HKSCC/2025/ce_HKSCC_SKA_138_2025.pdf) 证实 07747 为 HKD、09747 为 USD 柜台；[交易安排](https://www.hkex.com.hk/eng/market/sec_tradinfo/tradarng/tradarng_news/premonth/news.htm) 显示 09747 美元柜台于 2026-09-18 终止，不再当作活跃可交易柜台。ETF 基础货币与交易柜台货币不同，代码字段只表示交易货币。07747 因缺少可比 10/09 基准收盘，被移出正式 7D 榜，仍在排除诊断显示其正确类型。

原周报 48 股票有 34 条 `amount_latest=0`：15 US、12 CN、7 HK；9 商品亦全为零。34 股票零值中 33 条来自 `trend_pool_refresh_eod`，1 条为 `trend_pool_history_backfill`；商品 8 条来自前者、1 条来自 `commodity`。SQLite 的 `amount` 列 `NOT NULL`，刷新源该周连续批量写入 0，未保存“已核实真实零成交”标志。新导出将这类零值解释为 **missing/null**，并附 `amount_status`、来源、币种、时间依据与缺失原因；只有有独立确认的零成交输入才允许输出 `verified_zero` 和 0。新变化率必须有最新有效金额、同来源且前后半段各至少两条金额样本；否则为 null。由于商品金额单位/来源未证实，商品金额变化率维持 null。

原来 6 条矛盾记录 BSP、105.LQDA、106.BBWI、605058、000823、002990 的最后一条都是 `trend_pool_refresh_eod` 的 0；此前的正金额分散在 `heatmap`、`daily_hot`、`trend_pool_history_backfill`。旧代码把这些非零历史金额跨来源分段平均，分别得到 +190.47%、+596.51%、-4.86%、+39.53%、-2.00%、+107.33%，却同时报告最新 0。它们不代表“当前日放量”，新回放中只要入榜，这两字段均为 null，不保留不可核对的放量结论。证据查询：

```sql
SELECT market,symbol,source,COUNT(*) n,
       SUM(CASE WHEN amount>0 THEN 1 ELSE 0 END) positive_amount,
       MIN(CASE WHEN amount>0 THEN date END) first_positive,
       MAX(CASE WHEN amount>0 THEN date END) last_positive
FROM trenddailybar
WHERE symbol IN ('BSP','105.LQDA','106.BBWI','605058','000823','002990')
  AND date BETWEEN '2026-10-01' AND '2026-10-17'
GROUP BY market,symbol,source;
```

## 时间窗口、过期传播与合同

原 07747 的目标 10/09、真实基准 10/06、终点 10/16：10/07、10/08、10/09 在香港市场有其它证券归档，不能当作普通休市补足。新 `baseline_quality=missing_asset_bars`，`exclusion_reason` 说明跳过了 3 个已观测市场会话；周末/已知无会话日仍可前移。正式榜先过滤 `eligibility_status=eligible`，再重算同市场中位数、相对强弱和 Top 排名。`window_start_target` 统一按市场终点，另标 `actual_baseline_date`、`baseline_gap_days`、`window_basis`、`archive_date`。美股 `archive_date=10/17`、`asset_session_date=10/16` 是正常映射，不以归档日期直接删掉。

宏观缓存的检查时点新鲜，并不使其旧观测变新鲜。原 DGS2、DGS10、DFII10 `data_asof=10/15`、期望 10/16，三项保持 `stale=true`；新 `source_health.macro` 区分 `collection_freshness=current` 与 `observation_freshness=stale`，顶层 `stale=true`、`stale_item_count=3`、`stale_item_ids` 指明三项。股票/Crypto 各自健康状态不被连坐。旧 CN 001389 已从正式榜排除，因此不计“有效报告项”的 stale 个数，但诊断保留其缺口。

商品期货 10/17（周六）为数据库**归档日**，并未保存可验证的交易所交易日；回放将 CF `session_date` / `market_reference_session` 明确置 null，另给 `archive_reference_date=10/17`，不把周六说成商品交易日。CF 的 7D/30D 仍是归档日口径、质量 partial，未来需接交易所会话数据才能升格为交易日口径。

## 验收结果与前后对照

| 指标 | 原归档 | 隔离回放 |
|---|---:|---:|
| 股票/商品正式榜总记录 | 57 | 57（巧合；36 保留、21 移出、21 递补，不固定配额补旧价） |
| 指定 11 条旧股票仍在正式榜 | 11 | 0；11 条均有排除诊断 |
| 07709 / 07747 类型 | STOCK / STOCK | 07709 正式榜 LEVERAGED_ETF；07747 排除诊断 LEVERAGED_ETF |
| 股票最新金额 0 | 34 | 0；新榜 36 条 null，数目因名单变化 |
| 最新金额缺失但变化率非空 | 原 6 条零值矛盾 | 0 |
| 宏观过期指标 | 3，顶层 false | 3，顶层 true |
| 周报字节数 | 104502 | 197410 |

本次新增/覆盖的单测 31 项，另有原 Agent 数据质量测试 5 项、其余 Agent 生成器/冒烟/发布测试 9 项：生产仓库测试 **45/45 通过**。T01—T05 覆盖旧交易日、美股归档、停牌与缺源区分、重算相对强弱、统一终点；T06—T09 覆盖香港产品与多柜台及美股杠杆回归；T10—T14 覆盖缺失/可证实零金额、变化率可比性与商品零值；T15—T19 覆盖股票/宏观过期传播及领域隔离；T20—T21 覆盖 07747 基准缺口和正常休市；T22—T24 覆盖日快照兼容、30D 复用、不可变归档；T25 包括隔离回放 JSON Schema、语义校验和无 NaN/Infinity。额外加入商品归档日期不误标交易日、已终止柜台不可排名回归。30D 生产输入独立预览 `2026-10-19`：185985 字节、股票/商品 60、Crypto 30，通过同一校验，**不是月底正式发布**。最初月度预览因排除诊断过大达到 203647 字节，压缩月度诊断上限后通过；正式排名规则未变。最初 CF 空会话聚合在隔离回放触发 `TypeError`，增加回归并修复后通过；这两次失败均未发布。

验证命令：

```bash
python3 -B -m unittest discover -s tests -p 'test_generate_agent*.py' -q
python3 -B -m unittest discover -s tests -p 'test_agent_data_quality.py' -q
python3 -B -m unittest discover -s tests -p 'test_publish_agent_snapshot.py' -q
python3 -B scripts/validate_agent_period_quality.py data/agent_replay/agent/v1/replay/weekly-2026-10-18-quality-v2.json
python3 -B scripts/validate_agent_period_quality.py data/agent_preview/agent/v1/monthly/2026-10-19.json
```

## 文件、发布边界与回滚

改动仅限 `scripts/generate_agent_period_snapshot.py`、`scripts/generate_agent_snapshot.py`、新增 `scripts/replay_agent_weekly_quality.py`、`scripts/validate_agent_period_quality.py`、`tests/test_generate_agent_period_snapshot.py` 与本文件。无 `/trend` 前端、Trend 算法、TG 推送、调度器和端口变更。新增字段不删除旧字段；`amount_latest` 的零值语义改为“未证实则 null”，正式榜资产集合会变化，调用方不得依赖固定 Top 数量。日快照只扩展验证过的香港产品分类/元数据，原 `agent/v1/history/` 结构和发布触发不变。

隔离回放目标路径是 `agent/v1/replay/weekly-2026-10-18-quality-v2.json`，不是 `weekly/2026-10-18.json`。代码可通过还原对应提交回滚；历史正式 JSON 不需也不得回滚覆盖。若新发布质量异常，应暂停未来的 `trend-agent-period-snapshot.timer`、保留现有归档，修复并先在隔离路径验证，再恢复定时任务；不要手工改旧归档。`agent-data` 发布及 GitHub 回读另记在交付说明，生成成功与推送成功分别验收。

剩余缺口：没有逐证券供应商成交时间、停牌/退市权威事件、全市场官方交易日历、商品交易所会话映射、成交额零值确认标志、跨来源成交额单位契约、Crypto/宏观的 point-in-time 输入版本。代码选择保守 null/排除/partial，不为这些缺口补造数值。
