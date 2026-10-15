# Trend Agent 数据质量 v2：2026-10-13/14 审计与部署

范围：仅 `agent/v1` GitHub 导出。未更改 `/trend` 前端、Trend 选股、TG 推送或 `alpha_news` Worker。正式 `history/2026-10-13.json`、`history/2026-10-14.json` 为不可变归档；回放不得替换它们。

## 根因：样本不是同一时刻、同一用途的池

直接读取两份已发布 JSON：

| 发布日/北京时间 | 市场 | 旧 `candidate_universe` | 旧上涨/下跌 | 上涨主要来源 | 下跌主要来源 |
|---|---|---:|---:|---|---|
| 10-13 20:58 | CN | 135 | 10/10 | TG daily selected board | `trend_pool_refresh_eod` |
| 10-13 20:58 | HK | 48 | 10/10 | TG + `trend_pool_refresh_eod` | `trend_pool_refresh_eod` |
| 10-13 20:58 | US | 105 | 10/10 | heatmap | `trend_pool_refresh_eod` |
| 10-14 18:30 | CN | 50 | 10/0 | TG daily selected board | 无 |
| 10-14 18:30 | HK | 3 | 3/0 | TG daily selected board | 无 |
| 10-14 18:30 | US | 105 | 10/10 | heatmap/daily_hot | `trend_pool_refresh_eod` |

SQL 证据：`SELECT market,date,source,COUNT(DISTINCT symbol),MIN(updated_at),MAX(updated_at) FROM trenddailybar WHERE date IN ('2026-10-13','2026-10-14') GROUP BY market,date,source`；并以 `updated_at <= 发布时刻 UTC` 复核。10-14 CN/HK 早于 18:30 的记录主要是 `daily_hot`/`heatmap`（CN 40+10，HK 3）；更广的 `trend_pool_refresh_eod` 于 19:10 北京时间写入（CN 88、HK 45），晚于当日 Agent 发布。调度器原本在 19:10/21:10 跑这个刷新，不可能供 18:30 快照使用。US 的广池已在当日 08:12 左右写入。现在数据库回看 10-14 已是 CN 138、HK 48、US 105 个不同日线代码；10-13 的历史日线也有事后回填，不能把“当前库的历史日期记录数”伪称为当时已发布覆盖数。

原导出器 `stock_snapshot()` 以快照时刻的 `TrendDailyBar` 当日有效代码数写入 `candidate_universe`；TG 日榜仅用于优先排序和个别 1 日回报覆盖，没有阻断日线池查询。按两个真实发布时间的 `updated_at` 截止，只读 SQL 重算当时有真实相邻日线（前价距目标日不超过 4 天）的收益符号，顺序为上/下/平/无近邻：10-13 CN 48/54/1/32、HK 16/32/0/0、US 28/55/13/9；10-14 CN 12/0/0/38、HK 1/0/0/2、US 40/50/7/8。CN 在 10-14 有 15 只“当前价低于库内上次旧价”，但旧价大多停留在数周或数月前，**不能拿来冒充当日跌幅**。因此广池 19:10 才到和早到样本缺相邻历史，合起来造成 18:30 的空下跌榜。真正问题是把这个随采样时间变化的**精选/日线候选池**当成可代表市场涨跌的扫描池，并且字段名没有说明采样范围。不是 A/H 市场没有下跌，也不是 TG 代码主动过滤下跌。

10-14 宏观缓存的采集检查时间很新，但 FRED 图表源仍给出 DGS2/DGS10/DFII10 的 10-09 观测值。10-12 为美国假期；至北京时间 10-14 18:30，10-13 的 H.15 常规发布窗口已过。原逻辑仅按观测日期距离当前日期是否超过 7 天判断，故误标 `quality=ok, stale=false`。新的 H.15 专项规则按美国工作日及 16:45 纽约时间宽限判断观测日；三项改标 `stale`，并记录预期日与真实日。美元广义指数等未核实完整发布日历的序列标 `unknown/partial`，不假装新鲜。该修复是导出标签修复，未修改原采集器。

## 数据流与契约

`TrendDailyBar + TG 日榜 → 原 Trend Selection → trend_selected / overview`（不改变原算法和页面）；另一路 `120 日 TrendSeedPool ∪ 当日归档代码 → CN/HK 独立可信报价源收盘扫描；US 已归档日线 + 前日真实收盘 → market_anomalies`。CN/HK 必须是当天收盘后的带时间戳报价；失效、盘中、异日、无涨跌幅的样本均排除。US 缺真实前日价不计算 1 日收益。每个市场按真实 1 日收益分别输出最多 10 个涨、10 个跌，以及相对**本扫描样本中位数**的强/弱各 3 个；这不是指数超额收益，也不是统计 Z-score。成交额、历史基准、复权口径不可靠时保留 `null`。

兼容字段 `overview`、`anomalies.stocks_up/down`、`latest.json`、`history/YYYY-MM-DD.json` 保留；新增 `trend_selected`、`market_anomalies` 与 `health/YYYY-MM-DD.json`。新 `anomalies.stocks_up/down` 指向独立扫描，旧精选详情留在 `trend_selected`/`overview`。`candidate_universe` 保留但明确定义为发布时刻的有效归档代码数，不等于全市场。新覆盖字段分别列出来源池、可用、合格、入选、排除原因和来源贡献；只有同口径来源/合格样本才计算覆盖率。连续 5 个同版本同范围的历史报告供相对变化检查；超过 30% 只作复核提示，不自动判定采集故障。空下跌结果区分 `none_observed` 与 `insufficient`。

时间分开记录 `session_date`、`observed_at`、`source_checked_at`、`snapshot_generated_at`。美股原始供应商代码保留在 `raw_symbol`/`asset_id`，三位数字点前缀仅生成供展示的 `canonical_symbol`，不用于去重。ETF/杠杆 ETF 按现有资产元数据及明确代码/名称识别，无法验证交易所、标的和杠杆倍数时为 `null`。A/H 独立报价标 `eod_vendor_close`，美股历史价标 `archived_latest`，不冒称交易所官方收盘或已复权。CEX 现货/永续、链上 chain/contract/pool 仍用原导出器口径，不改交易监控；缺少的 OI/价格调整/安全事实继续为 `null`。无新增付费服务或公网端口；复用已安装的 Trade_db 报价能力。

## 只读回放与质量结果

10-14 回放于当日约 21:04 北京时间执行，独立报价扫描输出写入 `/tmp/trend-agent-v2-staging`，未写正式归档。它不是声称在 18:30 已获得这些数据；18:10/18:30 的次日生产调度仍需实测。回放结果：

| 市场 | 来源池/可用/合格 | 旧上涨/下跌 | 新独立上涨/下跌 | 新下跌来源/例子 |
|---|---:|---:|---:|---|
| CN | 138/137/137 | 10/0 | 10/10 | AkShare 可信报价；301310 -12.98%、002487 -10.00% |
| HK | 50/47/47 | 3/0 | 10/10 | AkShare 可信报价；08412 -9.375%、02670 -9.171% |
| US | 105/105/97 | 10/10 | 10/10 | `TrendDailyBar:trend_pool_refresh_eod`；8 只缺可比前日价 |

CN 1 只无报价，HK 3 只被识别为未达收盘时间的报价，未补造。新输出在可用报价中分别找到 CN 69、HK 32 个负收益样本。FRED 三项利率由旧 `ok/false` 改为 `stale/true`，预期 H.15 观测日 10-13，实际 10-09。10-13 正式快照已有 CN/HK/US 各 10 个负向候选；由于当日没有保存完整的独立报价扫描历史，无法诚信地重造 10-13 新版逐只榜单，故只做正式归档/SQL 源时刻复核，不声称有历史行情重放。

隔离回放实际发布在 GitHub `agent-data` 分支：`agent/v1/replay/2026-10-14-v2-readonly.json` 和 `agent/v1/replay/2026-10-14-v2-readonly-health.json`（提交 `8d25a74`）。二者仅用于验证新契约，不能替代 18:30 正式归档。正式 10-13/14 `history/` 文件 SHA-256 仍分别为 `26335178817050325dc7f4e93941c362f9113089425c79f3852ec229b73d0a21`、`98d46d02fde2f4d614b909904bd6de5bc1391df659abdd8bc7920117cfbc97cc`。

## 验收、调度与回滚

本地 Agent 专项单测覆盖正常涨跌、TG 三只港股仍可独立扫描、报价部分失败及发布门禁、零下跌、假期与新采集旧观测、不同交易日、ETF 类型、同日不可变归档、周/月契约。生产只读回放：10-14 生成 JSON 约 212 KB（220 KB 上限），health 约 3.3 KB；三项陈旧指标使 health 为 `partial`，其余独立市场覆盖足够。正式 10-13/14 历史文件不覆盖。

自 2026-10-16 起，工作日日快照在北京时间 19:25 首次尝试、19:40/19:55 重试，等待 19:10 的 A/H 趋势池日线补齐；发布门禁还要求当天 CN/HK 均有 `trend_pool_refresh_eod` 归档记录，不因时间到了就把小榜当完整数据。周/月快照使用独立 period-only service/timer，在 18:30/18:40 保留原发布窗口。扫描及发布在现有 GitHub `agent-data` 独立 checkout 内原子写档、幂等提交。若独立扫描覆盖不足、当天 A/H 日线/成交总额未就绪、加密源过期，不发布伪完整日档；健康报告只有成功冻结后随同发布。`weekly/`、`monthly/` 继续沿用现有 7/30 日独立导出，不从日榜拼装。周/月本次未改算法。

部署当晚定时器曾因新增 18:10 条目立即补触发，遇到 10-14 已存在的旧 schema 档并由新版门禁拒绝；未覆盖或推送任何数据。发布器现对当日既有旧版不可变档幂等跳过，手动重试输出 `Published []`，不会为旧版档伪造新版健康报告；10-15 起才首次创建 v2 正式日档。

10-15 首次 v2 定时发布暴露了扫描前预取 `now` 的时序 bug：报价检查时间晚于旧 `now`，被健康审计误报为未来时间。提交 `54326d7` 将快照时间改在扫描结束后取得，13 项专项测试通过，10-15 正式日档于 20:42 补发（`agent-data` 提交 `bb8decb`）；并未赶上原 19:00 日报。此事促使日快照从原先的 18 点时段移到 A/H 趋势池补齐后的 19:25，周/月发布独立保留原窗口。

部署后验证：检查两个 `trend-agent-*.timer` 的下次触发和各自 service 日志；下一个工作日在 `agent-data` 分支读取 `agent/v1/history/YYYY-MM-DD.json` 与 `agent/v1/health/YYYY-MM-DD.json`，核对同一 `snapshot_id`、正负覆盖和 FRED 质量。日快照应以**下一实际交易日**的 GitHub 文件时间和日志确认能否在 20:20 日报前发布；若 19:55 重试后仍未就绪，应报告未发布，不得伪装成功。周/月仍以原 18:45 前发布窗口验收。

快速回滚：在 Trade_db `main` 对本次代码提交执行 `git revert <commit>`（不动数据目录）；禁用新增 `trend-agent-period-snapshot.timer`，并将原 `trend-agent-snapshot.service/timer` 恢复至上一版本，然后 `systemctl daemon-reload` 并重启原 timer。已冻结 GitHub 历史档保持不变；不要重写 `agent-data` 历史。`/root/alpha_news` 无本次修改，不需重启。

尚未解决：真正全市场广度、20 日波动率 Z-score、行业/指数基准超额收益、复权口径、美元指数精确发布日历、官方收盘价验证；这些需要额外可信历史源与单独验收，不为 P0 人为补值。回放时 A/H 可信报价覆盖是候选观察池而非全市场，GPT 不应将其上涨/下跌计数推断为全市场涨跌家数。
