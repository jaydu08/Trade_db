# 跨市场量化信号第一版（2026-11-24）

## 范围与入口

- 页面：现有 `/macro` 的“市场状态与量化信号”，沿用现有主题、账号认证和刷新流程。没有新增侧栏入口、下单、配仓或 TG 推送。
- 九个市场对象：CYB、CSI300、HSI、HSTECH、BTC/USD、ETH/USD、SPX、NDX、SOX；共66个市场×规则。原六对象46组保持不变，美股增加20组。VHSI 对恒科仅是跨指数参考；RV 不是 VIX；QVIX 是第三方ETF隐波。
- API：原 `/api/macro/dashboard` 增加 `quant`；新增只读 `/api/quant/evidence?market=BTC` 与 `/api/quant/events?market=BTC&limit=200`，复用原认证。未登录仍401，未知市场422。
- 私有数据：`data/quant/quant.sqlite`、`latest.json`、`evidence.json`、`backtest_events.json`、`research_input/`、`raw/<source>/<sha>.bin`、`events/`。迁移时保留整目录，尤其 launched_at、事件、修订和前瞻记录，不能清空后冒充连续前瞻。

## 冻结研究审计

附件 HTML 与包内 report.html SHA256 相同：`2c31f4bcaab041e7a1c6218ca2f2567613ce5e1b27f114039b8daf204b7a0e9f`。

原 `study.py --cutoff 2026-11-20` 已在独立目录复跑。原始响应与 sources.json 所有哈希一致；复跑结果、价格/特征 CSV 与附件逐项对比。生产独立引擎重新计算816条统计、46组主结果和5010条跨取样/期限事件记录（不是5010次独立试验）。四个重点结果保持14/16、10/13、4/5、3/4，所有失败规则一并保留。

版本：`formula_version=quant-1.0.0`，`research_version=frozen-20261120-audit-r1`。修正原 `int(year*.95)` 向下取整问题，使用ceil，并要求完整365自然日跨度及完整252/365根先前窗口；对本次冻结样本的主结果没有影响。恒科20条微小 OHLC 小数精度差使用相对1e-6校验容差，没有改动任何价格。

来源切换与实时更新不会改写冻结研究。历史全样本、历史分段、单事件详情与 prospective 分别标注；不将半年历史胜率解释成明日概率，不合并相关信号概率，不生成综合买入分。

```bash
# 把用户研究包放在私有 data/quant/research_input，先在隔离副本运行原study.py。
OPENBLAS_NUM_THREADS=1 python3 scripts/quant_research_audit.py \
  --root /root/Trade_db --input /root/Trade_db/data/quant/research_input \
  --replay /root/quant-audit-20261124/cross_market_study
python3 scripts/run_quant_monitor.py --root /root/Trade_db --force
python3 -m unittest discover -s tests -p 'test_quant_monitor.py'
```

原研究包、HTML、原始响应及第三方解码源码不提交到公开代码分支。新浪解码调用已安装 AKShare 的解码器；AKShare 使用 MIT 许可证（https://github.com/akfamily/akshare/blob/main/LICENSE）。软件许可不被当作金融数据再分发授权；公开导出限于所需派生研究、状态、事件与来源引用。

## 正常运行与数据限制

复用现有调度器，每30分钟计算一次，采集有持久化 TTL；价格目标交易日变化时提前刷新，IV/恐贪按小时有限重试。运行锁避免重入，网页读缓存不抓源。股票只用完成的交易日，BTC/ETH只用已结束UTC日线。缺口、过期或缺指标为 unknown，不作false；不向前填IV。IV滞后按实际已完成价格交易日数计算，不按自然日：同session就绪、落后1个session待发布、更多则过期；发布时间未知仍单独保留不可判定。

来源成功：新浪 A股指数/恒科/VHSI、Yahoo恒指、Deribit DVOL、Alternative.me恐贪。Yahoo 加密日线实际落后时，自动改用 Coinbase 原生美元日线；必须有至少10个重叠交易日且最大收盘偏离不超过2%，否则报 source_conflict。2%仅为换源工程校验，不是交易信号。原始响应逐次哈希保存；换源、重叠区间、差异和数据版本均留痕，不能静默把 BTCUSDT 当 BTC/USD。

## 美股扩展（2026-11-24）

| 对象 | 价格对象与历史来源 | 波动率与独立检验 |
| --- | --- | --- |
| SPX 标普500 | Yahoo `^GSPC` 价格指数，1990年起 | Cboe VIX；7组既定通用规则 |
| NDX 纳斯达克100 | Yahoo `^NDX` 价格指数，1990年起 | Cboe VXN；7组既定通用规则 |
| SOX 费城半导体 | Yahoo `^SOX` 价格指数，实际覆盖起于1994年 | 自身RV，6组非IV规则；无隐波规则 |

默认增加SPX高VIX下跌、NDX高VXN下跌、SOX高RV下跌三个独立观察项，选择在结果计算前固定，不按历史赢家选规则。高波动定义仍是自身过去一年90分位阈值且20日收益<0。半导体不使用VIX/VXN、SMH或SOXX替代，不共享其他标的胜率。SOX无隐波是not_applicable，不是缺数据，也不拖累整体质量。

美股收盘、感恩节/圣诞半日市及夏令时复用既有 `modules.market_sessions` 已核验2026–2028 NYSE日历；收盘后20分钟才期望当天数据，股票价格收盘与Cboe波指日收盘分开注明。未知日历年份只暂停该市场。早间/北京时间同日报告仍用最近完成的纽约交易日。

官方VIX下载从1990年开始，VXN本次CSV从2009-09-14开始，不能验证2000年互联网泡沫和2008年危机；不从文章、未核验前序波指或零散报价补造。来源：
- Cboe官方历史数据说明：https://www.cboe.com/tradable_products/vix/vix_historical_data
- 官方下载：`https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv`、`VXN_History.csv`，使用历史下载入口，不抓受限报价表。
- VXN对象核对：https://fred.stlouisfed.org/series/VXNCLS （原编制者Cboe、日收盘，FRED并非实时源；本机直接访问超时未用于拼接。）
- SOX对象及价格/全收益版本：https://indexes.nasdaq.com/Index/Overview/SOX 、https://indexes.nasdaq.com/docs/Index_Versions_SOX.pdf
- 纽约日历：https://www.nyse.com/trade/hours-calendars

美股历史截止日同为2026-11-20，价格指数不含分红；21/63/126根持有期、0.2%摩擦、127根主冷却、64根敏感性冷却及同规则可用日期的匹配基准与原研究一致。20组美股主结果、360条期限/分段统计独立计算，包含失败及零完成样本；零样本胜率为null。前2023分段剔除跨边界退出；历史分段不是前瞻检验。实际冻结输入做了前缀无前视检查、独立收益/RV公式复核，原六市场记录逐项保持。

版本分离：原公式仍为quant-1.0.0（原6对象数值公式未变）；新目录版本quant-catalog-1.1.0，不覆盖原catalog_versions/quant-1.0.0.json；美股独立研究us-20261120-r1，聚合研究frozen-20261120-us-extension-r2，旧r1公开文件与已发布日快照保持原样。新美股对象有各自持久化上线时刻，不把上线前回补算前瞻。私有冻结输入在data/quant/us_research_input，旧研究备份在evidence_versions，原始响应仍保留哈希。

```bash
OPENBLAS_NUM_THREADS=1 python3 scripts/quant_us_research.py --root /root/Trade_db
python3 scripts/run_quant_monitor.py --root /root/Trade_db --force
python3 -m unittest discover -s tests -p 'test_quant_*.py'
```

冻结输入首次生成后复用，重复审计结果一致；改变输入必须升研究版本。重跑原六市场审计时用隔离--root，脚本拒绝用旧r1覆盖已安装r2。美股数据独立采集失败时保留null与日期/错误，其他市场正常展示；不新加公共服务、付费源或TG信号。

QVIX HTTPS 当前证书握手失败，未关闭证书校验，未冒用其他波指。最后有效值仍为2026-11-18，页面显示该日期，IV规则不可判定，价格/RV/均线等独立工作。恢复后自动重试，不用旧IV伪造新事件。

股票2026日历复用已核验沪深/HKEX安排；HKEX2027假期与半日市另有显式配置。CN2027官方交易所日历尚未核验，不以工作日假定开市。新年度未核验时只暂停该市场，不遮蔽其他市场；可用私有 `data/quant/calendar_overrides.json` 加入已核验年份：

```json
{"CN":{"2027":{"verified":true,"source_url":"官方交易所公告链接","holidays":["YYYY-MM-DD"],"half_sessions":[]}}}
```

不能把上述示例当成实际2027安排。当前已核验来源：
- SSE：https://www.sse.com.cn/disclosure/dealinstruc/closed/
- HKEX2027：https://www.hkex.com.hk/-/media/HKEX-Market/Services/Circulars-and-Notices/Participant-and-Members-Circulars/SEHK/2026/cc_SEHK_CT_077_2026.pdf
- HKEX半日市：https://www.hkex.com.hk/Services/Trading/Derivatives/Overview/Trading-Calendar-and-Holiday-Schedule?sc_lang=en
- DVOL：https://docs.deribit.com/api-reference/market-data/public-get_volatility_index_data
- Coinbase OHLC：https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles

## 状态、事件与前瞻

特征包含前一日状态和滚动覆盖信息；前20日收盘高点/90分位不含当日，均线含当日。percentile是此前有效值中不高于当前的比例，阈值不是百分位值。跨越缺口只能确认当前state，不能确定首次事件。时间区分 session_date、bar_end_at、available_at、fetched_at；发布时间不可得时 available_at 是首次HTTP观测上界，不声称提前可用。

事件 ID 为 market + signal + session + formula 的哈希，重复任务幂等，实时状态变化不套回测半年冷却。修订导致事件消失时标为 revised_out，保留旧版本。历史回补一律不是prospective；上线时间持久化，只有上线后完成K线的新事件进入前瞻台账。此前已实际观察到的前瞻事件，不因后续供应商撤销而删除评价。

前一交易日曾实际记录为unknown时，补齐数据不能倒推今天是新触发；延续的不确定起点继续保留。只在首次插入、最新完整K线且前态可确定时标记实时事件资格。计算落后时不会用unknown覆盖已有上一交易日观测。上线时刻按公式版本分别持久化，新公式不能复用旧公式的前瞻起点。

纸面入场为下一根完成K线的open，不声称真实成交；初次记录后固定入场价。股票21/63/126根、加密30/90/180根，达到期限才计完成胜率；摩擦0.2%/0.4%。持有区间缺K线标为data_gap，不能靠后续更多K线凑到期。MAE以进入价为基准，非峰值最大回撤。到期结果冻结不被后续数据修订覆盖。前瞻按市场/规则/期限/公式分别统计，不把不同期限合成系统胜率。同源相同值重复抓取保留首次available_at，fetched_at单独更新；修订的新值另记可用时间。

## GitHub发布

正常日报/周报/月报顶层增加 `quant_signals` 摘要，不改变既有研究口径、排名和正式发布时刻。周/月详细量化文件包含本周期所有已记录事件，不是最后一日Top。单市场源缺失或pending不新增为原报告阻断条件；结构/哈希/不可变版本冲突仍拒绝发布，不能生成引用断裂或伪健康报告。

父快照保留原500KB/300KB上限：日报内嵌66个规则的精简状态与7个重点历史证据摘要，周/月报使用更小的市场状态/重点规则/事件计数及引用。预算不足进一步退为引用，完整特征、66组主证据及1176条变体统计在详细文件中，不因接入量化而删减旧榜单。父快照与详细量化导出共同持有运行读锁，避免调度更新穿插造成版本不一致。

路径（分支始终 `agent-data`）：

```text
agent/v1/quant/catalog.json
agent/v1/quant/catalog_versions/quant-catalog-1.1.0.json
agent/v1/quant/history/YYYY-MM-DD.json
agent/v1/quant/weekly/YYYY-MM-DD.json
agent/v1/quant/monthly/YYYY-MM-DD.json
agent/v1/quant/evidence/frozen-20261120-audit-r1.json
agent/v1/quant/evidence/frozen-20261120-us-extension-r2.json
agent/v1/quant/backtest_events/frozen-20261120-audit-r1.json
agent/v1/quant/events/<market>.json
agent/v1/quant/event_versions/<content_sha256>.json
agent/v1/quant/manifest.json
```

历史快照引用不可变证据、catalog和事件版本，不依赖会变的latest。manifest含同一发布提交所需文件、哈希和版本。先写依赖→校验哈希→更新manifest→同一Git提交推送→从origin/agent-data回读。失败重试会重暂存冻结快照的全部依赖，不只上传主报告。已有日期快照不可变。

盘前开发验收使用明确的preview，不提前冻结晚间正式日报：

```bash
python3 scripts/publish_quant_snapshot.py --root /root/Trade_db --repo /root/trend_agent_public
# 不抓源，使用已有缓存和质量状态：加 --offline
```

preview在 `agent/v1/quant/previews/<snapshot_id>.json`，manifest明确指向preview；历史消费者仍使用日/周/月固定日期路径。原工作日19:12发布、19:40/19:55重试，以及周/月早间安排保持不变。

## 部署、验收及回退

变更文件索引：
- `modules/quant/{__init__,engine,calendar,sources,research,us_research,store,runtime,export}.py`：公式、来源、冻结研究、日历、台账与导出。
- `scripts/{quant_research_audit,quant_us_research,run_quant_monitor,publish_quant_snapshot,verify_quant_e2e}.py`：审计、任务、预览发布和实际日/周/月验收。
- `scripts/{generate_agent_snapshot,generate_agent_period_snapshot,publish_agent_snapshot}.py`：复用现有父快照与发布事务。
- `core/scheduler.py`、`interface/api.py`、`modules/monitor/macro_dashboard.py`：调度、认证只读API和看板数据。
- `web/src/components/QuantSignals.vue`、`web/src/views/MacroDashboardView.vue`：页面展示。
- `tests/test_quant_monitor.py`、`tests/test_quant_us.py`、`requirements.txt`、`.gitignore`、本文档：验收、部署依赖、排除私有量化数据与运维说明。

前端 `web/src/components/QuantSignals.vue` 只读后端计算；`web/src/views/MacroDashboardView.vue` 插入组件。以现有锁文件构建，不新增运行时基础设施。部署备份 `/root/migration-20261124-quant/integration-before.tar.gz` 保留旧API、调度、生成脚本、页面和dist，旧静态资源保留兼容已打开页面。

已验证：冻结原脚本复跑、原始哈希和价格/特征CSV一致、无前视、分位与突破边界、95%窗口、缺口恢复、UTC/假日/半日市、事件幂等与撤销留痕、固定入场和到期结果、未到期不计赢、manifest失败恢复/依赖防篡改、真实生产API认证及BTC/CYB市场隔离、桌面/手机真实数据回放、过滤/详情/MAE排序与浏览器零异常。

浏览器回放没有导出服务器认证凭证。生产接口认证测试只在服务器内存中使用短期凭证，不打印、不保存、不改账号。验收未下单，未向原TG行情频道发新信号。

美股扩展额外验收：原六对象816条研究重新独立复跑与旧r1完全一致；美股冻结原始响应哈希校验、实际历史前缀无前视检查、纽约假期/半日市/夏令时、SOX不继承IV、不同指数身份校验、零样本null、旧目录不可变。原有148项回归与50项量化测试合计198项通过，实际生产API验证九对象/66规则、三种美股证据与事件独立，桌面/手机回放零页面异常。扩展部署备份在 `/root/migration-20261124-quant-us/before.tar.gz`。

回退时只恢复备份中对应源文件和原dist、重启Trade_db；保留 `data/quant` 以维持审计和前瞻身份，不删除台账。备份与运行数据不包含在公开agent-data导出中。
