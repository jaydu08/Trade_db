# Trend Agent 多周期静态快照

发布目标仍是 GitHub `jaydu08/Trade_db` 的 `agent-data` 分支；没有新 HTTP 服务、登录或付费设施。读取时必须指定 `ref=agent-data` 和下列完整路径：

- 工作日：`agent/v1/history/YYYY-MM-DD.json`（`agent/v1/latest.json` 是便利指针，不作为历史事实）
- 周日：`agent/v1/weekly/YYYY-MM-DD.json`
- 每月最后一个自然日：`agent/v1/monthly/YYYY-MM-DD.json`

服务器将日快照与周/月快照分开调度。`trend-agent-snapshot.timer` 在工作日北京时间 19:25、19:40、19:55 尝试日快照，等待 19:10 的 A/H 趋势池日线补齐；`trend-agent-period-snapshot.timer` 每天 18:30、18:40 检查是否为周日或月末，仅在到期时发布 7/30 日快照。工作日只有 A/H 当日收盘归档和成交额、当日 `trend_pool_refresh_eod` 记录、最近完整纽约交易日、三类新鲜加密采样和宏观缓存都齐备时才更新日快照；缺上游数据时退出失败，保留已有文件，不伪称完整。日期文件使用原子且不可覆盖写入；重试幂等。首次部署在非周日/月末生成的周/月文件带 `run_kind=initial_preview`，不能解释为当期正式定时报告。

7 日、30 日均为**自然日锚定**，不是 7/30 个交易日。股票/商品复用 `/api/trend?days=7/30` 所用 TrendCalculator 分数与 TrendDailyBar 区间价差口径：选择目标日或更早的最近有效收盘，允许有限稀疏基准延迟，逐日排除疑似拆股跳价。现有 `/api/trend` 只输出上涨候选；快照从同一历史日线池另算下跌侧，不改其算法或前端。月榜使用完整 30 自然日价格序列，不使用月底单日涨幅榜。加密使用 `alpha_news` 同一来源/交易池的小时采样，回看 7×24 / 30×24 小时，基准最多落后 1 小时。全球指数/ETF 复用宏观缓存的 `7d/30d` 自然日变化；原始基准日期不在缓存里，明确为 `null`。利率等宏观序列当前缓存只有最新值，周期收益保持 `null`。

覆盖池有边界：股票和商品仅为 Trade_db 已归档候选，不是全市场；CEX 为 Binance USDT 现货和永续各自的 24h 成交额 Top 150；链上为已发现合约且流动性、24h 成交额各不少于 5 万美元，排除已拦截合约。按市场各保留上涨、下跌代表。原始历史价的复权状态未获可靠验证，`price_adjustment=unknown_from_existing_archive`，不推断为前复权或后复权。ETF/杠杆 ETF 在元数据库类型不可靠时由名称/代码保守识别，并附 `asset_type_source`；可能漏认，不能当作完整证券类型注册表。相对强弱是**同市场候选池收益中位数差**，非指数超额收益。`trend_score` 仅对现有上涨 Trend 入选项填入；下跌或未入选项为 `null` 并说明原因。成交变化仅在可比历史额可得时计算；加密缺同口径历史额则为 `null`。安全检测“通过”也不等于保证可卖出。

所有文件含 `snapshot_id`、`generated_at`、`session_date`、`data_asof`、`quality`、`stale`、`missing_reason`、窗口和来源。US `session_date` 为已完成纽约交易日；A/H 为当地交易日；加密以 UTC 采样时刻为准，`session_date=null`。发布分支只包含公开行情摘要；密钥、聊天 ID 和用户私有数据不会进入导出。

验收：离线测试 `python3 -m unittest discover -s tests -p 'test_generate_agent*py' -v`；服务器真实归档只读导出 7/30 日后检查各市场涨跌、文件大小、质量字段、幂等；通过 GitHub 连接指定分支与路径回读文件。
