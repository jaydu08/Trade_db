# Trend × ChatGPT Agent：第一阶段数据接入 PoC

实施与复核时间：2026-10-13 21:03（Asia/Shanghai）。已用**现有公开 GitHub 仓库 + 免费 jsDelivr CDN**完成公网 HTTPS 静态 JSON，不需要 Cloudflare 账号、购买域名、开放新端口或改变 9800 上的业务 API。Trade_db 主仓库 `/root/Trade_db` 只新增生成/发布/校验脚本与 systemd unit；市场快照单独提交在同一 GitHub 仓库的 `agent-data` 分支，本机独立 checkout 为 `/root/trend_agent_public`。现有 `trade_db.service` 与 `crypto-trend.service` 均保持 active，未重启。

## 公网地址与读取方式

- [smoke.json](https://cdn.jsdelivr.net/gh/jaydu08/Trade_db@main/public/agent/v1/smoke.json)：独立本机与服务器 curl 实测 HTTPS 200、可信证书、`application/json; charset=utf-8`、无需 Cookie/Bearer/JS、含真实 ISO-8601 生成时间。
- [latest.json](https://cdn.jsdelivr.net/gh/jaydu08/Trade_db@agent-data/agent/v1/latest.json)：真实跨市场快照，首次独立公网 curl 实测 HTTPS 200、`application/json`，54,570 bytes。
- [2026-10-13 历史快照](https://cdn.jsdelivr.net/gh/jaydu08/Trade_db@agent-data/agent/v1/history/2026-10-13.json)：SHA-256 为 `26335178817050325dc7f4e93941c362f9113089425c79f3852ec229b73d0a21`，与当日 `latest.json` 字节相同。后续日期沿用 `/agent/v1/history/YYYY-MM-DD.json`。

**重要缓存限制：** jsDelivr 官方说明 Git 分支路径会缓存约 12 小时，而且本次实测固定路径的响应含 `max-age=604800`、`s-maxage=43200`。所以 `latest.json` 虽可访问，**不能保证每日 20:00 立即读到当天快照**；日报/周报应优先按北京时间当日构造**日期唯一**的 `history/YYYY-MM-DD.json`，19:45 发布后于 20:00 才读取。日期归档首次发布后不可变，恰好适合 CDN。若未来要可靠的固定 `latest.json` 低延迟刷新，应启用现有公开仓库的免费 GitHub Pages 或其他可控缓存的免费静态站点；目前 GitHub Pages 未启用，不能宣称完成这项优化。[GitHub Pages 免费公开仓库说明](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site)、[jsDelivr 缓存说明](https://github.com/jsdelivr/jsdelivr#caching)。

公网 curl 可读仍**不等于目标 ChatGPT 普通对话/定时任务已实测可读**。本次按范围未配置 ChatGPT 任务，也未调用 OpenAI API、MCP 或构建 Agent Prompt。用户可以在目标 ChatGPT 对话中给出上述日期归档链接做最后一项产品端验证；[OpenAI 的定时任务说明](https://learn.chatgpt.com/docs/automations)不构成“本账号已能读该 URL”的证明。

## 数据契约与实际覆盖

`schema_version=1.0`，顶层包含 `snapshot_id/generated_at/timezone/overview/anomalies/source_health/limitations`。每条市场/数据域标注 `session_date/data_asof/source/quality/stale/missing_reason`；不可得字段为 `null`，不填假零。股票上下行每市场各至多 10 条；当前 10 月 13 日 CN/HK/US 上行各 10、下行各 10；加密 Binance spot/futures 与 onchain 各 10；宏观商品/板块 20 项。发布文件低于 100 KB，未含原始 K 线、用户数据或后台配置。

复用 `Trade_db/data/ledger.db` 的 `TrendDailyBar/DailyRank`、`data/tg_daily_boards/` 精选日榜、`data/market_activity_stock.json` 成交量、`data/macro_dashboard_snapshot.json` 宏观/商品/全球板块、`alpha_news/data/crypto_trend.sqlite` 的 assets/bars/security/health。只读 SQLite 打开，不调用外部行情 API，不做数据库迁移。股票负向异常是已覆盖候选池的区间跌幅，**不是**波动率标准化异常，也不是全市场跌幅榜；日线历史不足时相应收益为 `null`。美股归档可能按北京时间入库，快照按其 UTC 更新时刻转纽约日期：本次 US `session_date=2026-10-12`，CN/HK 为 `2026-10-13`。股票行业/催化标签只有 TG 精选归档包含时才填；本次 US 当天无 TG 精选归档，已在 limitations 标注。

加密 CEX 仅 Binance USDT 现货/永续各按 24h 成交额 Top 150，异动窗口为现有 12h 小时线；链上仅覆盖已发现合约，24h 使用同定价池前一完整小时对比，要求流动性与 24h 成交额各 ≥$50k，排除明确 blocked，并重验 `pass` 是否过期及定价池是否变化。`unknown/review` 仍可能出现，但有 `security_status`，**不代表安全可卖**。宏观观测日期与快照生成时间分离；重要综合指数/全市场广度现有可靠归档不足，保持 `null`。

## 生成、发布、幂等与调度

手动验证/生成（后一个命令读取已有同日不可变归档，重复运行不改历史字节）：

```bash
cd /root/Trade_db
python3 scripts/generate_agent_snapshot.py --root /root/Trade_db --crypto-db /root/alpha_news/data/crypto_trend.sqlite --output /root/trend_agent_public/agent/v1
python3 scripts/validate_agent_snapshot.py --root /root/Trade_db --crypto-db /root/alpha_news/data/crypto_trend.sqlite --latest /root/trend_agent_public/agent/v1/latest.json
python3 scripts/publish_agent_snapshot.py --root /root/Trade_db --crypto-db /root/alpha_news/data/crypto_trend.sqlite --repo /root/trend_agent_public
```

正式调度由独立 `trend-agent-snapshot.timer` 在服务器本地 Asia/Shanghai **每日 19:45** 调用一次性 service；下一次为 2026-10-14 19:45 CST。相较于重启现有 Trade_db/APScheduler，独立定时器把 Git 发布的网络故障隔离在一个短作业里，避免影响线上 Web/TG 进程。发布脚本先 fetch/ff-only 合并数据分支、对同日归档执行原子不可变创建、原子替换 latest，再仅 stage 三个白名单 JSON 路径并推送；重复执行同日不产生新提交，发布失败不把昨天快照冒充今天。历史不自动删除，因此至少保留 30 天。`systemctl start trend-agent-snapshot.service` 已实测 `Result=success, ExecMainStatus=0`；timer 已 enabled/active。查看运行结果：

```bash
systemctl list-timers trend-agent-snapshot.timer
journalctl -u trend-agent-snapshot.service -n 50 --no-pager
```

## 验收记录

1. 公网：smoke 与 latest 由服务器和另一台本机分别通过受信任 HTTPS 获取，200、JSON MIME、匿名，无登录/JS；日期归档由本机校验哈希。未在用户的 ChatGPT 对话中实测。
2. 真实性：`validate_agent_snapshot.py` 对比原始库 7 条，CN/HK/US 上行各 1、下行各 1、宏观 1，**全部 PASS**；加密资产每分钟会覆盖，若原始 `updated` 已变化则不拿实时值伪作当时的逐条比对。
3. 过期/读取失败：本地离线测试把健康时间推进 2 天并模拟加密数据库缺失，`stale` 或 `quality=missing/missing_reason` 明确出现；不会填 0。共 4 个新单元测试通过，服务器同样通过。
4. 覆盖：本次 CN/HK/US 各有上下行记录，加密三类有记录，宏观有 20 项；完整市场广度和主要综合指数明确缺失。
5. 历史：同日重复生成/发布后 `snapshot_id`、归档哈希及数据分支 commit 不变；`latest` 与历史字节一致。首次发布后再次运行未产生新提交。

## 安全、限制及回滚

- 静态数据分支仅有三个白名单 JSON 文件；绝不发布业务仓库根目录、数据库、账户/仓位、供应商密钥、JWT、Cookie、日志、敏感钱包记录。发布前检查 54,570-byte 文件的常见敏感模式，无命中。第三方市场数据的二次分发许可仍需核对；公网资源无法保证删除后从 CDN 彻底撤回，因此今后新增字段必须继续白名单审核。
- 既有 `interface/api.py` 的“仅用户名即可登录”、365 天 JWT 与默认签名密钥回退属于**高优先级独立安全问题**。建议补密码验证、强制环境密钥、缩短令牌有效期并制定平滑会话迁移；本 PoC 不触碰旧登录，避免线上用户意外登出。
- 停止未来发布：`systemctl disable --now trend-agent-snapshot.timer`。如需撤销代码，用 Git 对 `ae91813`、`2de245a` 两个 PoC 提交做定向 revert（先审查是否有后续提交），并移除对应 unit；勿对含用户运行数据的工作树执行 hard reset。`agent-data` 分支可停止更新或在明确同意后删除，但已被 CDN 缓存的公开内容不能保证收回。
