# 📊 Trade_db — AI 量化监控系统

> 实时异动监控 · 大宗商品追踪 · 每日榜单 · AI驱动归因 · Telegram推送

---

## 一、系统概述

**Trade_db** 是一个基于 Python 的 AI 驱动量化金融监控系统，通过 Telegram Bot 与用户交互，由后台调度器自动执行各类监控与分析任务。

**核心设计理念：**
- **JIT（即时数据）**：不存储历史 K 线。所有行情在决策时通过新浪/东方财富 API **实时拉取**。
- **AI 归因 + 降级**：异动/复盘优先 LLM 归因，失败时可回退到本地事件库（定向新闻）兜底，不阻塞推送。
- **多市场覆盖**：A股（CN）、港股（HK）、美股（US）、大宗商品期货（CF）。
- **Telegram 优先**：所有输出均通过 Telegram Bot 推送，支持自选股管理与 AI 问答。

---

## 二、目录结构

```
Trade_db/
├── main.py                    # 系统入口：初始化DB → 启动调度器 → 启动 Telegram Bot
├── run.sh / stop.sh           # 后台进程管理脚本
├── config/
│   └── settings.py            # 全局常量（路径、LLM参数、市场常量等）
├── core/
│   ├── db.py                  # DB管理器：SQLite(meta/ledger) + ChromaDB 单例
│   ├── scheduler.py           # APScheduler 定时任务注册与执行
│   ├── llm.py                 # LLM客户端封装（OpenAI兼容，支持结构化输出）
│   ├── agent.py               # ReAct Agent + Tools（web_search/database_search/get_quote）
│   └── cache.py               # DiskCache 装饰器（行情缓存，防封IP）
├── domain/
│   ├── meta/                  # SQLModel ORM：Asset（资产表）
│   └── ledger/                # SQLModel ORM：DailyRank（日榜）、WatchlistAlert（预警记录）
├── modules/
│   ├── ingestion/
│   │   ├── akshare_client.py  # AkShare封装（重试装饰器+DiskCache）
│   │   ├── data_factory.py    # DataManager：并发多源搜索路由（SearXNG/Tavily/Bocha）
│   │   ├── sync_news.py       # 定向新闻同步（trend/heatmap + 自选/持仓）→ ChromaDB
│   │   ├── sync_reports.py    # 研报同步 → ChromaDB
│   │   ├── sync_financial.py  # 财务数据同步 → SQLite
│   │   ├── sync_profile.py    # 公司画像向量化 → ChromaDB
│   │   ├── sync_relations.py  # 供应链关系提取（LLM）→ ChromaDB
│   │   └── sync_asset.py      # 股票资产列表同步 → meta.db
│   ├── monitor/
│   │   ├── scanner.py         # 个股异动监控（MonitorService）
│   │   ├── commodity_scanner.py  # 大宗商品异动监控（CommodityScanner）
│   │   ├── daily_rank_service.py # 每日榜单抓取与持久化（DailyRankService）
│   │   ├── notifier.py        # Telegram 通知封装（含3次重试+指数退避）
│   │   ├── manager.py         # 监控列表管理（增删查）
│   │   ├── news_intel.py      # 新闻情报层（新闻强度分/兜底归因）
│   │   ├── repository.py      # watchlist.json 读写（线程安全单例）
│   │   └── resolver.py        # 股票代码识别（规则匹配+联网搜索）
│   │   ├── us_premarket_scanner_service.py # 美股盘前猎手（盘前异动扫描推送）
│   ├── analysis/
│   │   └── heatmap.py         # 热门榜单生成+LLM归因+Telegram推送
│   └── probing/
│       ├── async_prober.py    # 异步行情探针（aiohttp + Semaphore并发控制 + tenacity重试）
│       └── market.py          # 市场时间工具
├── strategies/
│   └── chain_mining.py        # 产业链RAG挖掘策略
├── interface/
│   └── telegram_bot.py        # Telegram Bot 命令处理
├── data/
│   ├── meta.db                # SQLite：资产基础信息
│   ├── ledger.db              # SQLite：日榜记录、预警记录
│   ├── watchlist.json         # JSON：自选股监控列表（含chat_id绑定）
│   ├── vector_store/          # ChromaDB 持久化目录
│   └── cache/                 # DiskCache 行情缓存
└── tests/
    └── run_all_features.py    # E2E 集成测试脚本
```

---

## 三、数据存储架构（3层 + 1缓存）

### 3.1 `meta.db`（SQLite，元数据）

| 表 | 字段 | 说明 |
|---|---|---|
| `asset` | symbol, name, market, industry, listing_status | 股票基础信息 |

### 3.2 `ledger.db`（SQLite，业务记录）

| 表 | 关键字段 | 说明 |
|---|---|---|
| `dailyrank` | date, market, rank_type, symbol, name, price, change_pct, amount, turnover_rate | 每日榜单 |
| `watchlistalert` | symbol, name, market, alert_reason, price, change_pct, status | 异动预警记录 |
| `papertrade` | symbol, name, market, entry_date, entry_price, status, exit_price, pnl_pct, review_status, review_attempts, review_error, review_source, last_reviewed_at | AI 模拟投研复盘（含状态机与失败追踪） |

### 3.3 `vector_store`（ChromaDB，语义知识库）

| Collection | 内容 | 用途 |
|---|---|---|
| `company_chunks` | 公司画像分块文本 | 公司语义检索、产业链映射 |
| `industry_knowledge` | 研报摘要、产业链结构描述 | RAG 产业链挖掘 |
| `market_events` | 异动归因 + 定向新闻事件 | 复盘/异动兜底/趋势新闻强度 |
| `entity_relation` | 供应链关系三元组 | 知识图谱查询 |

### 3.4 `data/cache/`（DiskCache，运行时缓存）

通过 `@cached(key, ttl)` 装饰器对 AkShare API 结果缓存：
- 行情数据 TTL = 60s
- 股票列表、财务数据 TTL = 1h / 24h

---

## 四、定时任务一览

由 `APScheduler (BackgroundScheduler)` 管理，系统启动后自动运行：

| Job | 触发方式 | 功能 |
|---|---|---|
| `sync_news` | `NEWS_SYNC_INTERVAL_MINUTES`（默认120分钟） | 定向新闻同步（trend/heatmap + 自选/持仓）到 `market_events` |
| `monitor_scan` | **每 1 分钟** | 自选股异动监控 → Telegram推送 |
| `commodity_scan` | 周二至周六 08:00 | 大宗商品每日固定战报 → Telegram推送 |
| `cn_heatmap` | 工作日 18:35 | A股热门榜单 → Telegram推送 |
| `hk_heatmap` | 工作日 18:30 | 港股热门榜单 → Telegram推送 |
| `us_heatmap` | 周二至周六 08:00 | 美股热门榜单 → Telegram推送 |
| `us_premarket_hunter` | 工作日 20:00/21:00 | 美股盘前猎手（函数内按纽约08:00守卫，自动兼容DST） |
| `sync_reports` | 每天 18:00 | 同步行业研报到ChromaDB |
| `sync_fundamentals` | 每天 02:00 | 全量财务数据+公司画像更新 |
| `trend_7d` | 每周日 10:00 | 7日趋势简报推送 |
| `trend_30d` | 每月最后一天 11:00 | 30日趋势简报推送 |
| `daily_summary` | 工作日 19:00 | 每日推送标的汇总TXT |
| `trend_pool_refresh_cn_hk` | 工作日 19:10 | 趋势池日线补齐(A/H) |
| `trend_pool_refresh_cn_hk_retry` | 工作日 21:10 | 趋势池日线补齐(A/H)重试 |
| `trend_pool_refresh_us_cf` | 周二至周六 08:10 | 趋势池日线补齐(US/CF) |
| `trend_pool_refresh_us_cf_retry` | 周二至周六 10:10 | 趋势池日线补齐(US/CF)重试 |
| `ipo_tomorrow` | 每天 16:30 | 次日新股预告推送（A/H/US） |
| `paper_trade_check` | 每天 19:20 | 模拟交易到期检查与自动平仓复盘推送 |

---

## 五、功能模块详解

### 5.1 个股异动监控（`MonitorService`）

**数据流：**
```
watchlist.json → 过滤开盘中标的 → AsyncMarketProber（新浪 HQ 异步批量拉取）
→ 检查涨跌幅阈值（默认3%，可按股设置）→ 冷却判断（同一日只触发一次）
→ 线程池提交 _analyze_and_report（多源搜索 + LLM归因）
→ 单条完整消息推送 Telegram（价格+归因+置信度+摘要）
→ 归因结果存入 ChromaDB market_events
```

**市场开盘时间（时区感知）：**
- CN（上海）：09:30-11:35，13:00-15:30，仅工作日
- HK（香港）：09:30-12:05，13:00-16:30，仅工作日
- US（纽约）：09:30-16:30，仅工作日

**归因步骤（`_analyze_and_report`）：**
1. 并发双路搜索（个股专项 + 大盘板块背景）
2. LLM结构化输出（`AnalysisResult`：reason/confidence/summary/sources）
3. 组装单条紧凑消息后发送（**不再两阶段发送**）
4. 若 LLM 不可用，使用 `news_intel.build_fallback_reason` 基于近端事件兜底归因，并附带新闻强度

### 5.2 全市场长线趋势挖掘（`/trend` 生态）

**架构流转：**
```
每日收盘盘后 (CN/HK/US/商品) → 提取热点 Top 10 → 归因 
→ 存入 SQLite TrendSeedPool (带30日滚动清理)
→ 用户触发 /trend N → 提取 N日内种子池
→ 获取所有初筛池标的 当前价 vs N天前真实收盘价
→ 聚合同池标的多日异动理由 + 近端新闻强度（news_intel）→ 送入大模型提取【产业链/宏观主线归因】
→ Telegram 并发分包投递长线深度研报
```

### 5.2 大宗商品监控（`CommodityScanner`）

**功能定位：**
每天 **08:00 定时执行**的盘前大宗商品战报，涵盖前一日日盘及夜盘的全貌，不再产生高频碎片化推送。

**数据流：**
```
ak.futures_display_main_sina()（拉取全市场约 80+ 个实物主力合约）
→ 逐个查询 ak.futures_zh_spot(market="CF") 截面收盘数据
→ 按内置的 5 大骨干分类字典（贵金属、黑色系、化工、农产品、特殊）进行静态归置
→ （特殊防御逻辑：匹配不到的新品种统一兜底至“航运及特殊”板块）
→ 每个板块按当日绝对涨幅降序排列，各自精准截断 Top 3（且仅筛选涨幅 > 0%）
→ 将最高 15 个品种提交 _analyze_and_push：
    ├── 聚合各板块最新资讯
    ├── LLM 以“板块”为维度，结构化输出（SectorAnalysis）：
    │   ├── 提炼板块集体异级的核心【宏观催化剂】
    │   └── 映射 A/港股核心【受益标的】
    └── 组装为【图文并茂的单条每日复盘战报】
→ Notifier.broadcast() 发送
```

### 5.3 每日榜单（`DailyRankService`）

补充：每日 19:00 汇总 TXT（`daily_summary`）的股票行已与推送口径对齐，统一输出
`现价 | 涨幅 | 市值`，并支持 A/H/US 三市场市值展示。

**数据流：**
```
检查 _should_sync_today()（CN/HK跳过周末，US跳过周日/周一北京时间）
→ 检查当天是否已同步
→ akshare_client.get_daily_top_ranks(market, rank_type, top_n=10)：
    ├── CN：ak.stock_zh_a_spot_em() → 粗排300条 → _distill_ranks精排
    ├── HK：ak.stock_hk_spot_em()
    └── US：ak.stock_us_spot_em()
→ 存入 ledger.db DailyRank 表
→ Top3 提交 AI 归因（_analyze_and_report）
```

**精排算法（`_distill_ranks`）：**
- 综合评分 = 涨幅归一化×40% + 换手率归一化×40% + 成交额归一化×20%

### 5.4 热门榜单（`MarketHeatMap` + 新闻强度重排）

**CN 热榜（当前版本：五因子评分 + 市场状态切换）**
- 候选池：`成交额 >= 5000万`，并按板块涨停幅度做归一化涨幅过滤
  - 主板按 10%，创业板/科创板按 20%，北交所按 30%
- 入池阈值：
  - 常态：`normalized_pct >= 0.60`
  - 弱市降级：`normalized_pct >= 0.50`
- 近涨停抹平：`0.97~1.03` 统一视作 `1.0`，避免 19.98/19.99 这类噪声
- 换手率因子：
  - 优先使用实时换手率
  - 缺失时用近似换手率回填：`amount / (circ_mv_100m * 1e8) * 100`
- 市值因子：
  - 候选池内按成交额优先抓取总市值（腾讯接口），`log1p` 后做百分位评分
  - 目标是提高大票趋势线标的权重，而非硬性过滤
- 趋势延续因子：
  - 综合近 5/10 日动量 + 近阶段新高特征（来自 `TrendDailyBar`）
- 最终评分（默认）= 五因子加权：
  - `rank_pct + rank_log_amount + rank_turnover + rank_mcap + rank_trend`
  - 震荡市默认：`0.16/0.30/0.20/0.24/0.10`
  - 趋势市默认：`0.22/0.28/0.20/0.18/0.12`
  - 程序会基于"上涨占比>=60%"且"全市场成交额>=近7日均量*1.05"自动切换权重
  - 可叠加新闻强度重排：`heat_score_v2 = (1-HEATMAP_W_NEWS)*rank(heat_score)+HEATMAP_W_NEWS*news_intensity`
- 结构性加成：创业板/科创板且涨幅 >10%，默认 `1.10x`

**HK/US 热榜筛选规则**
- 涨幅 ≥ 5%，成交额过滤仙股
- 评分 = 涨幅百分位 + 成交额百分位 + 换手率百分位
- HK/US 市值补齐：
  - HK：`Finnhub profile2` 优先，`Yahoo Finance` 兜底，展示 `市值:xx亿港元`
  - US：`Finnhub profile2`（百万美元口径）
- US 额外：去权证、同底层杠杆ETF去重；并做市值过滤
- 美股超大市值提权：>500亿美金 *1.3，>1000亿 *1.6，>3000亿 *2.0


### 5.4.1 美股盘前猎手（`USPremarketScannerService`）

- 扫描时点：北京时间 20:00/21:00 双触发，函数内仅在纽约时间工作日 08:00 执行（防重复且自动兼容夏令时）
- 数据源：Yahoo quote 接口（`preMarketPrice` / `preMarketVolume` / `preMarketChangePercent` / `marketCap`）
- 入池门槛：
  - 盘前成交额 `preMarketPrice * preMarketVolume > 500万 USD`
  - `abs(盘前涨跌幅) > 5%`
  - `总市值 > 10亿 USD`
- 输出：按盘前成交额与波动排序，推送 Top 5 至 Telegram

### 5.5 ReAct Agent（`ReactAgent`）

手动实现的 ReAct 循环（无需 LangChain），最多6轮工具调用：

| 指令格式 | 对应工具 | 功能 |
|---|---|---|
| `SEARCH: 关键词` | `Tools.web_search` | 并发多搜索引擎聚合（SearXNG/Tavily/Bocha） |
| `QUOTE: 代码或名称` | `Tools.get_quote` | 多源行情（Finnhub → AkShare → Tushare） |
| `DB: 关键词` | `Tools.database_search` | ChromaDB 公司画像语义搜索 |


### 5.6 模拟交易与复盘（`PaperTradingService` + `PaperTradeReviewer`）

**交互命令：**
- `/buy 代码 [天数] [逻辑]`：建仓并进入跟踪
- `/sell 代码`：平仓并立即触发复盘
- `/review 代码`：手动复盘最近一笔交易记录
- `/holds`：查看当前持仓

**复盘状态机：**
- `PENDING`：待复盘
- `DONE`：复盘成功
- `FAILED`：复盘失败（记录 `review_error`）

**可靠性改进：**
- `/sell` 与 `/review` 都会持久化复盘结果，不再“只推送不落库”
- 复盘事件优先消费 `targeted_news`，并注入“新闻强度/新闻条数”到复盘 Prompt
- 自动到期复盘任务失败不会阻断调度主流程
- 复盘记录保留 `review_attempts/review_source/last_reviewed_at` 便于追踪


---

## 六、Telegram Bot 指令

| 指令 | 功能 |
|---|---|
| `/add 代码或名称` | 添加自选股监控（自动识别A股/港股/美股） |
| `/list` | 查看当前监控列表 |
| `/del 代码` | 移除监控标的 |
| `/quote 代码` | 实时行情查询 |
| `/chain 产业名` | 产业链深度挖掘 (已过滤 AI 推理过程) |
| `/buy 代码 [天数] [逻辑]`| 模拟建仓并进入跟踪状态 |
| `/sell 代码` | 模拟平仓并输出 AI 深度回测报告 |
| `/holds` | 查看当前正在进行的模拟持仓列表 |
| `/review 代码` | 手动提取特定股票的最新 AI 回测复盘 |
| 任意文本 | 触发 AI 投研问答（被@或群内直接提及即可） |

---

## 七、搜索数据源（DataManager 路由）

`DataManager` 并发调用所有可用搜索源，自动过滤失去响应的源：

| Provider | 类型 | 配置方式 |
|---|---|---|
| `SearXNGProvider` | 搜索 | `localhost:8080`（本地部署） |
| `TavilyProvider` | 搜索 | `TAVILY_API_KEY` 环境变量 |
| `BochaProvider` | 搜索 | `BOCHA_API_KEY` 环境变量 |
| `FinnhubProvider` | 行情 | `FINNHUB_API_KEY` 环境变量 |
| `AkShareProvider` | 行情 | 免费，内置 |
| `TushareProvider` | 行情 | `TUSHARE_TOKEN` 环境变量 |

---

## 八、环境配置

复制 `.env.example` 为 `.env` 并填写以下配置：

```env
# LLM（必填）
OPENAI_API_KEY=sk-...
OPENAI_API_BASE=https://api.openai.com/v1   # 可替换为任何兼容接口
OPENAI_MODEL=gpt-4o-mini

# Telegram Bot（必填）
TELEGRAM_BOT_TOKEN=...
TELEGRAM_ADMIN_ID=...          # 接收广播推送的群组ID或个人ID (建议填入群组以实现群发)
ALLOWED_USER_IDS=...           # 允许私聊使用的个人ID，逗号分隔
ALLOWED_GROUP_IDS=...          # 允许该群组成员全体使用的群组ID，逗号分隔

# 搜索（至少配置一个，否则定向新闻/LLM归因无新闻来源）
BOCHA_API_KEY=...
TAVILY_API_KEY=...

# 可选行情补充
FINNHUB_API_KEY=...
TUSHARE_TOKEN=...

# 定向新闻同步（建议）
NEWS_SYNC_INTERVAL_MINUTES=120
NEWS_TARGET_MAX_SYMBOLS=24
NEWS_LIMIT_PER_SOURCE=2
NEWS_SEARCH_TIMEOUT=12
NEWS_TREND_LOOKBACK_DAYS=2
NEWS_RANK_LOOKBACK_DAYS=1

# Heatmap 新闻强度（可选）
HEATMAP_W_NEWS=0.12
HEATMAP_NEWS_LOOKBACK_DAYS=3

# Trend 新闻强度（可选）
TREND_NEWS_BOOST=0.18
TREND_NEWS_LOOKBACK_DAYS=7

# A股 Heatmap 五因子参数（可选）
CN_HEAT_NORM_MIN=0.60
CN_HEAT_NORM_FALLBACK_MIN=0.50
CN_HEAT_NEAR_LIMIT_LOW=0.97
CN_HEAT_NEAR_LIMIT_HIGH=1.03
CN_HEAT_GEM_BONUS=1.10
CN_HEAT_TURNOVER_FETCH_CAP=220
CN_HEAT_MCAP_FETCH_CAP=260
CN_HEAT_TREND_LOOKBACK_DAYS=20

# A股 Heatmap 市场状态切换阈值（可选）
CN_HEAT_REGIME_POS_RATIO=0.60
CN_HEAT_REGIME_MEDIAN_PCT=1.00

# 震荡市权重（pct/amount/turnover/mcap/trend）
CN_HEAT_W_PCT_RANGE=0.16
CN_HEAT_W_AMOUNT_RANGE=0.30
CN_HEAT_W_TURNOVER_RANGE=0.20
CN_HEAT_W_MCAP_RANGE=0.24
CN_HEAT_W_TREND_RANGE=0.10

# 趋势市权重（pct/amount/turnover/mcap/trend）
CN_HEAT_W_PCT_TREND=0.22
CN_HEAT_W_AMOUNT_TREND=0.28
CN_HEAT_W_TURNOVER_TREND=0.20
CN_HEAT_W_MCAP_TREND=0.18
CN_HEAT_W_TREND_TREND=0.12
```

---

## 九、快速启动

> 单实例说明：同一个 Telegram Bot Token 必须只运行一个进程，否则会出现 `409 Conflict`。

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境
cp .env.example .env
# 编辑 .env 填写 Token

# 3. 后台启动
sh run.sh

# 4. 查看实时日志
tail -f trade_db.log

# 5. 停止服务
sh stop.sh
```

---

## 十、技术栈

| 类目 | 选型 |
|---|---|
| 语言 | Python 3.12 |
| 关系数据库 | SQLite + SQLModel |
| 向量数据库 | ChromaDB（本地持久化） |
| 运行时缓存 | DiskCache |
| 定时调度 | APScheduler |
| 异步行情 | aiohttp + tenacity |
| LLM | OpenAI SDK（兼容 DeepSeek、通义、本地模型） |
| Embedding | `BAAI/bge-small-zh-v1.5`（CPU） |
| 市场数据 | AkShare（主）+ 新浪 HQ API（行情）|
| Telegram | python-telegram-bot |