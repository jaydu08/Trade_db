# 📊 Trade_db — AI 量化监控系统

> 实时异动监控 · 大宗商品追踪 · 每日榜单 · AI驱动归因 · Telegram推送

---

## 一、系统概述

**Trade_db** 是一个基于 Python 的 AI 驱动量化金融监控系统，通过 Telegram Bot 与用户交互，由后台调度器自动执行各类监控与分析任务。

**核心设计理念：**
- **JIT（即时数据）**：不存储历史 K 线。所有行情在决策时通过新浪/东方财富 API **实时拉取**。
- **AI 归因**：所有异动、榜单均触发 LLM（OpenAI 兼容接口）+ 多源网络搜索进行智能归因。
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
│   │   ├── sync_news.py       # 新闻同步（财联社+Google News RSS）→ ChromaDB
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
│   │   ├── repository.py      # watchlist.json 读写（线程安全单例）
│   │   └── resolver.py        # 股票代码识别（规则匹配+联网搜索）
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

### 3.3 `vector_store`（ChromaDB，语义知识库）

| Collection | 内容 | 用途 |
|---|---|---|
| `company_chunks` | 公司画像分块文本 | 公司语义检索、产业链映射 |
| `industry_knowledge` | 研报摘要、产业链结构描述 | RAG 产业链挖掘 |
| `market_events` | 异动归因报告 | 历史事件回溯 |
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
| `sync_news` | 每 10 分钟 | 同步财联社/Google News到ChromaDB |
| `monitor_scan` | **每 1 分钟** | 自选股异动监控 → Telegram推送 |
| `commodity_scan` | **每 5 分钟** | 大宗商品期货异动监控 → Telegram推送 |
| `cn_heatmap` | 每天 15:30 | A股热门榜单 → Telegram推送 |
| `hk_heatmap` | 每天 16:30 | 港股热门榜单 → Telegram推送 |
| `us_heatmap` | 每天 10:00 | 美股热门榜单 → Telegram推送 |
| `sync_reports` | 每天 18:00 | 同步行业研报到ChromaDB |
| `sync_fundamentals` | 每天 02:00 | 全量财务数据+公司画像更新 |

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

### 5.2 大宗商品监控（`CommodityScanner`）

**数据流：**
```
ak.futures_display_main_sina()（主力合约列表，按日缓存）
→ 分批30个查询 ak.futures_zh_spot(market="CF")
→ 涨跌幅 ≥ 2.0% 触发 _process_anomaly
→ Notifier.broadcast() 发送初始预警
→ 线程池提交 _analyze_and_map：
    ├── LLM 商品归因（CommodityAttribution）
    ├── Path A：本地 ChromaDB 产业链映射
    ├── Path B：LLM生成靶向搜索词 + 全网实时搜索
    └── LLM 合成交易思路（TradingIdea：logic/target_tickers/action）
→ Notifier.broadcast() 发送完整映射策略报告
```

### 5.3 每日榜单（`DailyRankService`）

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

### 5.4 热门榜单（`MarketHeatMap`）

**CN 热榜筛选规则：**
- 涨幅 ≥ 3%，成交额 ≥ 5000万
- 过滤一字板（涨幅接近限制且换手率 < 1%）
- 热度评分 = 归一化涨幅 × log(成交额/千万) × 换手率

**HK/US 热榜筛选规则：**
- 涨幅 ≥ 5%，成交额过滤仙股
- 热度评分 = 涨幅 × log(成交额/百万) × 换手率

### 5.5 ReAct Agent（`ReactAgent`）

手动实现的 ReAct 循环（无需 LangChain），最多6轮工具调用：

| 指令格式 | 对应工具 | 功能 |
|---|---|---|
| `SEARCH: 关键词` | `Tools.web_search` | 并发多搜索引擎聚合（SearXNG/Tavily/Bocha） |
| `QUOTE: 代码或名称` | `Tools.get_quote` | 多源行情（Finnhub → AkShare → Tushare） |
| `DB: 关键词` | `Tools.database_search` | ChromaDB 公司画像语义搜索 |

---

## 六、Telegram Bot 指令

| 指令 | 功能 |
|---|---|
| `/add 代码或名称` | 添加自选股监控（自动识别A股/港股/美股） |
| `/list` | 查看当前监控列表 |
| `/remove 代码` | 移除监控标的 |
| 任意文本 | 触发 AI 投研问答（ReAct Agent） |

---

## 七、搜索数据源（DataManager 路由）

`DataManager` 并发调用所有可用搜索源，自动过滤失去响应的源：

| Provider | 类型 | 配置方式 |
|---|---|---|
| `SearXNGProvider` | 搜索 | `localhost:8080`（本地部署） |
| `TavilyProvider` | 搜索 | `TAVILY_API_KEY` 环境变量 |
| `BochaProvider` | 搜索 | `BOCHA_API_KEY` 环境变量 |
| `FinnhubProvider` | 行情 | `FINNHUB_TOKEN` 环境变量 |
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
TELEGRAM_ADMIN_ID=...          # 管理员 Telegram User ID（接收广播）
ALLOWED_USER_IDS=...           # 允许使用Bot的用户ID，逗号分隔

# 搜索（至少配置一个，否则LLM归因无新闻来源）
BOCHA_API_KEY=...
TAVILY_API_KEY=...

# 可选行情补充
FINNHUB_TOKEN=...
TUSHARE_TOKEN=...
```

---

## 九、快速启动

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