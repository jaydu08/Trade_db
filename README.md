# 🌌 AlphaBase: AI-Native Logic-Mapping Engine

## 1. Project Context (项目背景)
**AlphaBase** 是一个为个人开发者设计的、模块化、可拓展、易维护的 **AI 原生量化投研系统**。

**核心宣言：**
1.  **JIT (Just-In-Time) Information:** 拒绝存储繁重的历史 K 线数据。系统维护静态资产列表，所有行情数据（价格/PE/涨跌幅）在决策时通过 `AkShare` **实时探测**。
2.  **Logic Mapping (逻辑映射):** 专注于捕捉“二阶效应”（Second-Order Effects）。通过向量检索与 LLM 推理，将非结构化信息（新闻/产业链逻辑）转化为可交易标的。
3.  **Separation of Concerns (关注点分离):** 数据存储采用 **"3+1" 分层架构**，将元数据、向量知识、交易账本和运行时缓存严格物理隔离。

---

## 2. System Architecture (系统架构)

系统由 **Kernel (内核)**、**Memory (存储)** 和 **Plugins (插件)** 组成。

### Directory Structure (目录结构)
```text
project_root/
├── config/                 # 全局配置 (Env, Constants)
├── core/                   # 核心基础设施 (单例模式)
│   ├── db.py               # 数据库连接池 (Meta, Ledger, Vector)
│   ├── cache.py            # Runtime Cache (DiskCache)
│   ├── llm.py              # LLM Client (OpenAI/DeepSeek compatible)
│   └── events.py           # 简单的 Event Bus
├── domain/                 # 数据模型 (Schema Definitions)
│   ├── meta.py             # SQLModel: 资产与概念
│   ├── ledger.py           # SQLModel: 信号与订单
│   └── vector.py           # Pydantic: 向量文档结构
├── modules/                # 功能组件 (Tools)
│   ├── ingestion/          # ETL: 同步资产列表、研报入库
│   ├── probing/            # 实时探测器 (AkShare Wrapper)
│   └── analysis/           # 通用分析 (如: 产业链拆解器)
├── strategies/             # 策略插件 (Business Logic)
│   ├── chain_mining.py     # 产业链挖掘策略 (你的核心需求)
│   ├── event_driven.py     # 事件驱动策略
│   └── ...
├── data/                   # 本地存储 (GitIgnore)
│   ├── meta.db             # SQLite: 元数据主库
│   ├── ledger.db           # SQLite: 交易账本库
│   ├── vector_store/       # ChromaDB: 向量知识库
│   └── cache/              # DiskCache: 运行时缓存
└── main.py                 # CLI 入口
```

---

## 3. Technology Stack & Constraints (技术约束)

**AI 助手在生成代码时必须严格遵守以下技术选型：**

* **Language:** Python 3.10+ (强制 100% Type Hinting)。
* **Databases:**
    * **Relational:** `sqlite3` + `SQLModel` (用于 Meta 和 Ledger)。
    * **Vector:** `chromadb` (Persistent Client, 本地文件模式)。
    * **Cache:** `diskcache` (用于 API 结果缓存，防止封 IP)。
* **Data Source:** `akshare` (必须封装 Error Handling 和 Retry 逻辑)。
* **LLM:** `langchain` 或 `openai` SDK (支持 Structured Output)。
* **Environment:** **CPU-First**。Embedding 模型使用 `BAAI/bge-small-zh-v1.5` (量化版或 ONNX)。

---

## 4. The "3+1" Data Architecture (数据契约)

这是系统的核心，必须严格实现。

### A. `meta.db` (元数据主库 - SQLite)
*The Source of Truth. Read-Heavy, Write-Rarely.*

```python
class Asset(SQLModel, table=True):
    symbol: str = Field(primary_key=True)   # "000001"
    name: str
    market: str = "CN"                      # "CN", "US"
    industry: str | None                    # "电子-半导体"
    listing_status: str = "ACTIVE"          # "ACTIVE", "DELISTED"
    
class Concept(SQLModel, table=True):
    code: str = Field(primary_key=True)     # "BK0001"
    name: str                               # "边缘计算"

class AssetConceptMap(SQLModel, table=True):
    symbol: str = Field(primary_key=True)
    concept_code: str = Field(primary_key=True)
```

### B. `vector_store` (向量知识库 - ChromaDB)
*The Brain. Logic & Semantics.*

| Collection | Content (Document) | Metadata Schema | 用途 |
| :--- | :--- | :--- | :--- |
| `c_profiles` | 公司简介 + 主营业务 + 核心产品 | `{"symbol": "...", "industry": "..."}` | 标的映射 |
| `c_industry_logic` | 研报摘要 / 产业链节点描述 | `{"industry": "AI眼镜", "node": "Upstream"}` | **产业链推导** |
| `c_events` | 历史新闻摘要 | `{"date": "...", "impact": "Positive"}` | 历史回溯 |

### C. `ledger.db` (交易账本库 - SQLite)
*The Journal. Write-Heavy.*

```python
class Signal(SQLModel, table=True):
    id: int = Field(primary_key=True)
    timestamp: datetime
    strategy: str           # "ChainMining_v1"
    symbol: str
    direction: str          # "LONG"
    strength: float         # 0.0 - 1.0
    reasoning: str          # LLM 的思考过程 (CoT)
    status: str             # "PENDING", "EXECUTED"
```

### D. `runtime_cache` (运行时缓存 - DiskCache)
*The Buffer.*
* **Key:** `ak_spot_{symbol}_{minute}`
* **Value:** `DataFrame` (Snapshot)
* **TTL:** 60 seconds (防止高频调用 AkShare)

---

## 5. Core Workflow: Chain Extraction Strategy (核心工作流)

针对 **“产业链挖掘”** (如 AI 眼镜) 场景，系统按以下流程运行：

1.  **Decomposition (拆解):**
    * 用户输入: "AI 眼镜产业链"。
    * LLM + `c_industry_logic` 推理: 输出 `["光波导 (上游)", "Micro-LED (上游)", "代工组装 (中游)"]`。
2.  **Vector Mapping (映射):**
    * 系统遍历上述关键词。
    * 在 `c_profiles` 中进行语义检索 (Hybrid Search)。
    * *Query:* "光波导, 衍射光学" -> *Hit:* "水晶光电", "蓝特光学"。
3.  **Filtering (过滤):**
    * 在 `meta.db` 中检查状态 (Active?)。
    * 在 `asset_concept_map` 中检查关联度。
4.  **Probing (探测):**
    * 调用 `modules/probing` 获取实时行情。
    * 过滤掉涨停、停牌或流动性差的标的。
5.  **Signal Generation (决策):**
    * 生成信号并写入 `ledger.db`。

---

## 6. AI Action Plan (Vibe Coding 指引)

**Phase 1: Infrastructure (地基)**
> **Prompt:** "Create the project structure. Implement `core/db.py` to handle SQLite (SQLModel) and ChromaDB connections as singletons. Implement `core/cache.py` using DiskCache."

**Phase 2: The Roster (数据入库)**
> **Prompt:** "Create `modules/ingestion/sync_meta.py`. Fetch all A-share stocks and concepts from AkShare. Store them in `meta.db`. Then, fetch stock profiles, vectorize them, and store in ChromaDB `c_profiles`."

**Phase 3: The Probe (实时探测)**
> **Prompt:** "Create `modules/probing/market.py`. Wrap `ak.stock_zh_a_spot_em`. Implement a caching layer so that repeated calls within 1 minute do not hit the API."

**Phase 4: The Strategy (产业链挖掘)**
> **Prompt:** "Implement `strategies/chain_mining.py`. It should take an industry name, use LLM to decompose it into supply chain nodes, and then query ChromaDB to find matching assets. Finally, check their real-time prices."

---