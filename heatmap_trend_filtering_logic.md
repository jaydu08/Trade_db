# Heatmap & Trend 推送标的筛选逻辑

> 本文档描述 Trade_db 中 **Heatmap（每日盘后热门榜单）** 和 **Trend（长线趋势简报）** 两大模块对 CN/A股、HK/港股、US/美股、CF/期货 四市场的标的筛选全流程。

---

## 一、Heatmap 筛选逻辑

### 1.1 数据源与通用预处理

| 市场 | 数据源 | 接口 |
|---|---|---|
| CN | 新浪批量行情 | `AkShareClient._fetch_bulk_sina('CN')` |
| HK | AkShare 港股行情 | `get_stock_info_hk()` → fallback `stock_hk_spot_em()` |
| US | AkShare 美股行情 | `get_stock_info_us()` → fallback `stock_us_spot_em()` |

**通用预处理（`_generate_heatmap`）：**
1. 字段标准化映射（代码→symbol、名称→name、最新价→price、涨跌幅→pct_chg 等）
2. 必填字段检查（symbol/name/price/pct_chg/amount）
3. 缺失字段补 0（turnover/open/high/volume）
4. `pd.to_numeric` 类型转换 + `dropna`
5. **仙股过滤**：`amount < min_amount` 直接剔除

| 市场 | min_amount | top_n |
|---|---|---|
| CN | 5000万 CNY | 10 |
| HK | 5000万 CNY | 5 |
| US | 2000万 USD | 10 |

---

### 1.2 A股 / CN 热榜 — 五因子评分 + 市场状态切换

#### Step 1: Hard Funnel（硬性过滤）

```
成交额 >= max(min_amount, cn_hard_amount_min=2亿)
    ↓
并发获取总市值（get_cn_market_metrics，16线程）
    ↓
市值失败不丢票：
    - 未知市值（获取失败）→ 保留
    - 已知市值 >= cn_hard_total_mv_100m_min=50亿 → 保留
    - 已知市值 < 50亿 → 剔除
```

#### Step 2: 归一化涨幅 + 入池门槛

```
按板块涨停幅度归一化：
    - 主板（非30/688/8/4开头）: limit = 10%
    - 创业板（30开头）/ 科创板（688开头）: limit = 20%
    - 北交所（8/4开头）: limit = 30%

normalized_pct = pct_chg / limit

入池门槛：normalized_pct >= CN_HEAT_NORM_MIN=0.60
    ↓ 若候选池为空
弱市降级：normalized_pct >= CN_HEAT_NORM_FALLBACK_MIN=0.50
```

#### Step 3: 近涨停抹平

```
normalized_pct ∈ [0.97, 1.03] → 统一视为 1.0
（避免 19.98%/19.99% 噪声干扰排序）
裁剪到 [0, 1.2]
```

#### Step 4: 五因子评分

| 因子 | 计算方式 | 说明 |
|---|---|---|
| **rank_pct** | normalized_pct 的百分位排名 | 涨幅因子 |
| **rank_amount** | `log1p(amount)` 的百分位排名 | 成交额因子 |
| **rank_turnover** | 换手率百分位（缺失时用市值近似回填） | 活跃度因子 |
| **rank_mcap** | `log1p(total_mv)` 的百分位排名 | 大票因子 |
| **rank_trend** | 5/10日动量 + 近阶段新高特征 | 趋势延续因子 |

**权重配置（自动切换）：**

| 市场状态 | pct | amount | turnover | mcap | trend |
|---|---|---|---|---|---|
| 震荡市（默认） | 0.16 | 0.30 | 0.20 | 0.24 | 0.10 |
| 趋势市 | 0.22 | 0.28 | 0.20 | 0.18 | 0.12 |

**趋势市判定条件：** 上涨占比 >= 60% **且** 全市场成交额 >= 近7日均量 * 1.05

```
heat_score = rank_pct*w_pct + rank_amount*w_amount + rank_turnover*w_turnover + rank_mcap*w_mcap + rank_trend*w_trend
```

#### Step 5: 结构性加成

- 创业板/科创板 且涨幅 > 10%：`heat_score *= 1.10`

#### Step 6: FOMO 惩罚（高位派发检测）

```
upper_shadow_pct = (high - max(open, price)) / max(open, price)

is_fomo = upper_shadow_pct > FOMO_UPPER_SHADOW_PCT(=3%)
        and volume > MA5(volume)

触发时：heat_score *= FOMO_PENALTY_FACTOR(=0.95)
```

#### Step 7: 新闻强度重排

```
取 heat_score Top 50 → 并发查新闻（lookback=3天）
heat_score_v2 = (1 - HEATMAP_W_NEWS=0.12) * rank(heat_score)
                + HEATMAP_W_NEWS * news_intensity
```

最终取 Top 10。

---

### 1.3 港股 / HK 热榜

```
涨幅 >= 5%
    ↓
Hard Funnel: 成交额 >= HK_HARD_AMOUNT_MIN=5亿
    ↓
heat_score = rank_pct*0.50 + rank_amount*0.30 + rank_turnover*0.20
    ↓
FOMO 惩罚
    ↓
新闻强度重排 → Top 5
```

---

### 1.4 美股 / US 热榜

```
涨幅 >= 5%
    ↓
Hard Funnel:
    1. 按成交额降序
    2. 取前 US_HEAT_MCAP_FETCH_CAP=160 只候选
    3. 并发获取市值（get_us_market_metrics，8线程）
       - Finnhub → Yahoo Finance → DiskCache(TTL=1800s)
    4. 市值失败不丢票：
       - 未知市值 → 保留
       - 已知市值 >= US_HARD_MCAP_MUSD_MIN=1000M USD → 保留
       - 已知市值 < 1000M USD → 剔除
    ↓
heat_score = rank_pct*0.50 + rank_amount*0.30 + rank_turnover*0.20
    ↓
超大市值提权（基于 market_cap_musd）：
    >= 3000亿 USD → *2.0
    >= 1000亿 USD → *1.6
    >= 500亿 USD  → *1.3
    ↓
FOMO 惩罚
    ↓
去重：
    - 去除权证（名称含 wt/warrant/rights/rts/unit）
    - 杠杆ETF去重（同底层 ticker 只留成交额最大的）
    ↓
新闻强度重排 → Top 10
```

---

### 1.5 Heatmap 推送前处理（所有市场通用）

1. **市值补齐**（`_enrich_market_metrics`）
   - CN: `get_cn_market_metrics` + `get_cn_fund_flow`
   - HK: `get_hk_market_metrics`
   - US: 已在 Hard Funnel 中获取

2. **行业标签**（`enrich_industry_labels`）
   - 优先复用 `get_*_market_metrics` 缓存中的 `finnhub_industry`
   - 零额外 Finnhub 请求

3. **入库**
   - `DailyRank`（当日热榜持久化）
   - `TrendSeedPool`（长线趋势种子池）
   - `TrendDailyBar`（日线快照）

---

## 二、Trend 筛选逻辑

### 2.1 种子池（TrendSeedPool）

**入池来源（EOD_SOURCES）：**
- `daily_rank`: 每日榜单 Top 10
- `heatmap`: 热榜结果
- `commodity`: 大宗商品
- `trend_pool_refresh_eod`: 趋势池日线补齐
- `selftest`: 自检

**保留策略：**
- 60 天滚动清理
- 按市场硬顶（优先保留最近出现的标的）：

| 市场 | 硬顶 |
|---|---|
| CN | 100 |
| US | 100 |
| HK | 50 |
| CF | 30 |

---

### 2.2 日线数据（TrendDailyBar）

- 180 天滚动保留
- 仅接受 EOD_SOURCES 来源的数据写入
- **US 优先用 Finnhub 实时价覆盖**（防口径偏差）
- **港/美价格异常保护**：
  - close / 历史中位数 > 2.5 或 < 0.4
  - 且涨跌幅 <= 25%
  - → 判定为错误口径（如币种错位），丢弃

---

### 2.3 Trend 计算（`TrendCalculator.calculate_trend`）

#### Step 1: 查询种子池

查询过去 N 天（默认7天）的 `TrendSeedPool` 记录。

#### Step 2: 去重合并 + 候选池限额

```
按 (market, symbol) 去重
    ↓
每市场最多 TREND_MAX_SYMBOLS_PER_MARKET=60 只
    - 排序依据：最近出现日期 desc → 理由数量 desc
```

#### Step 3: 并发计算每标的

**a) N 日涨幅计算（仅本地数据，零外部调用）：**
```
优先级1: TrendDailyBar 本地收盘价序列
优先级2: DailyRank 历史价格序列
```

**b) 理由聚合（同类去重 + 新鲜度衰减）：**
```
1. 同票同类理由去重（SequenceMatcher 相似度 >= 0.82）
2. 越新的信号权重越高：weight = 0.88 ^ days_ago
3. 取 Top 3 理由聚合
4. signal_strength = Top 3 权重和
```

**c) 新闻强度：**
```
summarize_symbol_news(lookback_days, max_items=18)
news_intensity = intensity_score
```

**d) 趋势总分：**
```
freshness_factor = min(1.25, 0.85 + signal_strength * 0.18)
news_factor      = min(1.25, 0.90 + news_intensity * TREND_NEWS_BOOST=0.18)
trend_score      = return_pct * freshness_factor * news_factor
```

#### Step 4: 分组取 Top 10

```
按 market 分组
    ↓
trend_score 降序
    ↓
每市场取 Top 10
```

---

### 2.4 Trend Report 推送前处理

#### Step 1: 新鲜度检查（`_is_fresh_price`）

| 市场 | 最大价格年龄 |
|---|---|
| US / CF | 4 天 |
| CN / HK | 3 天 |

超过则该标的不进入推送列表。

#### Step 2: 数量精简（`_pick_market_items`）

| 市场 | 推送数量 |
|---|---|
| HK | 前 5 |
| 其他 | 前 8 |

#### Step 3: CF 特殊处理（`_pick_cf_items`）

```
按商品分类分组
    ↓
每类取 trend_score 最高的 1 个代表
    ↓
保底 3 个，最多 5 个
```

#### Step 4: 实时价补齐（`_refresh_market_prices`）

| 市场 | 策略 |
|---|---|
| US | **强制刷新**，必须 Finnhub provider（带 120s 缓存） |
| CN / HK | 仅 price <= 0 时补齐 |

#### Step 5: 市值补齐（`_enrich_market_caps`）

| 市场 | 来源 |
|---|---|
| CN | `get_cn_market_metrics` + `get_cn_fund_flow` |
| HK | `get_hk_market_metrics` |
| US | `get_us_market_metrics`（Finnhub → Yahoo Finance → Cache） |

#### Step 6: 行业标签 + LLM 总结

- `enrich_industry_labels`: 复用市值缓存中的 `finnhub_industry`
- `_llm_summary`: 内存保护（RSS > 1500MB 时降级），输出 3 行（主线/抱团/独立）

---

## 三、关键环境变量速查

### Heatmap

| 变量 | 默认值 | 说明 |
|---|---|---|
| `CN_HEAT_NORM_MIN` | 0.60 | A股归一化涨幅入池门槛 |
| `CN_HEAT_NORM_FALLBACK_MIN` | 0.50 | 弱市降级门槛 |
| `CN_HARD_AMOUNT_MIN` | 2亿 | A股成交额硬过滤 |
| `CN_HARD_TOTAL_MV_100M_MIN` | 50 | A股市值硬过滤（亿元） |
| `HK_HARD_AMOUNT_MIN` | 5亿 | 港股成交额硬过滤 |
| `US_HARD_MCAP_MUSD_MIN` | 1000 | 美股市值硬过滤（百万美元） |
| `US_HEAT_MCAP_FETCH_CAP` | 160 | 美股市值查询候选上限 |
| `HEATMAP_W_NEWS` | 0.12 | 新闻强度重排权重 |
| `FOMO_UPPER_SHADOW_PCT` | 0.03 | FOMO上影线阈值 |
| `FOMO_PENALTY_FACTOR` | 0.95 | FOMO降权乘数 |

### Trend

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TREND_MAX_SYMBOLS_PER_MARKET` | 60 | 每市场候选池上限 |
| `TREND_NEWS_BOOST` | 0.18 | 新闻强度对 trend_score 的放大系数 |
| `TREND_NEWS_LOOKBACK_DAYS` | max(3, days) | 新闻回溯天数 |
| `TREND_CALC_WORKERS` | 5 | 并发计算线程数 |
| `TREND_LLM_TIMEOUT_SEC` | 180 | LLM总结超时 |
| `TREND_LLM_SKIP_RSS_MB` | 1500 | 内存保护阈值 |
