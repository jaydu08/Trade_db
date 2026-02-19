# Phase 3 & 4 开发完成

## ✅ 已完成功能

### Phase 3: 实时探测模块

**文件：** `modules/probing/market.py`

**功能：**
- ✅ 实时行情获取（带 60 秒缓存）
- ✅ 批量行情查询
- ✅ 涨停/跌停/停牌检测
- ✅ 流动性过滤（成交额、换手率）
- ✅ 可交易状态过滤

**API：**
```python
from modules.probing import (
    get_realtime_quote,      # 单个股票行情
    get_batch_quotes,        # 批量行情
    filter_by_liquidity,     # 流动性过滤
    check_trading_status,    # 交易状态检查
)

# 示例
quote = get_realtime_quote("000001")
print(quote["price"], quote["change_pct"])
```

### Phase 4: 产业链挖掘策略

**文件：** `strategies/chain_mining.py`

**核心流程：**
1. **LLM 拆解** - 将产业拆解为上中下游 + 关键词
2. **向量映射** - 在 ChromaDB 中检索匹配公司
3. **行情过滤** - 过滤涨跌停、低流动性标的
4. **信号生成** - 生成交易信号并保存到 Ledger

**CLI 命令：**
```bash
# 基础用法
python main.py strategy chain "AI眼镜"

# 自定义参数
python main.py strategy chain "新能源汽车" \
  --top-k 15 \
  --min-amount 50000000 \
  --threshold 0.7

# 测试模式（不保存信号）
python main.py strategy chain "半导体" --no-save
```

**Python API：**
```python
from strategies.chain_mining import mine_industry_chain

result = mine_industry_chain(
    industry="AI眼镜",
    top_k=10,
    min_amount=10_000_000,
    strength_threshold=0.6,
)

print(f"生成 {result['signals_generated']} 个信号")
for stock in result['stocks']:
    print(f"{stock['symbol']} {stock['name']}")
```

### LLM 集成

**文件：** `core/llm.py`

**支持的 API：**
- OpenAI（gpt-4o-mini）
- DeepSeek（最便宜，¥1/1M tokens）
- 阿里通义千问
- 任何 OpenAI 兼容接口

**配置方式：**
```bash
# 1. 复制配置模板
cp .env.example .env

# 2. 编辑 .env 文件
vim .env

# 3. 测试配置
python test_llm.py
```

详见：`SETUP_LLM.md`

## 📁 新增文件

```
core/
  llm.py                    # LLM 客户端封装

modules/
  probing/
    __init__.py
    market.py               # 实时行情探测器

strategies/
  chain_mining.py           # 产业链挖掘策略

.env.example                # 环境变量模板
SETUP_LLM.md               # LLM 配置指南
test_llm.py                # LLM 测试脚本
```

## 🎯 使用流程

### 1. 配置 LLM（必需）

```bash
# 创建 .env 文件
cp .env.example .env

# 编辑并添加 API Key
# 推荐使用 DeepSeek（便宜）或 OpenAI（效果好）
vim .env

# 测试配置
python test_llm.py
```

### 2. 等待数据同步完成

```bash
# 检查同步进度
bash check_sync_status.sh

# 或查看统计
python main.py stats
```

### 3. 运行产业链挖掘

```bash
# 示例：AI 眼镜产业链
python main.py strategy chain "AI眼镜"

# 示例：新能源汽车
python main.py strategy chain "新能源汽车"

# 示例：半导体
python main.py strategy chain "半导体设备"
```

### 4. 查看生成的信号

```bash
# 查看统计
python main.py stats

# 查询数据库
sqlite3 data/ledger.db "
SELECT 
  symbol, 
  direction, 
  strength, 
  substr(reasoning, 1, 100) as reasoning_preview,
  timestamp
FROM signal 
ORDER BY timestamp DESC 
LIMIT 10;
"
```

## 💡 工作原理示例

**输入：** "AI眼镜"

**步骤 1 - LLM 拆解：**
```
上游：光波导、Micro-LED、衍射光学元件
中游：模组组装、光学设计、系统集成
下游：品牌商、渠道商
```

**步骤 2 - 向量检索：**
```
"光波导" → 水晶光电(002273)、蓝特光学(688127)
"Micro-LED" → 三安光电(600703)、华灿光电(300323)
...
```

**步骤 3 - 行情过滤：**
```
过滤掉：
- 涨停/跌停
- 成交额 < 1000万
- 换手率 < 0.5%
```

**步骤 4 - 生成信号：**
```
Signal(
  symbol="002273",
  direction="LONG",
  strength=0.85,
  reasoning="匹配关键词: 光波导, 产业链位置: 上游..."
)
```

## 🔧 参数调优

### top_k（每个关键词返回结果数）
- 默认：10
- 建议：5-20
- 影响：越大覆盖越广，但噪音越多

### min_amount（最小成交额）
- 默认：10,000,000（1000万）
- 建议：5,000,000 - 50,000,000
- 影响：流动性要求

### strength_threshold（信号强度阈值）
- 默认：0.6
- 建议：0.5 - 0.8
- 影响：信号数量和质量

## 📊 成本估算

### LLM 调用成本（DeepSeek）
- 单次产业链挖掘：约 ¥0.001（不到1分钱）
- 每天 100 次：约 ¥0.1/天
- 每月：约 ¥3/月

### AkShare 调用
- 免费，但有频率限制
- 已实现缓存机制（60秒）

## 🚀 下一步

### Phase 5: 辅助功能（可选）
- [ ] 同业对标组（Peer Group）
- [ ] 字段映射表（Field Mapping）
- [ ] 市场事件库（可选）

### 策略优化
- [ ] 多因子评分（匹配度 + 技术指标 + 基本面）
- [ ] 回测框架
- [ ] 风险控制（仓位管理、止损）

### 生产化
- [ ] 定时任务（每日自动挖掘）
- [ ] 信号推送（邮件/微信/钉钉）
- [ ] Web 界面

## ❓ 常见问题

### Q: 没有 LLM API 怎么办？
A: 可以手动创建产业链知识库，或使用预定义模板。详见 `docs/manual_chain_setup.md`

### Q: 向量库为空怎么办？
A: 等待 profile 同步完成。运行 `bash check_sync_status.sh` 查看进度。

### Q: 生成的信号质量不高？
A: 调整参数：
1. 提高 `strength_threshold`（如 0.7）
2. 增加 `min_amount`（如 50,000,000）
3. 优化 LLM prompt（编辑 `strategies/chain_mining.py`）

### Q: 如何批量运行多个产业？
A: 创建脚本：
```bash
#!/bin/bash
for industry in "AI眼镜" "新能源汽车" "半导体设备"; do
  python main.py strategy chain "$industry"
done
```

## 📝 总结

Phase 3 和 4 已完成，核心功能包括：
- ✅ 实时行情探测和过滤
- ✅ LLM 驱动的产业链拆解
- ✅ 向量检索映射
- ✅ 自动信号生成

现在可以开始使用产业链挖掘策略了！记得先配置 LLM API。
