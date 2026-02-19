# 📊 AlphaBase 当前状态与下一步计划

## ✅ 已完成功能

### 1. 数据层（Meta DB）
- ✅ 股票列表：23,076 条（CN + HK + US）
- ✅ 概念板块：466 个
- ✅ 行业分类：498 个
- ✅ 概念关联：58,679 条
- ✅ 行业关联：16,452 条
- 🔄 公司简介：5,293 条（23% 完成，后台同步中）

### 2. 向量知识库（ChromaDB）
- ✅ 向量化功能正常
- 🔄 数据量：2 条（等待 profile 同步完成）
- ✅ 语义搜索功能正常

### 3. LLM 集成
- ✅ 火山引擎 DeepSeek V3 配置成功
- ✅ 结构化输出功能正常
- ✅ 成本：约 ¥0.001/次

### 4. 实时探测模块（Phase 3）
- ✅ 实时行情获取
- ✅ 涨跌停/停牌检测
- ✅ 流动性过滤
- ✅ 批量查询

### 5. 产业链挖掘策略（Phase 4）
- ✅ LLM 产业链拆解
- ✅ 向量映射引擎
- ✅ 行情过滤
- ✅ 信号生成
- ⏳ 等待向量库数据充足后可用

## 📈 当前数据同步进度

```bash
# 查看进度
bash check_sync_status.sh

# 当前状态（预计）
公司简介: 5,293 / 23,076 (23%)
向量库: 随 profile 同步自动增长
预计完成时间: 6-8 小时
```

## 🎯 你现在主要需要什么

### 立即可做：

**1. 等待数据同步完成**
- 后台进程正在运行，无需干预
- 定期检查：`bash check_sync_status.sh`

**2. 测试实时行情功能**
```bash
# 测试单个股票
python -c "
from modules.probing import get_realtime_quote
quote = get_realtime_quote('000001')
print(f'{quote[\"name\"]}: {quote[\"price\"]} ({quote[\"change_pct\"]}%)')
"
```

**3. 测试 LLM 产业链拆解**
```bash
python -c "
from strategies.chain_mining import chain_mining_strategy
chain = chain_mining_strategy.decompose_chain('新能源汽车')
for node in chain.nodes:
    print(f'{node.position}: {node.keywords}')
"
```

### 数据同步完成后：

**1. 运行完整的产业链挖掘**
```bash
# 基础用法
python main.py strategy chain "AI眼镜"

# 查看生成的信号
python main.py stats
sqlite3 data/ledger.db "SELECT * FROM signal ORDER BY timestamp DESC LIMIT 10;"
```

**2. 测试不同产业**
```bash
python main.py strategy chain "新能源汽车"
python main.py strategy chain "半导体设备"
python main.py strategy chain "人工智能"
```

**3. 调优参数**
```bash
# 提高匹配精度
python main.py strategy chain "AI眼镜" --threshold 0.7

# 增加流动性要求
python main.py strategy chain "AI眼镜" --min-amount 50000000

# 更多候选股票
python main.py strategy chain "AI眼镜" --top-k 20
```

## 🔧 可选优化（Phase 5）

### 1. 手动创建产业链知识库
如果想提高产业链挖掘的准确性，可以手动添加产业链知识：

```python
# 示例：添加 AI 眼镜产业链知识
from core.db import get_collection

collection = get_collection('industry_knowledge')
collection.add(
    ids=['ai_glasses_upstream_1'],
    documents=['AI眼镜上游：光波导技术是核心，主要供应商包括水晶光电、蓝特光学'],
    metadatas=[{
        'industry': 'AI眼镜',
        'node': 'upstream',
        'keywords': '光波导,衍射光学',
    }]
)
```

### 2. 定时任务
创建每日自动挖掘脚本：

```bash
# crontab -e
# 每天 9:30 运行
30 9 * * * cd /root/Trade_db && python main.py strategy chain "AI眼镜" >> logs/chain_mining.log 2>&1
```

### 3. 信号推送
添加钉钉/企业微信通知：

```python
# 在 strategies/chain_mining.py 的 save_signals 方法中添加
def save_signals(self, signals):
    # ... 保存逻辑 ...
    
    # 推送通知
    if signals:
        send_notification(f"发现 {len(signals)} 个新信号")
```

### 4. 回测框架
评估策略历史表现：

```python
# 创建 backtest.py
# 1. 读取历史信号
# 2. 获取历史行情
# 3. 计算收益率
# 4. 生成报告
```

## 📊 数据质量检查

### 检查向量库质量
```bash
python -c "
from modules.ingestion.sync_profile import profile_syncer

# 测试搜索
results = profile_syncer.search_companies('光波导', n_results=10)
for r in results:
    meta = r['metadata']
    print(f'{meta[\"symbol\"]} {meta[\"name\"]} - 距离: {r[\"distance\"]:.3f}')
"
```

### 检查信号质量
```bash
sqlite3 data/ledger.db "
SELECT 
    symbol,
    strength,
    substr(reasoning, 1, 100) as reasoning_preview
FROM signal 
WHERE strategy = 'ChainMining_v1'
ORDER BY strength DESC
LIMIT 10;
"
```

## 🐛 故障排查

### 问题：向量库为空
```bash
# 检查 profile 数据
sqlite3 data/meta.db "SELECT COUNT(*) FROM asset_profile WHERE main_business IS NOT NULL AND main_business != '';"

# 手动触发向量化
python -c "
from modules.ingestion.sync_profile import profile_syncer
profile_syncer.sync_profile('000001', market='CN')
"
```

### 问题：LLM 调用失败
```bash
# 测试 LLM
python test_llm.py

# 检查环境变量
cat .env
```

### 问题：行情获取失败
```bash
# 测试 AkShare
python -c "
import akshare as ak
df = ak.stock_zh_a_spot_em()
print(f'获取 {len(df)} 条行情')
"
```

## 📝 开发建议

### 1. 策略优化方向
- 多因子评分（匹配度 + 技术指标 + 基本面）
- 动态阈值（根据市场环境调整）
- 行业轮动（识别热点行业）

### 2. 风险控制
- 仓位管理（单票不超过 X%）
- 止损机制（跌破 Y% 自动平仓）
- 分散投资（同一产业链不超过 Z 个标的）

### 3. 数据增强
- 新闻情绪分析
- 研报摘要提取
- 社交媒体热度

## 🎓 学习资源

### AkShare 文档
- 官方文档：https://akshare.akfamily.xyz/
- GitHub：https://github.com/akfamily/akshare

### 向量数据库
- ChromaDB 文档：https://docs.trychroma.com/

### LLM 应用
- DeepSeek 文档：https://platform.deepseek.com/docs

## 📞 需要帮助？

如果遇到问题，可以：
1. 运行 `python test_all_modules.py` 诊断
2. 查看日志文件
3. 检查数据库状态 `python main.py stats`

---

**当前状态：** ✅ 核心功能已完成，等待数据同步

**下一步：** 等待 profile 同步完成（6-8 小时），然后运行产业链挖掘策略
