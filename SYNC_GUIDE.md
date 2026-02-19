# 📊 数据同步完整指南

## 🚀 快速开始（推荐流程）

### 步骤 1: 快速同步数据库（不向量化）

```bash
# 方式 A: 逐个市场同步
python sync_without_vector.py --market CN --workers 20
python sync_without_vector.py --market HK --workers 20
python sync_without_vector.py --market US --workers 20

# 方式 B: 使用脚本一次性同步全市场
chmod +x sync_all_markets_fast.sh
./sync_all_markets_fast.sh
```

**预计时间：**
- CN (5,484 条): ~15 分钟
- HK (4,624 条): ~12 分钟
- US (12,968 条): ~35 分钟
- **总计: ~1 小时**

### 步骤 2: 批量向量化

```bash
# 向量化所有已同步的数据
python vectorize_all.py

# 或者先测试少量数据
python vectorize_all.py --limit 100
```

**预计时间：**
- 23,076 条数据
- 速度: ~50 条/秒
- **总计: ~8 分钟**

### 步骤 3: 验证结果

```bash
# 查看统计
python main.py stats

# 测试搜索
python -c "
from modules.ingestion.sync_profile import profile_syncer
results = profile_syncer.search_companies('银行', n_results=10)
for r in results:
    meta = r['metadata']
    print(f'{meta[\"symbol\"]} {meta[\"name\"]}')
"
```

## 📋 详细说明

### sync_without_vector.py
- **功能**: 多线程快速同步到数据库
- **优点**: 速度快（20线程），无向量化冲突
- **缺点**: 需要后续单独向量化

**参数：**
```bash
--market CN/HK/US    # 市场代码
--workers 20         # 线程数（建议 10-30）
--limit 100          # 限制数量（测试用）
```

### vectorize_all.py
- **功能**: 单线程批量向量化
- **优点**: 稳定，无并发问题
- **缺点**: 单线程（但速度已经很快）

**参数：**
```bash
--limit 100          # 限制数量（测试用）
```

## 🔍 监控进度

### 实时监控
```bash
# 终端 1: 运行同步
python sync_without_vector.py --market CN --workers 20

# 终端 2: 监控进度
watch -n 5 'bash check_sync_status.sh'
```

### 检查数据质量
```bash
# 检查有内容的 profile 数量
sqlite3 data/meta.db "
SELECT 
    market,
    COUNT(*) as total,
    COUNT(CASE WHEN main_business IS NOT NULL AND main_business != '' THEN 1 END) as has_data
FROM asset_profile
JOIN asset ON asset_profile.symbol = asset.symbol
GROUP BY market;
"
```

## ⚡ 性能对比

| 方法 | 速度 | 向量化 | 稳定性 | 推荐 |
|------|------|--------|--------|------|
| 单线程同步 | 0.6条/秒 | ✓ | ★★★★★ | ❌ |
| 多线程同步 (fast_sync_profiles.py) | 1条/秒 | ✓ | ★★★☆☆ | ❌ |
| 多线程同步 (sync_without_vector.py) | 1.5条/秒 | ✗ | ★★★★★ | ✓ |
| 后续向量化 (vectorize_all.py) | 50条/秒 | ✓ | ★★★★★ | ✓ |

**推荐组合**: sync_without_vector.py + vectorize_all.py
- 总时间: ~1小时 + 8分钟 = **1小时8分钟**
- 稳定性: ★★★★★

## 🐛 故障排查

### 问题 1: 同步速度慢
```bash
# 增加线程数
python sync_without_vector.py --market CN --workers 30
```

### 问题 2: 向量化失败
```bash
# 检查 ChromaDB
python -c "
from core.db import get_collection
collection = get_collection('company_chunks')
print(f'向量库数量: {collection.count()}')
"

# 重新向量化
python vectorize_all.py
```

### 问题 3: 数据为空
```bash
# 检查 AkShare API
python -c "
import akshare as ak
df = ak.stock_profile_cninfo(symbol='000001')
print(df)
"
```

## 📊 预期结果

同步完成后应该有：

```
数据库:
- asset_profile: 23,076 条
- 有效数据: ~20,000 条（部分股票可能无数据）

向量库:
- company_chunks: ~40,000 条（每个股票 2 个 chunk）
```

## 🎯 下一步

数据同步完成后：

1. **测试产业链挖掘**
```bash
python main.py strategy chain "AI眼镜"
```

2. **查看生成的信号**
```bash
python main.py stats
```

3. **优化策略参数**
```bash
python main.py strategy chain "新能源汽车" --threshold 0.7 --top-k 20
```

---

**当前推荐执行：**
```bash
# 1. 快速同步全市场（1小时）
python sync_without_vector.py --market CN --workers 20
python sync_without_vector.py --market HK --workers 20
python sync_without_vector.py --market US --workers 20

# 2. 批量向量化（8分钟）
python vectorize_all.py

# 3. 验证
python main.py stats
```
