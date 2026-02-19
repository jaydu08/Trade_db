#!/bin/bash
# 快速同步全市场 - 不向量化版本

echo "=========================================="
echo "  快速同步全市场公司简介"
echo "  方式: 多线程同步 + 后续批量向量化"
echo "=========================================="
echo ""

START_TIME=$(date +%s)

# CN 市场
echo "[1/3] 同步 CN 市场 (A股) - 5,484 条..."
python sync_without_vector.py --market CN --workers 20

# HK 市场
echo ""
echo "[2/3] 同步 HK 市场 (港股) - 4,624 条..."
python sync_without_vector.py --market HK --workers 20

# US 市场
echo ""
echo "[3/3] 同步 US 市场 (美股) - 12,968 条..."
python sync_without_vector.py --market US --workers 20

# 批量向量化
echo ""
echo "[4/4] 批量向量化所有数据..."
python vectorize_all.py

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo "=========================================="
echo "  全市场同步完成！"
echo "  总耗时: ${MINUTES}分${SECONDS}秒"
echo "=========================================="
echo ""

# 显示统计
python main.py stats
