#!/bin/bash
# 全市场快速同步（两步法）
# 第一步：快速同步到数据库（多线程）
# 第二步：批量向量化（单线程）

echo "=========================================="
echo "  全市场公司简介快速同步"
echo "=========================================="
echo ""

echo "第一步：快速同步到数据库（不向量化）"
echo "=========================================="

# CN 市场
echo "[1/3] 同步 CN 市场 (A股) - 5,484 条..."
python sync_without_vector.py --market CN --workers 20

echo ""
echo "[2/3] 同步 HK 市场 (港股) - 4,624 条..."
python sync_without_vector.py --market HK --workers 20

echo ""
echo "[3/3] 同步 US 市场 (美股) - 12,968 条..."
python sync_without_vector.py --market US --workers 20

echo ""
echo "第二步：批量向量化"
echo "=========================================="
python vectorize_all.py

echo ""
echo "=========================================="
echo "  全市场同步完成！"
echo "=========================================="
echo ""

# 显示统计
python main.py stats
