#!/bin/bash
# 全市场公司简介同步脚本

echo "=========================================="
echo "  全市场公司简介同步"
echo "=========================================="
echo ""

# CN 市场
echo "[1/3] 同步 CN 市场 (A股)..."
python fast_sync_profiles.py --market CN --workers 15

echo ""
echo "[2/3] 同步 HK 市场 (港股)..."
python fast_sync_profiles.py --market HK --workers 15

echo ""
echo "[3/3] 同步 US 市场 (美股)..."
python fast_sync_profiles.py --market US --workers 15

echo ""
echo "=========================================="
echo "  全市场同步完成！"
echo "=========================================="
echo ""

# 显示统计
python main.py stats
