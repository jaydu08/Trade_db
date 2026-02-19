#!/bin/bash
echo "========================================"
echo "    📊 Trade_db 数据库完整概览"
echo "========================================"
echo ""

echo "💾 数据库文件信息:"
echo "----------------------------------------"
ls -lh data/*.db
echo ""

echo "📋 Meta数据库表统计:"
echo "----------------------------------------"
for table in $(sqlite3 data/meta.db ".tables"); do 
    count=$(sqlite3 data/meta.db "SELECT COUNT(*) FROM [$table];" 2>/dev/null || echo "0")
    printf "%-20s: %s 条记录\n" "$table" "$count"
done
echo ""

echo "📋 Ledger数据库表统计:"
echo "----------------------------------------"
for table in order signal strategy position signal_ext strategy_run; do 
    count=$(sqlite3 data/ledger.db "SELECT COUNT(*) FROM [$table];" 2>/dev/null || echo "0")
    printf "%-20s: %s 条记录\n" "$table" "$count"
done
echo ""

echo "📈 数据分布详情:"
echo "----------------------------------------"
echo "A股市场 (CN): $(sqlite3 data/meta.db "SELECT COUNT(*) FROM asset WHERE market='CN';") 条记录"
echo "港股市场 (HK): $(sqlite3 data/meta.db "SELECT COUNT(*) FROM asset WHERE market='HK';") 条记录"  
echo "美股市场 (US): $(sqlite3 data/meta.db "SELECT COUNT(*) FROM asset WHERE market='US';") 条记录"
echo ""

echo "📊 概念板块: $(sqlite3 data/meta.db "SELECT COUNT(*) FROM concept;") 个"
echo "📊 行业分类: $(sqlite3 data/meta.db "SELECT COUNT(*) FROM industry;") 个"
echo "📊 同步日志: $(sqlite3 data/meta.db "SELECT COUNT(*) FROM data_sync_log;") 条"
echo ""

echo "✅ 数据库概览完成!"