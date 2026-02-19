#!/bin/bash
# 检查数据同步状态

echo "========================================"
echo "    📊 数据同步进度检查"
echo "========================================"
echo ""

# 检查 profile 进度
profile_count=$(sqlite3 data/meta.db "SELECT COUNT(*) FROM asset_profile;")
total_assets=$(sqlite3 data/meta.db "SELECT COUNT(*) FROM asset;")
profile_percent=$(echo "scale=2; $profile_count * 100 / $total_assets" | bc)

echo "📝 公司简介 (asset_profile):"
echo "   进度: $profile_count / $total_assets ($profile_percent%)"
echo ""

# 检查概念关联
concept_link_count=$(sqlite3 data/meta.db "SELECT COUNT(*) FROM asset_concept_link;")
echo "🔗 概念关联 (asset_concept_link): $concept_link_count 条"
echo ""

# 检查行业关联
industry_link_count=$(sqlite3 data/meta.db "SELECT COUNT(*) FROM asset_industry_link;")
echo "🔗 行业关联 (asset_industry_link): $industry_link_count 条"
echo ""

# 检查向量库
vector_count=$(sqlite3 data/vector_store/chroma.sqlite3 "SELECT COUNT(*) FROM embeddings;" 2>/dev/null || echo "0")
echo "🧠 向量库 (ChromaDB): $vector_count 条"
echo ""

# 检查是否完成
if [ "$profile_count" -eq "$total_assets" ]; then
    echo "✅ 公司简介同步已完成！"
else
    remaining=$((total_assets - profile_count))
    echo "⏳ 还需同步 $remaining 条公司简介"
fi

echo ""
echo "========================================"
