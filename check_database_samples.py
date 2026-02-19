#!/usr/bin/env python3
"""
数据库抽样检查脚本
随机抽取每个市场的样本数据，生成 Markdown 报告
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import random
from datetime import datetime
from sqlmodel import select

from core.db import db_manager, get_collection
from domain.meta import Asset, AssetProfile, Concept, Industry, AssetConceptLink, AssetIndustryLink


def generate_report():
    """生成数据库检查报告"""
    
    report = []
    report.append("# 📊 AlphaBase 数据库抽样检查报告")
    report.append(f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("\n---\n")
    
    # ============================================================
    # 1. 总体统计
    # ============================================================
    report.append("## 1. 总体统计\n")
    
    with db_manager.meta_session() as session:
        asset_count = len(list(session.exec(select(Asset)).all()))
        profile_count = len(list(session.exec(select(AssetProfile)).all()))
        concept_count = len(list(session.exec(select(Concept)).all()))
        industry_count = len(list(session.exec(select(Industry)).all()))
        concept_link_count = len(list(session.exec(select(AssetConceptLink)).all()))
        industry_link_count = len(list(session.exec(select(AssetIndustryLink)).all()))
        
        # 有内容的 profile
        profile_with_content = len(list(session.exec(
            select(AssetProfile).where(
                AssetProfile.main_business != None,
                AssetProfile.main_business != ""
            )
        ).all()))
    
    report.append("| 数据表 | 记录数 | 说明 |")
    report.append("|--------|--------|------|")
    report.append(f"| asset | {asset_count:,} | 股票列表 |")
    report.append(f"| asset_profile | {profile_count:,} | 公司简介 |")
    report.append(f"| └─ 有内容 | {profile_with_content:,} | 实际有数据的 |")
    report.append(f"| concept | {concept_count:,} | 概念板块 |")
    report.append(f"| industry | {industry_count:,} | 行业分类 |")
    report.append(f"| asset_concept_link | {concept_link_count:,} | 概念关联 |")
    report.append(f"| asset_industry_link | {industry_link_count:,} | 行业关联 |")
    
    # 向量库统计
    try:
        collection = get_collection('company_chunks')
        vector_count = collection.count()
        report.append(f"| ChromaDB (向量库) | {vector_count:,} | 向量化文档 |")
    except Exception as e:
        report.append(f"| ChromaDB (向量库) | ERROR | {str(e)[:50]} |")
    
    # Ledger DB 统计
    try:
        with db_manager.ledger_session() as session:
            from domain.ledger import Signal
            signal_count = len(list(session.exec(select(Signal)).all()))
            report.append(f"| signal (交易信号) | {signal_count:,} | 生成的信号 |")
    except Exception as e:
        report.append(f"| signal (交易信号) | ERROR | {str(e)[:30]} |")
    
    report.append("\n")
    
    # ============================================================
    # 2. 市场分布
    # ============================================================
    report.append("## 2. 市场分布\n")
    
    with db_manager.meta_session() as session:
        for market in ["CN", "HK", "US"]:
            count = len(list(session.exec(select(Asset).where(Asset.market == market)).all()))
            profile_count = len(list(session.exec(
                select(AssetProfile).join(Asset).where(
                    Asset.market == market,
                    AssetProfile.main_business != None,
                    AssetProfile.main_business != ""
                )
            ).all()))
            report.append(f"- **{market}**: {count:,} 只股票, {profile_count:,} 条简介")
    
    report.append("\n")
    
    # ============================================================
    # 3. 随机抽样 - CN 市场
    # ============================================================
    report.append("## 3. 随机抽样 - CN 市场 (A股)\n")
    
    with db_manager.meta_session() as session:
        cn_assets = list(session.exec(
            select(Asset).where(Asset.market == "CN")
        ).all())
        
        if cn_assets:
            samples = random.sample(cn_assets, min(5, len(cn_assets)))
            
            for i, asset in enumerate(samples, 1):
                report.append(f"### 3.{i} {asset.symbol} - {asset.name}\n")
                report.append(f"- **代码**: {asset.symbol}")
                report.append(f"- **名称**: {asset.name}")
                report.append(f"- **市场**: {asset.market}")
                report.append(f"- **状态**: {asset.listing_status}")
                
                # 查询 profile
                profile = session.get(AssetProfile, asset.symbol)
                if profile:
                    mb = profile.main_business[:150] if profile.main_business else '无'
                    cp = profile.company_profile[:150] if profile.company_profile else '无'
                    report.append(f"- **主营业务**: {mb}{'...' if len(profile.main_business or '') > 150 else ''}")
                    report.append(f"- **公司简介**: {cp}{'...' if len(profile.company_profile or '') > 150 else ''}")
                else:
                    report.append("- **简介**: 未同步")
                
                # 查询概念
                concept_links = list(session.exec(
                    select(AssetConceptLink).where(AssetConceptLink.symbol == asset.symbol)
                ).all())
                if concept_links:
                    concepts = [session.get(Concept, link.concept_code).name for link in concept_links[:5]]
                    report.append(f"- **概念**: {', '.join(concepts)}")
                
                # 查询行业
                industry_links = list(session.exec(
                    select(AssetIndustryLink).where(AssetIndustryLink.symbol == asset.symbol)
                ).all())
                if industry_links:
                    industries = [session.get(Industry, link.industry_code).name for link in industry_links[:3]]
                    report.append(f"- **行业**: {', '.join(industries)}")
                
                report.append("\n")
    
    # ============================================================
    # 4. 随机抽样 - HK 市场
    # ============================================================
    report.append("## 4. 随机抽样 - HK 市场 (港股)\n")
    
    with db_manager.meta_session() as session:
        hk_assets = list(session.exec(
            select(Asset).where(Asset.market == "HK")
        ).all())
        
        if hk_assets:
            samples = random.sample(hk_assets, min(5, len(hk_assets)))
            
            for i, asset in enumerate(samples, 1):
                report.append(f"### 4.{i} {asset.symbol} - {asset.name}\n")
                report.append(f"- **代码**: {asset.symbol}")
                report.append(f"- **名称**: {asset.name}")
                report.append(f"- **市场**: {asset.market}")
                
                profile = session.get(AssetProfile, asset.symbol)
                if profile and profile.main_business:
                    mb = profile.main_business[:150]
                    report.append(f"- **主营业务**: {mb}{'...' if len(profile.main_business) > 150 else ''}")
                    if profile.company_profile:
                        cp = profile.company_profile[:150]
                        report.append(f"- **公司简介**: {cp}{'...' if len(profile.company_profile) > 150 else ''}")
                else:
                    report.append("- **简介**: 未同步或无数据")
                
                report.append("\n")
    
    # ============================================================
    # 5. 随机抽样 - US 市场
    # ============================================================
    report.append("## 5. 随机抽样 - US 市场 (美股)\n")
    
    with db_manager.meta_session() as session:
        us_assets = list(session.exec(
            select(Asset).where(Asset.market == "US")
        ).all())
        
        if us_assets:
            samples = random.sample(us_assets, min(5, len(us_assets)))
            
            for i, asset in enumerate(samples, 1):
                report.append(f"### 5.{i} {asset.symbol} - {asset.name}\n")
                report.append(f"- **代码**: {asset.symbol}")
                report.append(f"- **名称**: {asset.name}")
                report.append(f"- **市场**: {asset.market}")
                
                profile = session.get(AssetProfile, asset.symbol)
                if profile and profile.main_business:
                    mb = profile.main_business[:150]
                    report.append(f"- **主营业务**: {mb}{'...' if len(profile.main_business) > 150 else ''}")
                    if profile.company_profile:
                        cp = profile.company_profile[:150]
                        report.append(f"- **公司简介**: {cp}{'...' if len(profile.company_profile) > 150 else ''}")
                else:
                    report.append("- **简介**: 未同步或无数据")
                
                report.append("\n")
    
    # ============================================================
    # 6. 概念板块抽样
    # ============================================================
    report.append("## 6. 概念板块抽样\n")
    
    with db_manager.meta_session() as session:
        concepts = list(session.exec(select(Concept)).all())
        
        if concepts:
            samples = random.sample(concepts, min(3, len(concepts)))
            
            for i, concept in enumerate(samples, 1):
                report.append(f"### 6.{i} {concept.name}\n")
                report.append(f"- **代码**: {concept.code}")
                report.append(f"- **名称**: {concept.name}")
                
                # 统计成分股
                links = list(session.exec(
                    select(AssetConceptLink).where(AssetConceptLink.concept_code == concept.code)
                ).all())
                report.append(f"- **成分股数量**: {len(links)}")
                
                if links:
                    stock_names = []
                    for link in links[:5]:
                        asset = session.get(Asset, link.symbol)
                        if asset:
                            stock_names.append(f"{asset.symbol}({asset.name})")
                    report.append(f"- **示例成分股**: {', '.join(stock_names)}")
                
                report.append("\n")
    
    # ============================================================
    # 7. 向量库抽样
    # ============================================================
    report.append("## 7. 向量库抽样\n")
    
    try:
        collection = get_collection('company_chunks')
        count = collection.count()
        
        if count > 0:
            # 随机获取一些数据
            results = collection.get(limit=min(5, count))
            
            report.append(f"向量库共有 {count:,} 条文档\n")
            
            for i, (doc_id, doc, metadata) in enumerate(zip(results['ids'], results['documents'], results['metadatas']), 1):
                report.append(f"### 7.{i} {metadata['symbol']} - {metadata['name']}\n")
                report.append(f"- **文档ID**: {doc_id}")
                report.append(f"- **类型**: {metadata['chunk_type']}")
                report.append(f"- **市场**: {metadata['market']}")
                report.append(f"- **内容**: {doc[:150]}...")
                report.append("\n")
        else:
            report.append("向量库为空，需要运行向量化脚本\n")
    
    except Exception as e:
        report.append(f"向量库检查失败: {e}\n")
    
    # ============================================================
    # 8. 交易信号抽样
    # ============================================================
    report.append("## 8. 交易信号抽样\n")
    
    try:
        with db_manager.ledger_session() as session:
            from domain.ledger import Signal
            signals = list(session.exec(select(Signal).order_by(Signal.timestamp.desc())).all())
            
            if signals:
                samples = signals[:3]
                
                for i, signal in enumerate(samples, 1):
                    report.append(f"### 8.{i} {signal.symbol} - {signal.strategy}\n")
                    report.append(f"- **股票代码**: {signal.symbol}")
                    report.append(f"- **策略**: {signal.strategy}")
                    report.append(f"- **方向**: {signal.direction}")
                    report.append(f"- **强度**: {signal.strength:.2f}")
                    report.append(f"- **状态**: {signal.status}")
                    report.append(f"- **时间**: {signal.timestamp}")
                    report.append(f"- **推理**: {signal.reasoning[:200]}...")
                    report.append("\n")
            else:
                report.append("暂无交易信号\n")
    except Exception as e:
        report.append(f"交易信号检查失败: {e}\n")
    
    # ============================================================
    # 9. 数据质量评估
    # ============================================================
    report.append("## 9. 数据质量评估\n")
    
    with db_manager.meta_session() as session:
        total_assets = len(list(session.exec(select(Asset)).all()))
        profiles_with_content = len(list(session.exec(
            select(AssetProfile).where(
                AssetProfile.main_business != None,
                AssetProfile.main_business != ""
            )
        ).all()))
        
        coverage = (profiles_with_content / total_assets * 100) if total_assets > 0 else 0
        
        report.append(f"- **Profile 覆盖率**: {coverage:.1f}% ({profiles_with_content:,}/{total_assets:,})")
        
        if coverage < 50:
            report.append("- **状态**: ⚠️ 需要继续同步")
            report.append("- **建议**: 运行 `python sync_without_vector.py --market CN --workers 20`")
        elif coverage < 90:
            report.append("- **状态**: 🔄 同步进行中")
        else:
            report.append("- **状态**: ✅ 同步完成")
            report.append("- **下一步**: 运行 `python vectorize_all.py` 进行向量化")
    
    report.append("\n---\n")
    report.append("\n*报告生成完毕*")
    
    return "\n".join(report)


if __name__ == "__main__":
    print("正在生成数据库检查报告...")
    report = generate_report()
    
    # 保存到文件
    output_file = "DATABASE_SAMPLE_REPORT.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report)
    
    print(f"✓ 报告已生成: {output_file}")
    print("\n" + "=" * 60)
    print(report)
