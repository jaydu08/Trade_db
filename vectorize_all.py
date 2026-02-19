#!/usr/bin/env python3
"""
批量向量化已有的公司简介
单线程，避免 ChromaDB 并发问题
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime
from tqdm import tqdm

from core.db import db_manager, get_collection
from domain.meta import AssetProfile, Asset
from sqlmodel import select


def vectorize_all_profiles(limit: int = None):
    """批量向量化所有 profile"""
    print("=" * 60)
    print("批量向量化公司简介")
    print("=" * 60)
    
    # 获取所有有内容的 profile
    with db_manager.meta_session() as session:
        statement = select(AssetProfile, Asset).join(Asset).where(
            AssetProfile.main_business != None,
            AssetProfile.main_business != "",
        )
        results = list(session.exec(statement).all())
    
    if limit:
        results = results[:limit]
    
    total = len(results)
    print(f"\n待向量化: {total} 条")
    
    # 获取 collection
    collection = get_collection('company_chunks')
    
    success_count = 0
    error_count = 0
    
    for profile, asset in tqdm(results, desc="向量化进度"):
        try:
            symbol = profile.symbol
            name = asset.name
            market = asset.market
            main_business = profile.main_business or ""
            company_profile = profile.company_profile or ""
            
            now = datetime.utcnow().isoformat()
            chunks_to_add = []
            
            # 公司简介 chunk
            if company_profile and len(company_profile) > 10:
                chunks_to_add.append({
                    "id": f"{symbol}_overview_v1",
                    "document": f"{name}: {company_profile}",
                    "metadata": {
                        "symbol": symbol,
                        "name": name,
                        "market": market,
                        "chunk_type": "overview",
                        "source": "akshare",
                        "confidence": 1.0,
                        "doc_version": 1,
                        "updated_at": now,
                    }
                })
            
            # 主营业务 chunk
            if main_business and len(main_business) > 10:
                chunks_to_add.append({
                    "id": f"{symbol}_business_v1",
                    "document": f"{name} 主营业务: {main_business}",
                    "metadata": {
                        "symbol": symbol,
                        "name": name,
                        "market": market,
                        "chunk_type": "business",
                        "source": "akshare",
                        "confidence": 1.0,
                        "doc_version": 1,
                        "updated_at": now,
                    }
                })
            
            if chunks_to_add:
                ids = [c["id"] for c in chunks_to_add]
                documents = [c["document"] for c in chunks_to_add]
                metadatas = [c["metadata"] for c in chunks_to_add]
                
                # 先删除旧的
                try:
                    collection.delete(ids=ids)
                except:
                    pass
                
                # 添加新的
                collection.add(
                    ids=ids,
                    documents=documents,
                    metadatas=metadatas,
                )
                
                success_count += 1
        
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # 只打印前5个错误
                print(f"\nError vectorizing {symbol}: {e}")
    
    print(f"\n" + "=" * 60)
    print(f"向量化完成!")
    print(f"成功: {success_count}")
    print(f"失败: {error_count}")
    print(f"向量库总数: {collection.count()}")
    print(f"=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="批量向量化公司简介")
    parser.add_argument("--limit", type=int, default=None, help="限制数量")
    
    args = parser.parse_args()
    
    vectorize_all_profiles(limit=args.limit)
