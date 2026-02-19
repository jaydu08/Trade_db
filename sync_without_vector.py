#!/usr/bin/env python3
"""
快速同步公司简介 - 不向量化版本
先快速同步到数据库，之后再批量向量化
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tqdm import tqdm
import pandas as pd

from core.db import db_manager
from domain.meta import Asset, AssetProfile
from modules.ingestion.akshare_client import akshare_client
from sqlmodel import select

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def sync_one_no_vector(symbol: str, market: str) -> tuple[str, bool]:
    """同步单个股票（不向量化）"""
    try:
        # 获取数据
        if market == "CN":
            try:
                df = akshare_client.get_stock_profile_cninfo(symbol)
                if df.empty:
                    df = akshare_client.get_stock_business(symbol)
            except:
                df = akshare_client.get_stock_business(symbol)
        elif market == "HK":
            df = akshare_client.get_stock_profile_hk(symbol)
        elif market == "US":
            df = akshare_client.get_stock_profile_us(symbol)
        else:
            return (symbol, False)
        
        if df is None or df.empty:
            return (symbol, False)
        
        # 解析数据
        info = {}
        if "公司名称" in df.columns or "机构简介" in df.columns:
            row = df.iloc[0]
            for col in df.columns:
                value = row[col]
                if value and str(value) != "nan" and str(value).strip():
                    info[col] = str(value).strip()
        elif "主营业务" in df.columns:
            for col in df.columns:
                value = df[col].iloc[0] if len(df) > 0 else ""
                if value and str(value) != "nan":
                    info[col] = str(value).strip()
        
        if not info:
            return (symbol, False)
        
        # 提取字段
        main_business = info.get("主营业务", info.get("经营范围", ""))
        business_scope = info.get("经营范围", info.get("主营业务", ""))
        products = info.get("核心产品", info.get("主要产品", info.get("产品名称", "")))
        company_profile = info.get("公司简介", info.get("公司介绍", info.get("机构简介", "")))
        
        # 保存到数据库（不向量化）
        with db_manager.meta_session() as session:
            profile = session.get(AssetProfile, symbol)
            
            if profile:
                profile.main_business = main_business
                profile.business_scope = business_scope or profile.business_scope
                profile.products = products or profile.products
                profile.company_profile = company_profile
                profile.updated_at = datetime.utcnow()
            else:
                profile = AssetProfile(
                    symbol=symbol,
                    main_business=main_business,
                    business_scope=business_scope or None,
                    products=products or None,
                    company_profile=company_profile,
                )
            
            session.add(profile)
        
        return (symbol, True)
    
    except Exception as e:
        logger.error(f"Error syncing {symbol}: {e}")
        return (symbol, False)


def fast_sync_no_vector(
    market: str = "CN",
    max_workers: int = 20,
    limit: int = None,
):
    """快速同步（不向量化）"""
    print(f"=" * 60)
    print(f"快速同步公司简介 (不向量化) - {market} 市场")
    print(f"线程数: {max_workers}")
    print(f"=" * 60)
    
    # 获取待同步的股票
    with db_manager.meta_session() as session:
        statement = select(Asset.symbol, Asset.market).where(Asset.market == market)
        assets_data = list(session.exec(statement).all())
    
    if limit:
        assets_data = assets_data[:limit]
    
    total = len(assets_data)
    print(f"\n待同步: {total} 条")
    
    # 多线程同步
    success_count = 0
    error_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(sync_one_no_vector, symbol, asset_market): symbol
            for symbol, asset_market in assets_data
        }
        
        with tqdm(total=total, desc="同步进度") as pbar:
            for future in as_completed(futures):
                symbol, success = future.result()
                if success:
                    success_count += 1
                else:
                    error_count += 1
                pbar.update(1)
                pbar.set_postfix({
                    "成功": success_count,
                    "失败": error_count
                })
    
    print(f"\n" + "=" * 60)
    print(f"同步完成!")
    print(f"成功: {success_count}")
    print(f"失败: {error_count}")
    print(f"=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="快速同步公司简介（不向量化）")
    parser.add_argument("--market", default="CN", help="市场代码: CN, HK, US")
    parser.add_argument("--workers", type=int, default=20, help="线程数")
    parser.add_argument("--limit", type=int, default=None, help="限制数量")
    
    args = parser.parse_args()
    
    fast_sync_no_vector(
        market=args.market,
        max_workers=args.workers,
        limit=args.limit,
    )
