#!/usr/bin/env python3
"""
快速同步公司简介 - 多线程版本
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tqdm import tqdm

from core.db import db_manager
from domain.meta import Asset, AssetProfile
from modules.ingestion.sync_profile import profile_syncer
from sqlmodel import select

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


import time
import random

def sync_one(symbol: str, market: str, skip_existing: bool = True) -> tuple[str, bool]:
    """同步单个股票"""
    try:
        if skip_existing:
             with db_manager.meta_session() as session:
                existing = session.get(AssetProfile, symbol)
                # If we have main business or company profile, consider it done
                if existing and (existing.main_business or existing.company_profile):
                    return (symbol, True)

        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(0.5, 2.0))
        result = profile_syncer.sync_profile(symbol, market=market)
        return (symbol, result is not None)
    except Exception as e:
        logger.error(f"Error syncing {symbol}: {e}")
        return (symbol, False)


def fast_sync_profiles(
    market: str = "CN",
    max_workers: int = 5,
    limit: int = None,
    skip_existing: bool = True,
):
    """
    快速同步公司简介
    
    Args:
        market: 市场代码
        max_workers: 线程数（建议 3-8，防止 IP 被封）
        limit: 限制数量
        skip_existing: 是否跳过已存在的
    """
    print(f"=" * 60)
    print(f"快速同步公司简介 - {market} 市场")
    print(f"线程数: {max_workers}")
    print(f"跳过已存在: {skip_existing}")
    print(f"=" * 60)
    
    # 获取待同步的股票
    with db_manager.meta_session() as session:
        # Check if profile already exists to skip
        # We can do this efficiently by checking AssetProfile table first
        # But for simplicity, let's just get all assets and let sync_profile handle or check.
        # Actually sync_profile does check but it's better to filter here if possible.
        # However, sync_profile logic is: check if exists, if exists update. 
        # The user wants to "brush into profile library", implying filling missing or updating.
        # Let's stick to existing logic but filter out if we want to be faster.
        # For now, just get all assets.
        statement = select(Asset.symbol, Asset.market).where(Asset.market == market)
        assets_data = list(session.exec(statement).all())
    
    if limit:
        assets_data = assets_data[:limit]
    
    total = len(assets_data)
    print(f"\n待同步: {total} 条")
    
    # 多线程同步
    success_count = 0
    error_count = 0
    
    # Use a smaller number of workers to be safe
    actual_workers = min(max_workers, 8) 
    
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        # 提交任务
        future_to_symbol = {
            executor.submit(sync_one, symbol, asset_market, skip_existing): symbol
            for symbol, asset_market in assets_data
        }
        
        # 进度条
        with tqdm(total=total, desc=f"同步进度 ({market})") as pbar:
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    _, success = future.result()
                    if success:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as exc:
                    logger.error(f"{symbol} generated an exception: {exc}")
                    error_count += 1
                
                pbar.update(1)
                pbar.set_postfix({
                    "成功": success_count,
                    "失败": error_count
                })
    
    print(f"\n" + "=" * 60)
    print(f"同步完成 ({market})!")
    print(f"成功: {success_count}")
    print(f"失败: {error_count}")
    print(f"=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="快速同步公司简介")
    parser.add_argument("--market", default="CN", help="市场代码: CN, HK, US")
    parser.add_argument("--workers", type=int, default=10, help="线程数 (5-20)")
    parser.add_argument("--limit", type=int, default=None, help="限制数量")
    parser.add_argument("--skip-existing", action="store_true", help="跳过已存在的")
    
    args = parser.parse_args()
    
    fast_sync_profiles(
        market=args.market,
        max_workers=args.workers,
        limit=args.limit,
        skip_existing=args.skip_existing,
    )
