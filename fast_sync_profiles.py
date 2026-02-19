#!/usr/bin/env python3
"""
快速同步公司简介 - 多线程版本
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tqdm import tqdm

from core.db import db_manager
from domain.meta import Asset
from modules.ingestion.sync_profile import profile_syncer
from sqlmodel import select

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def sync_one(symbol: str, market: str) -> tuple[str, bool]:
    """同步单个股票"""
    try:
        result = profile_syncer.sync_profile(symbol, market=market)
        return (symbol, result is not None)
    except Exception as e:
        logger.error(f"Error syncing {symbol}: {e}")
        return (symbol, False)


def fast_sync_profiles(
    market: str = "CN",
    max_workers: int = 10,
    limit: int = None,
):
    """
    快速同步公司简介
    
    Args:
        market: 市场代码
        max_workers: 线程数（建议 5-20）
        limit: 限制数量
    """
    print(f"=" * 60)
    print(f"快速同步公司简介 - {market} 市场")
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
        # 提交任务
        futures = {
            executor.submit(sync_one, symbol, asset_market): symbol
            for symbol, asset_market in assets_data
        }
        
        # 进度条
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
    
    parser = argparse.ArgumentParser(description="快速同步公司简介")
    parser.add_argument("--market", default="CN", help="市场代码: CN, HK, US")
    parser.add_argument("--workers", type=int, default=10, help="线程数 (5-20)")
    parser.add_argument("--limit", type=int, default=None, help="限制数量")
    
    args = parser.parse_args()
    
    fast_sync_profiles(
        market=args.market,
        max_workers=args.workers,
        limit=args.limit,
    )
