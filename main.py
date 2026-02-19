#!/usr/bin/env python3
"""
AlphaBase CLI 入口

Usage:
    python main.py init              # 初始化数据库
    python main.py sync assets       # 同步股票列表
    python main.py sync concepts     # 同步概念板块
    python main.py sync industries   # 同步行业分类
    python main.py sync profiles     # 同步公司简介
    python main.py sync all          # 同步所有数据
    python main.py sync daemon       # 后台循环同步
    python main.py search "光波导"   # 语义搜索
    python main.py stats             # 显示统计信息
"""
import sys
import logging
import time
from pathlib import Path

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent))

import click

from core.db import db_manager
from core.cache import cache_manager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """AlphaBase - AI 原生量化投研系统"""
    pass


@cli.command()
def init():
    """初始化数据库"""
    click.echo("Initializing databases...")
    
    try:
        db_manager.init_all()
        click.echo("✓ All databases initialized successfully!")
    except Exception as e:
        click.echo(f"✗ Initialization failed: {e}", err=True)
        sys.exit(1)


@cli.group()
def sync():
    """同步数据"""
    pass


@sync.command("assets")
@click.option("--markets", type=str, default="CN", help="市场列表: CN,HK,US,ALL")
def sync_assets_cmd(markets: str):
    """同步股票列表"""
    click.echo("Syncing stocks...")
    
    from modules.ingestion import sync_assets_by_markets
    
    try:
        markets_list = _parse_markets(markets)
        result = sync_assets_by_markets(markets_list)
        click.echo(
            f"✓ Synced {result['inserted']} new, {result['updated']} updated"
        )
    except Exception as e:
        click.echo(f"✗ Sync failed: {e}", err=True)
        sys.exit(1)


@sync.command("concepts")
@click.option("--with-constituents", is_flag=True, help="同时同步成分股")
@click.option("--limit", type=int, default=None, help="限制概念数量")
def sync_concepts_cmd(with_constituents: bool, limit: int):
    """同步概念板块"""
    click.echo("Syncing concept boards...")
    
    from modules.ingestion import sync_concepts, sync_concept_constituents
    
    try:
        result = sync_concepts()
        click.echo(f"✓ Synced {result['inserted']} new, {result['updated']} updated concepts")
        
        if with_constituents:
            click.echo("Syncing concept constituents...")
            result = sync_concept_constituents(limit=limit)
            click.echo(f"✓ Processed {result['concepts_processed']} concepts, {result['links_inserted']} links")
    except Exception as e:
        click.echo(f"✗ Sync failed: {e}", err=True)
        sys.exit(1)


@sync.command("industries")
@click.option("--with-constituents", is_flag=True, help="同时同步成分股")
@click.option("--limit", type=int, default=None, help="限制行业数量")
def sync_industries_cmd(with_constituents: bool, limit: int):
    """同步行业分类"""
    click.echo("Syncing industry boards...")
    
    from modules.ingestion import sync_industries, sync_industry_constituents
    
    try:
        result = sync_industries()
        click.echo(f"✓ Synced {result['inserted']} new, {result['updated']} updated industries")
        
        if with_constituents:
            click.echo("Syncing industry constituents...")
            result = sync_industry_constituents(limit=limit)
            click.echo(f"✓ Processed {result['industries_processed']} industries, {result['links_inserted']} links")
    except Exception as e:
        click.echo(f"✗ Sync failed: {e}", err=True)
        sys.exit(1)


@sync.command("profiles")
@click.option("--limit", type=int, default=None, help="限制同步数量")
@click.option("--force", is_flag=True, help="强制重新同步已有数据")
@click.option("--markets", type=str, default="CN", help="市场列表: CN,HK,US,ALL")
def sync_profiles_cmd(limit: int, force: bool, markets: str):
    """同步公司简介并向量化"""
    click.echo("Syncing company profiles...")
    
    from modules.ingestion import sync_profiles
    
    try:
        markets_list = _parse_markets(markets)
        summary = {"synced": 0, "skipped": 0, "errors": 0}
        for market in markets_list:
            result = sync_profiles(
                limit=limit,
                skip_existing=not force,
                market=market,
            )
            summary["synced"] += result["synced"]
            summary["skipped"] += result["skipped"]
            summary["errors"] += result["errors"]
        click.echo(
            f"✓ Synced {summary['synced']}, skipped {summary['skipped']}, errors {summary['errors']}"
        )
    except Exception as e:
        click.echo(f"✗ Sync failed: {e}", err=True)
        sys.exit(1)


@sync.command("all")
@click.option("--profiles-limit", type=int, default=100, help="公司简介同步数量限制")
@click.option("--markets", type=str, default="CN,HK,US", help="市场列表: CN,HK,US,ALL")
def sync_all_cmd(profiles_limit: int, markets: str):
    """同步所有数据"""
    click.echo("=" * 50)
    click.echo("Starting full sync...")
    click.echo("=" * 50)
    
    from modules.ingestion import (
        sync_assets_by_markets, sync_concepts, sync_industries, sync_profiles
    )
    
    try:
        markets_list = _parse_markets(markets)
        # 1. 同步股票列表
        click.echo("\n[1/4] Syncing assets...")
        result = sync_assets_by_markets(markets_list)
        click.echo(f"✓ Assets: {result['inserted']} new, {result['updated']} updated")
        
        # 2. 同步概念板块
        click.echo("\n[2/4] Syncing concepts...")
        result = sync_concepts()
        click.echo(f"✓ Concepts: {result['inserted']} new, {result['updated']} updated")
        
        # 3. 同步行业分类
        click.echo("\n[3/4] Syncing industries...")
        result = sync_industries()
        click.echo(f"✓ Industries: {result['inserted']} new, {result['updated']} updated")
        
        # 4. 同步公司简介 (限制数量)
        click.echo(f"\n[4/4] Syncing profiles (limit={profiles_limit})...")
        profile_total = 0
        for market in markets_list:
            result = sync_profiles(limit=profiles_limit, market=market)
            profile_total += result["synced"]
        click.echo(f"✓ Profiles: {profile_total} synced")
        
        click.echo("\n" + "=" * 50)
        click.echo("Full sync completed!")
        click.echo("=" * 50)
    
    except Exception as e:
        click.echo(f"✗ Sync failed: {e}", err=True)
        sys.exit(1)


@sync.command("daemon")
@click.option("--markets", type=str, default="CN,HK,US", help="市场列表: CN,HK,US,ALL")
@click.option("--interval", type=int, default=3600, help="循环间隔(秒)")
@click.option("--profiles-limit", type=int, default=0, help="公司简介同步数量限制")
@click.option("--once", is_flag=True, help="只执行一次")
def sync_daemon_cmd(markets: str, interval: int, profiles_limit: int, once: bool):
    click.echo("Starting sync daemon...")
    from modules.ingestion import (
        sync_assets_by_markets,
        sync_concepts,
        sync_industries,
        sync_profiles,
    )
    markets_list = _parse_markets(markets)
    while True:
        try:
            click.echo("Syncing assets...")
            sync_assets_by_markets(markets_list)
            click.echo("Syncing concepts...")
            sync_concepts()
            click.echo("Syncing industries...")
            sync_industries()
            click.echo("Syncing profiles...")
            for market in markets_list:
                sync_profiles(
                    limit=profiles_limit if profiles_limit > 0 else None,
                    market=market,
                )
        except Exception as e:
            click.echo(f"✗ Sync failed: {e}", err=True)
        if once:
            break
        time.sleep(interval)


@cli.command()
@click.argument("query")
@click.option("--limit", type=int, default=10, help="返回结果数量")
def search(query: str, limit: int):
    """语义搜索公司"""
    click.echo(f"Searching for: {query}")
    
    from modules.ingestion import search_companies
    
    try:
        results = search_companies(query, n_results=limit)
        
        if not results:
            click.echo("No results found.")
            return
        
        click.echo(f"\nFound {len(results)} results:\n")
        
        for i, r in enumerate(results, 1):
            metadata = r.get("metadata", {})
            symbol = metadata.get("symbol", "N/A")
            name = metadata.get("name", "N/A")
            chunk_type = metadata.get("chunk_type", "N/A")
            distance = r.get("distance", "N/A")
            
            doc = r.get("document", "")[:100]
            
            click.echo(f"{i}. [{symbol}] {name} ({chunk_type})")
            click.echo(f"   Distance: {distance:.4f}" if isinstance(distance, float) else f"   Distance: {distance}")
            click.echo(f"   {doc}...")
            click.echo()
    
    except Exception as e:
        click.echo(f"✗ Search failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def stats():
    """显示数据库统计信息"""
    from sqlmodel import select, func
    
    click.echo("=" * 50)
    click.echo("Database Statistics")
    click.echo("=" * 50)
    
    try:
        # Meta DB 统计
        from domain.meta import Asset, Concept, Industry, AssetProfile
        
        with db_manager.meta_session() as session:
            asset_count = len(list(session.exec(select(Asset)).all()))
            concept_count = len(list(session.exec(select(Concept)).all()))
            industry_count = len(list(session.exec(select(Industry)).all()))
            profile_count = len(list(session.exec(select(AssetProfile)).all()))
        
        click.echo(f"\n[Meta DB]")
        click.echo(f"  Assets:     {asset_count:,}")
        click.echo(f"  Concepts:   {concept_count:,}")
        click.echo(f"  Industries: {industry_count:,}")
        click.echo(f"  Profiles:   {profile_count:,}")
        
        # Vector DB 统计
        click.echo(f"\n[Vector DB]")
        for name in ["company_chunks", "industry_knowledge", "market_events", "entity_relation"]:
            try:
                collection = db_manager.get_collection(name)
                count = collection.count()
                click.echo(f"  {name}: {count:,}")
            except Exception:
                click.echo(f"  {name}: N/A")
        
        # Cache 统计
        cache_stats = cache_manager.stats()
        click.echo(f"\n[Cache]")
        click.echo(f"  Items:  {cache_stats['size']:,}")
        click.echo(f"  Volume: {cache_stats['volume'] / 1024 / 1024:.2f} MB")
        
        click.echo("\n" + "=" * 50)
    
    except Exception as e:
        click.echo(f"✗ Stats failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def clear_cache():
    """清空缓存"""
    click.echo("Clearing cache...")
    cache_manager.clear()
    click.echo("✓ Cache cleared!")


def _parse_markets(markets: str) -> list[str]:
    items = [m.strip().upper() for m in markets.split(",") if m.strip()]
    if not items:
        return ["CN"]
    if "ALL" in items:
        return ["CN", "HK", "US"]
    return items


if __name__ == "__main__":
    cli()
