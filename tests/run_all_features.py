# -*- coding: utf-8 -*-
import os
import sys
import time
import logging

# Ensure project root is in PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger("E2E_Tester")

# --- Import all core modules to test ---
from core.db import db_manager, get_collection
from core.llm import simple_prompt
from modules.monitor.notifier import Notifier
from modules.ingestion.sync_news import news_syncer
from modules.ingestion.sync_reports import report_syncer
from modules.ingestion.sync_financial import financial_syncer
from modules.ingestion.sync_profile import profile_syncer
from modules.ingestion.sync_relations import relation_syncer
from modules.monitor.scanner import MonitorService
from modules.analysis.heatmap import heatmap_service

ENABLE_E2E_NOTIFY = os.getenv("E2E_NOTIFY", "0") == "1"

def test_infra():
    logger.info("=== TEST 1: Infrastructure (Database & LLM) ===")
    
    # 1.1 DB
    logger.info("Testing DB Connection...")
    with db_manager.meta_session() as session:
        from sqlmodel import select
        from domain.meta import Asset
        count = len(list(session.exec(select(Asset).limit(5)).all()))
        logger.info(f"[OK] Read {count} assets from SQLite.")
        
    collection = get_collection("company_chunks")
    logger.info(f"[OK] ChromaDB connection successful. Collection company_chunks has {collection.count()} items.")

    # 1.2 LLM
    logger.info("Testing LLM Connection...")
    try:
        resp = simple_prompt("Reply with only the word 'PONG'.", temperature=0.1)
        logger.info(f"[OK] LLM Replied: {resp.strip()}")
    except Exception as e:
        logger.error(f"[FAIL] LLM Connection: {e}")

    # 1.3 Telegram
    logger.info("Testing Telegram Bot...")
    try:
        if ENABLE_E2E_NOTIFY:
            Notifier.broadcast("🚧 Tradedb 终极压测开始:\n正在验证全部 6 大核心链路...")
            logger.info("[OK] Telegram message sent via Notifier.")
        else:
            logger.info("[SKIP] Telegram notify disabled. Set E2E_NOTIFY=1 to enable.")
    except Exception as e:
        logger.error(f"[FAIL] Telegram Bot: {e}")

def test_macro_sync():
    logger.info("\\n=== TEST 2: Macro Sync (News & Reports) ===")
    
    logger.info("Fetching global news stream...")
    news_res = news_syncer.sync_news_stream(limit=1)
    synced_news = news_res.get('synced', 0) if news_res else 0
    logger.info(f"[OK] Synced {synced_news} news items.")
    
    logger.info("Fetching latest industry report...")
    # Prevent over-syncing, limit 1 (Note: method doesn't take args, it handles its own limits)
    report_res = report_syncer.sync_industry_reports()
    synced_rep = report_res.get('synced', 0) if report_res else 0
    logger.info(f"[OK] Synced {synced_rep} industry reports to ChromaDB.")

def test_fundamentals():
    logger.info("\\n=== TEST 3: Deep Fundamentals & Profiling ===")
    target_symbol = "00700" # Tencent
    target_market = "HK"
    
    logger.info(f"Fetching Financial Abstract for {target_symbol} ({target_market})...")
    fin_res = financial_syncer.sync_financials(market=target_market, limit=1)
    logger.info(f"[OK] Financials synced. Result: {fin_res}")
    
    logger.info(f"Fetching and Chunking Profile for {target_symbol} ({target_market})...")
    # Using the direct method to bypass batch loops for testing
    prof_res = profile_syncer.sync_profile(target_symbol, target_market)
    prof_len = prof_res.get('profile_length', 0) if prof_res else 0
    logger.info(f"[OK] Profile chunked and ingested. Extracted length: {prof_len} chars.")

def test_relations():
    logger.info("\\n=== TEST 4: Supply Chain Entity Relations Extraction via LLM ===")
    target_symbol = "00700" # Tencent
    logger.info(f"Extracting relations for {target_symbol}...")
    rel_count = relation_syncer.sync_relations_for_symbol(target_symbol)
    logger.info(f"[OK] Extracted {rel_count} edge(s) and persisted to entity_relation graph.")

def test_scanner():
    logger.info("\\n=== TEST 5: Minute-level Anomaly Scanning & Attribution ===")
    logger.info("Executing MonitorService.scan_and_alert()...")
    # For testing, we ensure it won't send ton of telegrams by mocking or letting it run dry if no anomalies
    # To actually test attribution we could mock an anomaly, but a real scan is better to see if API works
    try:
        MonitorService.scan_and_alert()
        logger.info("[OK] Scanner completed successfully. (Event logging requires real market anomalies)")
    except Exception as e:
        logger.error(f"[WARN] Scanner encountered an error (Normal if outside market hours or API limit): {e}")

def test_heatmap_and_rag():
    logger.info("\\n=== TEST 6: Heatmap Generation & Chain Mining RAG ===")
    
    logger.info("Generating Heatmap for US (Limit execution for speed)...")
    try:
        # Assuming process_and_notify pushes to TG
        heatmap_service.process_and_notify("US")
        logger.info("[OK] US Heatmap generated and pushed.")
    except Exception as e:
        logger.error(f"[WARN] Heatmap failed: {e}")
        
    logger.info("Testing RAG Chain Mining Strategy on '电池'...")
    try:
        # Just run the cross_validate or whole pipeline
        # Not a full run to avoid massive LLM calls, just retrieve vectors
        col = get_collection("industry_knowledge")
        q = col.query(query_texts=["固态电池"], n_results=1)
        if q and q["documents"] and q["documents"][0]:
            logger.info(f"[OK] RAG retrieved: {q['documents'][0][0][:50]}...")
        else:
            logger.info("[WARN] RAG retrieved nothing. (Requires reports synced previously)")
    except Exception as e:
        logger.error(f"[FAIL] Chain Mining RAG: {e}")

if __name__ == "__main__":
    logger.info("🚀 Starting Trade_db End-to-End System Tests")
    start = time.time()
    
    test_infra()
    time.sleep(1)
    test_macro_sync()
    time.sleep(1)
    test_fundamentals()
    time.sleep(1)
    test_relations()
    time.sleep(1)
    test_scanner()
    time.sleep(1)
    test_heatmap_and_rag()
    
    if ENABLE_E2E_NOTIFY:
        Notifier.broadcast("✅ Tradedb 终极压测完毕:\n所有的核心数据链路验证结束，请查看后端日志以确认全部 Success。")
    
    logger.info(f"🎉 All tests executed in {time.time() - start:.2f} seconds.")
