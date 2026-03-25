
"""
Task Scheduler - 核心调度系统
"""
import logging
import time
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# Import modules (Lazy import inside functions to avoid circular deps if any)
from modules.ingestion.sync_news import news_syncer
from modules.ingestion.sync_reports import report_syncer
from modules.ingestion.sync_financial import financial_syncer
from modules.ingestion.sync_profile import profile_syncer
from modules.ingestion.sync_relations import relation_syncer
from modules.analysis.heatmap import heatmap_service

logger = logging.getLogger(__name__)

class TaskScheduler:
    """
    任务调度器
    """
    def __init__(self):
        # 固定调度时区，避免部署机器时区变化导致任务错时执行
        self.scheduler = BackgroundScheduler(
            timezone="Asia/Shanghai",
            job_defaults={
                # 长任务错过调度点时合并执行，避免堆积补跑
                "coalesce": True,
                # 同一任务禁止并发重入，避免扫描和同步任务叠加
                "max_instances": 1,
                # 允许 5 分钟内的触发误差，超过则丢弃该次执行
                "misfire_grace_time": 300,
            },
        )
        self.jobs = []

    def start(self):
        """启动调度器"""
        self._register_jobs()
        self.scheduler.start()
        logger.info("Task Scheduler started.")

    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()
        logger.info("Task Scheduler stopped.")

    def _register_jobs(self):
        """注册所有定时任务"""
        
        # 1. 实时新闻监控 (每 10 分钟)
        # 盘中时间: 09:00 - 16:00 (A股) / 全天 (美股/Crypto)
        # 这里简化为全天运行，或者设置更细致的 Trigger
        self.scheduler.add_job(
            self._job_sync_news,
            IntervalTrigger(minutes=10),
            id="sync_news",
            name="实时新闻监控",
            replace_existing=True
        )
        
        # 2. 研报同步 (每天 18:00)
        self.scheduler.add_job(
            self._job_sync_reports,
            CronTrigger(hour=18, minute=0),
            id="sync_reports",
            name="研报同步",
            replace_existing=True
        )

        # 3. 异动监控 (每 1 分钟)
        self.scheduler.add_job(
            self._job_monitor_scan,
            IntervalTrigger(minutes=1),
            id="monitor_scan",
            name="股票异动监控",
            replace_existing=True
        )
        
        # 3.5 大宗商品每日龙虎榜战报 (周二至周六 08:00，涵盖前一日日盘及夜盘)
        self.scheduler.add_job(
            self._job_commodity_scan,
            CronTrigger(day_of_week='tue-sat', hour=8, minute=0),
            id="commodity_scan",
            name="大宗商品每日战报",
            replace_existing=True
        )
        
        # 4. CN 热度榜单 (工作日 18:30，留时间发酵盘后新闻)
        self.scheduler.add_job(
            self._job_cn_heatmap,
            CronTrigger(day_of_week='mon-fri', hour=18, minute=30),
            id="cn_heatmap",
            name="A股热门榜单",
            replace_existing=True
        )

        # 4.1 HK 热度榜单 (工作日 18:30，留时间发酵盘后新闻)
        self.scheduler.add_job(
            self._job_hk_heatmap,
            CronTrigger(day_of_week='mon-fri', hour=18, minute=30),
            id="hk_heatmap",
            name="港股热门榜单",
            replace_existing=True
        )

        # 5. US 热度榜单 (周二至周六 08:00，对应美股周一到周五收盘后)
        self.scheduler.add_job(
            self._job_us_heatmap,
            CronTrigger(day_of_week='tue-sat', hour=8, minute=0),
            id="us_heatmap",
            name="美股热门榜单",
            replace_existing=True
        )
        
        # 6. 基本面/财务/画像深度全量更新 (每天 02:00)
        self.scheduler.add_job(
            self._job_sync_fundamentals,
            CronTrigger(hour=2, minute=0),
            id="sync_fundamentals",
            name="基本面及画像同步",
            replace_existing=True
        )
        
        # 7. Trend 7日简报 (每周日 10:00)
        self.scheduler.add_job(
            self._job_trend_7d,
            CronTrigger(day_of_week='sun', hour=10, minute=0),
            id="trend_7d",
            name="7日趋势简报",
            replace_existing=True
        )
        
        # 8. Trend 30日简报 (每月最后一天 11:00)
        self.scheduler.add_job(
            self._job_trend_30d,
            CronTrigger(day='last', hour=11, minute=0),
            id="trend_30d",
            name="30日趋势简报",
            replace_existing=True
        )

        # 9. 每日推送标的汇总 TXT (工作日 20:00，北京时间)
        self.scheduler.add_job(
            self._job_daily_summary,
            CronTrigger(day_of_week='mon-fri', hour=20, minute=0),
            id="daily_summary",
            name="每日推送标的汇总",
            replace_existing=True
        )

        # 10. 趋势池日线补齐（A/H）: 工作日 19:10
        self.scheduler.add_job(
            self._job_trend_pool_refresh_cn_hk,
            CronTrigger(day_of_week='mon-fri', hour=19, minute=10),
            id="trend_pool_refresh_cn_hk",
            name="趋势池日线补齐(A/H)",
            replace_existing=True
        )

        # 10.1 趋势池日线补齐（A/H）重试: 工作日 21:10
        self.scheduler.add_job(
            self._job_trend_pool_refresh_cn_hk_retry,
            CronTrigger(day_of_week='mon-fri', hour=21, minute=10),
            id="trend_pool_refresh_cn_hk_retry",
            name="趋势池日线补齐(A/H)-重试",
            replace_existing=True
        )

        # 11. 趋势池日线补齐（US/CF）: 周二至周六 08:10
        self.scheduler.add_job(
            self._job_trend_pool_refresh_us_cf,
            CronTrigger(day_of_week='tue-sat', hour=8, minute=10),
            id="trend_pool_refresh_us_cf",
            name="趋势池日线补齐(US/CF)",
            replace_existing=True
        )

        # 11.1 趋势池日线补齐（US/CF）重试: 周二至周六 10:10
        self.scheduler.add_job(
            self._job_trend_pool_refresh_us_cf_retry,
            CronTrigger(day_of_week='tue-sat', hour=10, minute=10),
            id="trend_pool_refresh_us_cf_retry",
            name="趋势池日线补齐(US/CF)-重试",
            replace_existing=True
        )

        # 12/13. DailyRank 独立同步任务（默认关闭）
        # 当前主链路由 heatmap 直接写入 ledger.dailyrank，避免双链路口径冲突。
        if os.getenv("ENABLE_DAILY_RANK_JOB", "0") == "1":
            self.scheduler.add_job(
                self._job_daily_rank_cn_hk,
                CronTrigger(day_of_week='mon-fri', hour=18, minute=50),
                id="daily_rank_cn_hk",
                name="每日榜单同步(CN/HK)",
                replace_existing=True
            )

            self.scheduler.add_job(
                self._job_daily_rank_us,
                CronTrigger(day_of_week='tue-sat', hour=8, minute=5),
                id="daily_rank_us",
                name="每日榜单同步(US)",
                replace_existing=True
            )
        else:
            logger.info("DailyRank standalone jobs disabled; heatmap pipeline will persist DailyRank directly.")

        # 14. 模拟交易到期检查 (每天 19:20 扫一波)
        self.scheduler.add_job(
            self._job_paper_trade_check,
            CronTrigger(hour=19, minute=20),
            id="paper_trade_check",
            name="模拟交易到期检查",
            replace_existing=True
        )

        logger.info(f"Registered {len(self.scheduler.get_jobs())} jobs.")

    def _run_job(self, job_id: str, func, *args, **kwargs):
        """统一任务执行包装：记录耗时、结果与异常。"""
        started = time.perf_counter()
        logger.info("Job started: %s", job_id)
        try:
            result = func(*args, **kwargs)
            duration_ms = int((time.perf_counter() - started) * 1000)
            if isinstance(result, dict):
                logger.info("Job finished: %s | duration_ms=%s | result=%s", job_id, duration_ms, result)
            else:
                logger.info("Job finished: %s | duration_ms=%s", job_id, duration_ms)
            return result
        except Exception as e:
            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.error("Job failed: %s | duration_ms=%s | error=%s", job_id, duration_ms, e, exc_info=True)
            return None

    def _job_sync_news(self):
        """Job: 同步新闻"""
        self._run_job("sync_news", news_syncer.sync_news_stream, limit=20)

    def _job_sync_reports(self):
        """Job: 同步研报"""
        self._run_job("sync_reports", report_syncer.sync_industry_reports)

    def _job_cn_heatmap(self):
        """Job: 生成A股热门榜单"""
        self._run_job("cn_heatmap", heatmap_service.process_and_notify, "CN")

    def _job_hk_heatmap(self):
        """Job: 生成港股热门榜单"""
        self._run_job("hk_heatmap", heatmap_service.process_and_notify, "HK")

    def _job_us_heatmap(self):
        """Job: 生成美股热门榜单"""
        self._run_job("us_heatmap", heatmap_service.process_and_notify, "US")

    def _job_monitor_scan(self):
        """Job: 自选股异动扫描"""
        from modules.monitor.scanner import MonitorService
        self._run_job("monitor_scan", MonitorService.scan_and_alert)

    def _job_commodity_scan(self):
        """Job: 大宗商品每日战报"""
        from modules.monitor.commodity_scanner import CommodityScanner
        self._run_job("commodity_scan", CommodityScanner.generate_daily_report)

    def _job_trend_7d(self):
        """Job: 7日趋势简报"""
        from modules.monitor.trend_report_service import TrendReportService
        self._run_job("trend_7d", TrendReportService.generate_and_push, 7)

    def _job_trend_30d(self):
        """Job: 30日趋势简报"""
        from modules.monitor.trend_report_service import TrendReportService
        self._run_job("trend_30d", TrendReportService.generate_and_push, 30)

    def _job_daily_summary(self):
        """Job: 每日推送标的汇总 → 写入 logs/daily_summary_YYYYMMDD.txt"""
        from modules.monitor.daily_summary_service import DailySummaryService
        self._run_job("daily_summary", DailySummaryService.generate_and_save)

    def _job_sync_fundamentals(self):
        """Job: 全市场基本面与分析更新 (深水区任务)"""
        def _execute():
            # 1. 结构化：财务数据入 SQLite
            fin_results = {}
            for market in ["CN", "HK", "US"]:
                fin_results[market] = financial_syncer.sync_financials(market=market)
            
            # 2. 非结构化：画像 Chunking 入 Chroma
            profile_results = {}
            for market in ["CN", "HK", "US"]:
                profile_results[market] = profile_syncer.sync_profiles_batch(market=market, skip_existing=False)
                
            # 3. 供应链关系提取：由于 LLM 非常慢，这里演示取 A 股热门或者限制 limit=10
            # 实际业务中应针对池子进行增量
            from core.db import db_manager
            from sqlmodel import select
            from domain.meta import Asset
            
            with db_manager.meta_session() as session:
                statement = select(Asset.symbol).where(Asset.market == "CN").limit(5)
                symbols = list(session.exec(statement).all())
            
            relation_synced = 0
            for symbol in symbols:
                relation_synced += int(relation_syncer.sync_relations_for_symbol(symbol) or 0)

            return {
                "financial": fin_results,
                "profiles": profile_results,
                "relation_symbols": len(symbols),
                "relation_synced": relation_synced,
            }

        self._run_job("sync_fundamentals", _execute)

    def _job_trend_pool_refresh_cn_hk(self):
        """Job: 趋势池日线补齐（CN/HK）"""
        from modules.monitor.trend_service import TrendService
        self._run_job(
            "trend_pool_refresh_cn_hk",
            TrendService.refresh_pool_daily_bars,
            ["CN", "HK"],
            60,
            "trend_pool_refresh_eod",
            True,
        )

    def _job_trend_pool_refresh_cn_hk_retry(self):
        """Job: 趋势池日线补齐重试（CN/HK）"""
        from modules.monitor.trend_service import TrendService
        self._run_job(
            "trend_pool_refresh_cn_hk_retry",
            TrendService.refresh_pool_daily_bars,
            ["CN", "HK"],
            60,
            "trend_pool_refresh_eod",
            False,
        )

    def _job_trend_pool_refresh_us_cf(self):
        """Job: 趋势池日线补齐（US/CF）"""
        from modules.monitor.trend_service import TrendService
        self._run_job(
            "trend_pool_refresh_us_cf",
            TrendService.refresh_pool_daily_bars,
            ["US", "CF"],
            60,
            "trend_pool_refresh_eod",
            True,
        )

    def _job_trend_pool_refresh_us_cf_retry(self):
        """Job: 趋势池日线补齐重试（US/CF）"""
        from modules.monitor.trend_service import TrendService
        self._run_job(
            "trend_pool_refresh_us_cf_retry",
            TrendService.refresh_pool_daily_bars,
            ["US", "CF"],
            60,
            "trend_pool_refresh_eod",
            False,
        )

    def _job_daily_rank_cn_hk(self):
        """Job: 每日榜单同步（CN/HK）"""
        from modules.monitor.daily_rank_service import DailyRankService
        self._run_job("daily_rank_cn_hk", DailyRankService.sync_daily_ranks, ["CN", "HK"])

    def _job_daily_rank_us(self):
        """Job: 每日榜单同步（US）"""
        from modules.monitor.daily_rank_service import DailyRankService
        self._run_job("daily_rank_us", DailyRankService.sync_daily_ranks, ["US"])

    def _job_paper_trade_check(self):
        """Job: 模拟交易到期检查"""
        from modules.paper_trading.service import PaperTradingService
        from modules.paper_trading.reviewer import PaperTradeReviewer
        
        def _execute():
            expired_trades = PaperTradingService.check_expired_trades()
            if not expired_trades:
                return {"expired_count": 0}
            
            from interface.telegram_bot import bot_instance, TelegramHTMLRenderer
            import asyncio
            import nest_asyncio
            
            # Apply nest_asyncio to allow nested event loops in threads if necessary
            nest_asyncio.apply()
            
            success_count = 0
            for trade in expired_trades:
                try:
                    report = PaperTradeReviewer.generate_review(trade)
                    trade.review_text = report
                    from core.db import db_manager
                    with db_manager.ledger_session() as session:
                        session.add(trade)
                        session.commit()
                    
                    if bot_instance and trade.chat_id:
                        text = f"🚨 <b>【到期复盘】您的模拟持仓 {trade.name}({trade.symbol}) 已到达打卡时间！自动平仓并复盘：</b>\n\n{report}"
                        html_text = TelegramHTMLRenderer.render(text)
                        
                        loop = asyncio.get_event_loop()
                        if len(html_text) > 4000:
                            coro1 = bot_instance.app.bot.send_message(chat_id=trade.chat_id, text=html_text[:4000], parse_mode="HTML")
                            coro2 = bot_instance.app.bot.send_message(chat_id=trade.chat_id, text=html_text[4000:], parse_mode="HTML")
                            loop.run_until_complete(coro1)
                            loop.run_until_complete(coro2)
                        else:
                            coro = bot_instance.app.bot.send_message(chat_id=trade.chat_id, text=html_text, parse_mode="HTML")
                            loop.run_until_complete(coro)
                        success_count += 1
                except Exception as e:
                    logger.error(f"Failed to process and notify expired trade {trade.symbol}: {e}")
            
            return {"expired_count": len(expired_trades), "notified_count": success_count}
            
        self._run_job("paper_trade_check", _execute)

# 全局单例
task_scheduler = TaskScheduler()
