
"""
Task Scheduler - 核心调度系统
"""
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

# Import modules (Lazy import inside functions to avoid circular deps if any)
from modules.ingestion.sync_news import news_syncer
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
        self.scheduler = BackgroundScheduler()
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
        from modules.monitor.scanner import MonitorService
        self.scheduler.add_job(
            MonitorService.scan_and_alert,
            IntervalTrigger(minutes=1),
            id="monitor_scan",
            name="股票异动监控",
            replace_existing=True
        )
        
        # 3.5 大宗商品监控 (每 5 分钟)
        from modules.monitor.commodity_scanner import CommodityScanner
        self.scheduler.add_job(
            CommodityScanner.scan_and_alert,
            IntervalTrigger(minutes=5),
            id="commodity_scan",
            name="大宗商品异动监控",
            replace_existing=True
        )
        
        # 4. CN 热度榜单 (工作日 15:30 A股收盘后半小时)
        self.scheduler.add_job(
            self._job_cn_heatmap,
            CronTrigger(day_of_week='mon-fri', hour=15, minute=30),
            id="cn_heatmap",
            name="A股热门榜单",
            replace_existing=True
        )

        # 4.1 HK 热度榜单 (工作日 16:30 港股收盘后半小时)
        self.scheduler.add_job(
            self._job_hk_heatmap,
            CronTrigger(day_of_week='mon-fri', hour=16, minute=30),
            id="hk_heatmap",
            name="港股热门榜单",
            replace_existing=True
        )

        # 5. US 热度榜单 (周二至周六 10:00，对应美股周一到周五收盘后)
        self.scheduler.add_job(
            self._job_us_heatmap,
            CronTrigger(day_of_week='tue-sat', hour=10, minute=0),
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
        
        # 7. 7日监控战报 (每周五 18:00)
        from modules.monitor.performance_report import PerformanceReportService
        self.scheduler.add_job(
            PerformanceReportService.generate_and_push_report,
            CronTrigger(day_of_week='fri', hour=18, minute=0),
            args=[7],
            id="report_7d",
            name="7日表现战报",
            replace_existing=True
        )
        
        # 8. 30日监控战报 (每月最后一天 18:30)
        self.scheduler.add_job(
            PerformanceReportService.generate_and_push_report,
            CronTrigger(day='last', hour=18, minute=30),
            args=[30],
            id="report_30d",
            name="30日表现战报",
            replace_existing=True
        )
        
        logger.info(f"Registered {len(self.scheduler.get_jobs())} jobs.")

    def _job_sync_news(self):
        """Job: 同步新闻"""
        logger.info("Job started: Sync News")
        try:
            news_syncer.sync_news_stream(limit=20)
        except Exception as e:
            logger.error(f"Job failed (Sync News): {e}")

    def _job_sync_reports(self):
        """Job: 同步研报"""
        logger.info("Job started: Sync Reports")
        try:
            report_syncer.sync_industry_reports()
        except Exception as e:
            logger.error(f"Job failed (Sync Reports): {e}")

    def _job_cn_heatmap(self):
        """Job: 生成A股热门榜单"""
        logger.info("Job started: CN Heat Map")
        try:
            heatmap_service.process_and_notify("CN")
        except Exception as e:
            logger.error(f"Job failed (CN Heat Map): {e}")

    def _job_hk_heatmap(self):
        """Job: 生成港股热门榜单"""
        logger.info("Job started: HK Heat Map")
        try:
            heatmap_service.process_and_notify("HK")
        except Exception as e:
            logger.error(f"Job failed (HK Heat Map): {e}")

    def _job_us_heatmap(self):
        """Job: 生成美股热门榜单"""
        logger.info("Job started: US Heat Map")
        try:
            heatmap_service.process_and_notify("US")
        except Exception as e:
            logger.error(f"Job failed (US Heat Map): {e}")

    def _job_sync_fundamentals(self):
        """Job: 全市场基本面与分析更新 (深水区任务)"""
        logger.info("Job started: Sync Fundamentals")
        try:
            # 1. 结构化：财务数据入 SQLite
            for market in ["CN", "HK", "US"]:
                financial_syncer.sync_financials(market=market)
            
            # 2. 非结构化：画像 Chunking 入 Chroma
            for market in ["CN", "HK", "US"]:
                profile_syncer.sync_profiles_batch(market=market, skip_existing=False)
                
            # 3. 供应链关系提取：由于 LLM 非常慢，这里演示取 A 股热门或者限制 limit=10
            # 实际业务中应针对池子进行增量
            from core.db import db_manager
            from sqlmodel import select
            from domain.meta import Asset
            
            with db_manager.meta_session() as session:
                statement = select(Asset.symbol).where(Asset.market == "CN").limit(5)
                symbols = list(session.exec(statement).all())
                
            for symbol in symbols:
                relation_syncer.sync_relations_for_symbol(symbol)
                
        except Exception as e:
            logger.error(f"Job failed (Sync Fundamentals): {e}")

# 全局单例
task_scheduler = TaskScheduler()
