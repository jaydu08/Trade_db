
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
from modules.ingestion.sync_reports import report_syncer
# from modules.ingestion.sync_financial import financial_syncer # Can be heavy

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
        
        # 3. 财务数据/Profile 更新 (每天 02:00)
        # self.scheduler.add_job(...)
        
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

# 全局单例
task_scheduler = TaskScheduler()
