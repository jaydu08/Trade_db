
"""
Task Scheduler - 核心调度系统
"""
import logging
import time
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
        
        # 7. 7日监控战报 (每周五 18:00)
        self.scheduler.add_job(
            self._job_report_7d,
            CronTrigger(day_of_week='fri', hour=18, minute=0),
            id="report_7d",
            name="7日表现战报",
            replace_existing=True
        )
        
        # 8. 30日监控战报 (每月最后一天 18:30)
        self.scheduler.add_job(
            self._job_report_30d,
            CronTrigger(day='last', hour=18, minute=30),
            id="report_30d",
            name="30日表现战报",
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

    def _job_report_7d(self):
        """Job: 7日表现战报"""
        from modules.monitor.performance_report import PerformanceReportService
        self._run_job("report_7d", PerformanceReportService.generate_and_push_report, 7)

    def _job_report_30d(self):
        """Job: 30日表现战报"""
        from modules.monitor.performance_report import PerformanceReportService
        self._run_job("report_30d", PerformanceReportService.generate_and_push_report, 30)

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

# 全局单例
task_scheduler = TaskScheduler()
