
"""
Sync Financial - 同步股票财务数据
"""
import logging
from datetime import datetime, date
from typing import Optional, List
import pandas as pd
from sqlmodel import select

from domain.meta.financial import AssetFinancial
from domain.meta.sync_log import DataSyncLog
from domain.meta.asset import Asset
from core.db import db_manager
from modules.ingestion.akshare_client import akshare_client
import akshare as ak

logger = logging.getLogger(__name__)

class FinancialSyncer:
    """
    财务数据同步器
    """
    
    def _create_sync_log(self, table_name: str, sync_type: str = "FULL") -> int:
        log = DataSyncLog(
            table_name=table_name,
            sync_type=sync_type,
            data_source="akshare",
            started_at=datetime.utcnow(),
            status="RUNNING",
        )
        with db_manager.meta_session() as session:
            session.add(log)
            session.commit()
            session.refresh(log)
            return log.id

    def _update_sync_log(self, log_id: int, status: str, record_count: int = 0, error_msg: Optional[str] = None):
        with db_manager.meta_session() as session:
            log = session.get(DataSyncLog, log_id)
            if log:
                log.status = status
                log.record_count = record_count
                log.ended_at = datetime.utcnow()
                log.error_msg = error_msg
                session.add(log)

    def _safe_float(self, val):
        try:
            return float(val)
        except:
            return None

    def _sync_one_stock(self, symbol: str, valuation_data: dict):
        """
        同步单个股票的财务数据
        """
        with db_manager.meta_session() as session:
            financial = session.get(AssetFinancial, symbol)
            
            if not financial:
                financial = AssetFinancial(
                    symbol=symbol,
                    report_date=date.today(), # Using today to mark last sync date
                    updated_at=datetime.utcnow().isoformat()
                )
            
            # Update fields
            if "pe_ttm" in valuation_data: financial.pe_ttm = valuation_data["pe_ttm"]
            if "pb" in valuation_data: financial.pb = valuation_data["pb"]
            if "market_cap" in valuation_data: financial.market_cap = valuation_data["market_cap"]
            if "dv_ratio" in valuation_data: financial.dv_ratio = valuation_data["dv_ratio"]
            if "total_revenue" in valuation_data: financial.total_revenue = valuation_data["total_revenue"]
            if "net_profit" in valuation_data: financial.net_profit = valuation_data["net_profit"]
            if "gross_profit_margin" in valuation_data: financial.gross_profit_margin = valuation_data["gross_profit_margin"]
            if "net_profit_margin" in valuation_data: financial.net_profit_margin = valuation_data["net_profit_margin"]
            if "roe" in valuation_data: financial.roe = valuation_data["roe"]
            if "revenue_yoy" in valuation_data: financial.revenue_yoy = valuation_data["revenue_yoy"]
            if "net_profit_yoy" in valuation_data: financial.net_profit_yoy = valuation_data["net_profit_yoy"]
            
            financial.updated_at = datetime.utcnow().isoformat()
            session.add(financial)
            session.commit()

    def sync_financials(self, limit: Optional[int] = None, market: str = "CN") -> dict:
        """
        同步股票深度财务数据 (CN/HK/US)
        """
        sync_log_id = self._create_sync_log("asset_financial", "FULL")
        result = {"synced": 0, "errors": 0, "total": 0}
        
        try:
            # 1. 获取目标市场所有股票
            with db_manager.meta_session() as session:
                statement = select(Asset.symbol).where(Asset.market == market)
                symbols = list(session.exec(statement).all())
            
            if limit:
                symbols = symbols[:limit]
            
            result["total"] = len(symbols)
            logger.info(f"Syncing financials for {len(symbols)} {market} stocks...")
            
            # 2. 爬取并解析
            count = 0
            for i, symbol in enumerate(symbols):
                try:
                    valuation_data = {}
                    
                    if market == "CN":
                        df = akshare_client.get_financial_abstract_cn(symbol)
                        if not df.empty:
                            # 提取最新一期指标
                            latest = df.iloc[0]
                            valuation_data.update({
                                "total_revenue": self._safe_float(latest.get("营业总收入(元)")),
                                "net_profit": self._safe_float(latest.get("净利润(元)")),
                                "roe": self._safe_float(latest.get("净资产收益率(%)")),
                                "gross_profit_margin": self._safe_float(latest.get("销售毛利率(%)")),
                                "revenue_yoy": self._safe_float(latest.get("营业总收入同比增长率(%)")),
                                "net_profit_yoy": self._safe_float(latest.get("净利润同比增长率(%)")),
                            })
                            
                    elif market == "HK":
                        df = akshare_client.get_financial_hk(symbol)
                        if not df.empty and len(df) > 0:
                            latest = dict(zip(df['指标'], df[df.columns[1]])) # 第一列为最新周期
                            valuation_data.update({
                                "total_revenue": self._safe_float(latest.get("营业额")),
                                "net_profit": self._safe_float(latest.get("归母净利润")),
                                "roe": self._safe_float(latest.get("加权净资产收益率(%)")),
                                "revenue_yoy": self._safe_float(latest.get("营业额同比增长率(%)")),
                                "net_profit_yoy": self._safe_float(latest.get("净利润同比增长率(%)")),
                            })

                    elif market == "US":
                        df = akshare_client.get_financial_us(symbol)
                        if not df.empty and "收入" in df.columns:
                            # 雪球美股指标较简单
                            latest = df.iloc[0]
                            valuation_data.update({
                                "total_revenue": self._safe_float(latest.get("收入")),
                                "net_profit": self._safe_float(latest.get("净利润")),
                                "roe": self._safe_float(latest.get("ROE")),
                            })
                    
                    if valuation_data:
                        self._sync_one_stock(symbol, valuation_data)
                        count += 1
                        
                    if (i + 1) % 50 == 0:
                        logger.info(f"Progress: {i + 1}/{len(symbols)} ({market})")
                        
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
                    result["errors"] += 1

            result["synced"] = count
            self._update_sync_log(sync_log_id, "SUCCESS", record_count=count)
            logger.info(f"Financial sync completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Financial sync failed: {e}")
            self._update_sync_log(sync_log_id, "FAILED", error_msg=str(e))
            raise

financial_syncer = FinancialSyncer()
