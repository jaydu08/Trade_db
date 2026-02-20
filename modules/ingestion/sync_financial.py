
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
                    report_date=date.today(), # Using today as we are syncing realtime valuation
                    updated_at=datetime.utcnow().isoformat()
                )
            
            # Update fields
            if "pe_ttm" in valuation_data: financial.pe_ttm = valuation_data["pe_ttm"]
            if "pb" in valuation_data: financial.pb = valuation_data["pb"]
            if "market_cap" in valuation_data: financial.market_cap = valuation_data["market_cap"]
            if "dv_ratio" in valuation_data: financial.dv_ratio = valuation_data["dv_ratio"]
            
            financial.updated_at = datetime.utcnow().isoformat()
            session.add(financial)
            session.commit()

    def sync_financials(self, limit: Optional[int] = None, market: str = "CN") -> dict:
        """
        同步财务数据 (目前仅支持 A 股实时估值数据同步)
        """
        if market != "CN":
            logger.warning(f"Financial sync for {market} not fully supported yet.")
            return {"synced": 0, "errors": 0}

        sync_log_id = self._create_sync_log("asset_financial", "FULL")
        result = {"synced": 0, "errors": 0, "total": 0}
        
        try:
            # 1. 获取 A 股所有股票
            with db_manager.meta_session() as session:
                statement = select(Asset.symbol).where(Asset.market == market)
                symbols = list(session.exec(statement).all())
            
            if limit:
                symbols = symbols[:limit]
            
            result["total"] = len(symbols)
            logger.info(f"Syncing financials for {len(symbols)} stocks...")
            
            # 2. 批量获取实时行情数据作为基础估值 (PE/PB/市值)
            # stock_zh_a_spot_em: 代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,振幅,最高,最低,今开,昨收,量比,换手率,市盈率-动态,市净率,总市值,流通市值,涨速,5分钟涨跌,60日涨跌幅,年初至今涨跌幅
            logger.info("Fetching realtime quotes from AkShare...")
            try:
                spot_df = akshare_client.get_realtime_quotes()
                logger.info(f"Got {len(spot_df)} records.")
            except Exception as e:
                logger.error(f"Failed to get realtime quotes: {e}")
                spot_df = pd.DataFrame()

            # 3. 批量更新
            count = 0
            if not spot_df.empty:
                # Convert symbols to set for fast lookup
                target_symbols = set(symbols)
                
                for _, row in spot_df.iterrows():
                    symbol = str(row["代码"])
                    if symbol not in target_symbols:
                        continue
                        
                    try:
                        valuation_data = {
                            "pe_ttm": self._safe_float(row.get("市盈率-动态")),
                            "pb": self._safe_float(row.get("市净率")),
                            "market_cap": self._safe_float(row.get("总市值")),
                            # AkShare spot doesn't usually have dividend yield directly in this table
                        }
                        
                        self._sync_one_stock(symbol, valuation_data)
                        count += 1
                        if count % 100 == 0:
                            logger.info(f"Synced {count}/{len(symbols)} financials")
                            
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
