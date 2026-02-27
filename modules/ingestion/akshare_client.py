"""
AkShare API 封装 - 带重试和错误处理
"""
import logging
import time
from typing import Optional, Callable, TypeVar
from functools import wraps
import requests

# 全局 Patch requests 增加默认 timeout 和 User-Agent，防止连接挂死和触发爬虫拦截
_original_request = requests.Session.request
def _patched_request(self, method, url, **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 15
    if 'headers' not in kwargs:
        kwargs['headers'] = {}
    if 'User-Agent' not in kwargs['headers'] and 'user-agent' not in kwargs['headers']:
        kwargs['headers']['User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    kwargs['headers']['Connection'] = 'close'
    return _original_request(self, method, url, **kwargs)
requests.Session.request = _patched_request

import pandas as pd
import akshare as ak

from config.settings import (
    AKSHARE_RETRY_TIMES,
    AKSHARE_RETRY_DELAY,
    AKSHARE_REQUEST_TIMEOUT,
)
from core.cache import cached

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry_on_error(
    max_retries: int = AKSHARE_RETRY_TIMES,
    delay: float = AKSHARE_RETRY_DELAY,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    重试装饰器
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_error: Optional[Exception] = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"AkShare call failed (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))  # 递增延迟
            
            logger.error(f"AkShare call failed after {max_retries} retries: {last_error}")
            raise last_error
        
        return wrapper
    return decorator


class AkShareClient:
    """
    AkShare 客户端封装
    
    提供常用的市场数据获取方法
    """
    
    # ================================================================
    # 股票列表
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_info", ttl=3600)  # 缓存1小时
    def get_stock_info_a() -> pd.DataFrame:
        """
        获取 A 股股票列表
        
        Returns:
            DataFrame with columns: 代码, 名称, 上市日期, ...
        """
        logger.info("Fetching A-share stock list...")
        df = ak.stock_info_a_code_name()
        logger.info(f"Fetched {len(df)} A-share stocks")
        return df

    @staticmethod
    def _fetch_bulk_sina(market: str) -> pd.DataFrame:
        """从本地库读取股票列表，分批向新浪请求实时行情"""
        import sqlite3
        import requests
        import concurrent.futures
        
        # 尝试从 meta.db 获取资产列表
        try:
            conn = sqlite3.connect("data/meta.db")
            cursor = conn.cursor()
            cursor.execute("SELECT symbol, name FROM asset WHERE market=?", (market,))
            rows = cursor.fetchall()
            conn.close()
        except:
            return pd.DataFrame()
            
        query_map = {}
        for row in rows:
            sym, name = row[0], row[1]
            if market == "CN":
                s = f"sh{sym}" if sym.startswith("6") else f"sz{sym}"
            elif market == "HK":
                s = f"hk{sym}"
            elif market == "US":
                clean_sym = sym.split(".")[-1]
                s = f"gb_{clean_sym.lower().replace('.', '$')}"
            query_map[s] = (sym, name)
            
        sina_symbols = list(query_map.keys())
        batch_size = 400
        batches = [sina_symbols[i:i+batch_size] for i in range(0, len(sina_symbols), batch_size)]
        
        results = []
        def fetch_batch(batch):
            try:
                url = "http://hq.sinajs.cn/list=" + ",".join(batch)
                resp = requests.get(url, headers={"Referer": "http://finance.sina.com.cn"}, timeout=10)
                return resp.text.splitlines()
            except: return []
                
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for text_lines in executor.map(fetch_batch, batches):
                for line in text_lines:
                    if '=""' in line or '="' not in line: continue
                    try:
                        parts = line.split('="')[1].split('";')[0].split(',')
                        s_id = line.split('="')[0].split('hq_str_')[1]
                        sym, name = query_map.get(s_id, ("", ""))
                        if not sym: continue
                        
                        price, pct_chg, amount, turnover = 0.0, 0.0, 0.0, 0.0
                        
                        if market == "CN":
                            if len(parts) < 32: continue
                            prev = float(parts[2])
                            price = float(parts[3])
                            amount = float(parts[9])
                            if prev > 0: pct_chg = round((price - prev) / prev * 100, 2)
                        elif market == "HK":
                            if len(parts) < 12: continue
                            price = float(parts[6])
                            pct_chg = float(parts[8])
                            amount = float(parts[11])
                        elif market == "US":
                            if len(parts) < 11: continue
                            price = float(parts[1])
                            pct_chg = float(parts[2])
                            amount = float(parts[10]) * price
                            
                        results.append({
                            "代码": sym,
                            "名称": name,
                            "最新价": price,
                            "涨跌幅": pct_chg,
                            "成交额": amount,
                            "换手率": turnover
                        })
                    except: continue
                    
        return pd.DataFrame(results)

    @staticmethod
    @cached("ak_stock_hk", ttl=3600)
    def get_stock_info_hk() -> pd.DataFrame:
        logger.info("Fetching HK stock list using Sina batch...")
        return AkShareClient._fetch_bulk_sina("HK")

    @staticmethod
    @cached("ak_stock_us", ttl=3600)
    def get_stock_info_us() -> pd.DataFrame:
        logger.info("Fetching US stock list using Sina batch...")
        return AkShareClient._fetch_bulk_sina("US")
    
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_sh", ttl=3600)
    def get_stock_info_sh() -> pd.DataFrame:
        """获取上海证券交易所股票列表"""
        logger.info("Fetching Shanghai stock list...")
        df = ak.stock_info_sh_name_code()
        logger.info(f"Fetched {len(df)} SH stocks")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_sz", ttl=3600)
    def get_stock_info_sz() -> pd.DataFrame:
        """获取深圳证券交易所股票列表"""
        logger.info("Fetching Shenzhen stock list...")
        df = ak.stock_info_sz_name_code()
        logger.info(f"Fetched {len(df)} SZ stocks")
        return df
    
    # ================================================================
    # 概念板块
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_concept_board", ttl=3600)
    def get_concept_board_list() -> pd.DataFrame:
        """
        获取东方财富概念板块列表
        
        Returns:
            DataFrame with columns: 排名, 板块名称, 板块代码, ...
        """
        logger.info("Fetching concept board list...")
        df = ak.stock_board_concept_name_em()
        logger.info(f"Fetched {len(df)} concept boards")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_concept_cons", ttl=3600)
    def get_concept_constituents(board_name: str) -> pd.DataFrame:
        """
        获取概念板块成分股
        
        Args:
            board_name: 板块名称
        
        Returns:
            DataFrame with constituents
        """
        logger.info(f"Fetching constituents for concept: {board_name}")
        df = ak.stock_board_concept_cons_em(symbol=board_name)
        logger.info(f"Fetched {len(df)} constituents for {board_name}")
        return df
    
    # ================================================================
    # 行业板块
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_industry_board", ttl=3600)
    def get_industry_board_list() -> pd.DataFrame:
        """
        获取东方财富行业板块列表
        
        Returns:
            DataFrame with columns: 排名, 板块名称, 板块代码, ...
        """
        logger.info("Fetching industry board list...")
        df = ak.stock_board_industry_name_em()
        logger.info(f"Fetched {len(df)} industry boards")
        return df
    
    @staticmethod
    @retry_on_error()
    @cached("ak_industry_cons", ttl=3600)
    def get_industry_constituents(board_name: str) -> pd.DataFrame:
        """
        获取行业板块成分股
        
        Args:
            board_name: 板块名称
        
        Returns:
            DataFrame with constituents
        """
        logger.info(f"Fetching constituents for industry: {board_name}")
        df = ak.stock_board_industry_cons_em(symbol=board_name)
        logger.info(f"Fetched {len(df)} constituents for {board_name}")
        return df
    
    # ================================================================
    # 公司信息
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_profile_cninfo", ttl=86400)
    def get_stock_profile_cninfo(symbol: str) -> pd.DataFrame:
        """
        获取个股公司简介（巨潮资讯）
        
        Args:
            symbol: 股票代码
        
        Returns:
            DataFrame with company profile
        """
        logger.info(f"Fetching cninfo profile for: {symbol}")
        try:
            df = ak.stock_profile_cninfo(symbol=symbol)
            return df
        except Exception as e:
            logger.warning(f"Failed to get cninfo profile for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    @retry_on_error()
    @cached("ak_stock_profile_hk", ttl=86400)
    def get_stock_profile_hk(symbol: str) -> pd.DataFrame:
        logger.info(f"Fetching HK profile for: {symbol}")
        # Try new interface for HK profile
        try:
            return ak.stock_hk_company_profile_em(symbol=symbol)
        except Exception as e:
            logger.warning(f"Failed to get HK profile for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    @retry_on_error()
    @cached("ak_stock_profile_us", ttl=86400)
    def get_stock_profile_us(symbol: str) -> pd.DataFrame:
        logger.info(f"Fetching US profile for: {symbol}")
        # Clean symbol for XQ interface (e.g. 105.RXT -> RXT)
        clean_symbol = symbol.split(".")[-1]
        try:
            return ak.stock_individual_basic_info_us_xq(symbol=clean_symbol)
        except Exception as e:
            logger.warning(f"Failed to get US profile for {symbol}: {e}")
            return pd.DataFrame()
    
    @staticmethod
    @retry_on_error()
    @cached("ak_stock_business", ttl=86400)
    def get_stock_business(symbol: str) -> pd.DataFrame:
        """
        获取个股主营业务
        
        Args:
            symbol: 股票代码
        
        Returns:
            DataFrame with business info
        """
        logger.info(f"Fetching business for: {symbol}")
        try:
            df = ak.stock_zyjs_ths(symbol=symbol)
            return df
        except Exception as e:
            logger.warning(f"Failed to get business for {symbol}: {e}")
            return pd.DataFrame()
    
    # ================================================================
    # 财务数据 (深度基本面)
    # ================================================================
    @staticmethod
    @retry_on_error()
    @cached("ak_financial_cn", ttl=86400)
    def get_financial_abstract_cn(symbol: str) -> pd.DataFrame:
        """
        获取 A 股财务摘要 (同花顺)
        包含 ROE, 毛利率, 净利润同比等核心指标
        """
        logger.info(f"Fetching financial abstract for CN: {symbol}")
        try:
            return ak.stock_financial_abstract_ths(symbol=symbol, indicator="按报告期")
        except Exception as e:
            logger.warning(f"Failed to get CN financial for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    @retry_on_error()
    @cached("ak_financial_hk", ttl=86400)
    def get_financial_hk(symbol: str) -> pd.DataFrame:
        """
        获取港股财务报表 (东方财富)
        """
        logger.info(f"Fetching financial report for HK: {symbol}")
        try:
            return ak.stock_financial_hk_report_em(symbol=symbol)
        except Exception as e:
            logger.warning(f"Failed to get HK financial for {symbol}: {e}")
            return pd.DataFrame()

    @staticmethod
    @retry_on_error()
    @cached("ak_financial_us", ttl=86400)
    def get_financial_us(symbol: str) -> pd.DataFrame:
        """
        获取美股财务指标 (雪球)
        """
        logger.info(f"Fetching financial report for US: {symbol}")
        # 雪球格式去头
        clean_symbol = symbol.split(".")[-1]
        try:
            return ak.stock_financial_us_xq(symbol=clean_symbol)
        except Exception as e:
            logger.warning(f"Failed to get US financial for {symbol}: {e}")
            return pd.DataFrame()

    # ================================================================
    # 实时行情 (Sina HQ API 替代方案)
    # ================================================================
    @staticmethod
    def get_realtime_quote_eastmoney(symbol: str, market: str = "CN") -> dict:
        """
        原为东方财富获取实时行情，由于IP被限，替换为新浪行情接口 (兼容原返回格式)。
        """
        import requests
        
        # 构建 Sina symbol
        sina_symbol = ""
        if market == "CN":
            if symbol.startswith("6"): sina_symbol = f"sh{symbol}"
            else: sina_symbol = f"sz{symbol}"
        elif market == "HK":
            sina_symbol = f"hk{symbol}"
        elif market == "US":
            sina_symbol = f"gb_{symbol.lower().replace('.', '$')}"
            
        url = f"http://hq.sinajs.cn/list={sina_symbol}"
        headers = {"Referer": "http://finance.sina.com.cn"}
        
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            text = resp.text.strip()
            if not text or "=\"\"" in text or "=\"\"," in text:
                return {}
                
            parts = text.split("=\"")[1].split("\";")[0].split(",")
            if len(parts) < 5: return {}
            
            name, price, change, pct_chg, ts = "", 0.0, 0.0, 0.0, ""
            
            if market == "CN":
                if len(parts) < 32: return {}
                name = parts[0]
                prev_close = float(parts[2])
                price = float(parts[3])
                if prev_close > 0:
                    change = price - prev_close
                    pct_chg = round((change / prev_close) * 100, 2)
                ts = f"{parts[30]} {parts[31]}"
            elif market == "HK":
                if len(parts) < 19: return {}
                name = parts[1]
                price = float(parts[6])
                change = float(parts[7])
                pct_chg = float(parts[8])
                ts = f"{parts[17]} {parts[18]}"
            elif market == "US":
                if len(parts) < 6: return {}
                name = parts[0]
                price = float(parts[1])
                pct_chg = float(parts[2])
                ts = parts[3]
                change = float(parts[4])
                
            return {
                "symbol": symbol,
                "name": name,
                "price": price,
                "change": change,
                "pct_chg": pct_chg,
                "timestamp": ts
            }
            
        except Exception as e:
            logger.warning(f"Sina quote failed for {symbol}: {e}")
            
        return {}

    @staticmethod
    @cached("ak_spot", ttl=60)  # 缓存60秒
    def get_realtime_quotes() -> pd.DataFrame:
        """
        [DEPRECATED] 批量轮询已被弃用。
        请针对少量自选池使用 AsyncMarketProber，对全市场榜单使用 get_daily_top_ranks()。
        """
        logger.warning("get_realtime_quotes() is deprecated due to high-frequency polling bans.")
        return pd.DataFrame()

    @staticmethod
    def _distill_ranks(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
        """
        本地精排算法 (Distillation Strategy)
        在取得了全市场粗排数据后，结合量价数据的多个维度（如涨幅、换手率、成交额）
        合成一个综合 Alpha Score，从而筛选出最具资金共识的标的。
        """
        if df.empty or len(df) <= top_n:
            return df
            
        # 为了简单，采用标准化打分体系
        # 提取候选池进行精排
        distillation_pool = df.copy()
        
        # 归一化处理 (Min-Max Scaling)
        for col in ["change_pct", "turnover_rate", "amount"]:
            if col in distillation_pool.columns:
                min_val = distillation_pool[col].min()
                max_val = distillation_pool[col].max()
                if max_val > min_val:
                    distillation_pool[f"{col}_norm"] = (distillation_pool[col] - min_val) / (max_val - min_val)
                else:
                    distillation_pool[f"{col}_norm"] = 0.0

        # 精排打分公式 (可以根据需要调整因子权重)
        # 这里给涨幅40%，换手率（活跃度）40%，成交额（流动性）20% 的权重
        distillation_pool["alpha_score"] = (
            distillation_pool.get("change_pct_norm", 0) * 0.4 +
            distillation_pool.get("turnover_rate_norm", 0) * 0.4 +
            distillation_pool.get("amount_norm", 0) * 0.2
        )
        
        # 按照综合评分精排
        distilled = distillation_pool.sort_values(by="alpha_score", ascending=False).head(top_n)
        return distilled

    @staticmethod
    @retry_on_error()
    @cached("daily_top_ranks", ttl=86400) # 一天只取一次全市场
    def get_daily_top_ranks(market: str = "CN", rank_type: str = "change_pct", top_n: int = 10) -> pd.DataFrame:
        """
        获取每日全市场 Top 排行榜数据
        
        Args:
            market: 市场类型 "CN", "HK", "US"
            rank_type: 排行类型 "change_pct" (涨幅), "amount" (成交额), "turnover" (换手率)
            top_n: 获取前 n 条
            
        Returns:
            DataFrame 包含 symbol, name, price, change_pct, amount, turnover_rate
        """
        logger.info(f"Fetching daily top {top_n} {rank_type} for market: {market} using AKShare (single request)...")
        
        try:
            if market == "CN":
                df = ak.stock_zh_a_spot_em()
                symbol_col, name_col, price_col, pct_col, amt_col, turn_col = "代码", "名称", "最新价", "涨跌幅", "成交额", "换手率"
            elif market == "HK":
                df = ak.stock_hk_spot_em()
                symbol_col, name_col, price_col, pct_col, amt_col, turn_col = "代码", "名称", "最新价", "涨跌幅", "成交额", "换手率"
            elif market == "US":
                df = ak.stock_us_spot_em()
                symbol_col, name_col, price_col, pct_col, amt_col, turn_col = "代码", "名称", "最新价", "涨跌幅", "成交额", "换手率"
            else:
                return pd.DataFrame()
                
            if df.empty:
                return pd.DataFrame()
                
            # 清理数据类型
            for col in [price_col, pct_col, amt_col, turn_col]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            
            # 排序
            if rank_type == "change_pct":
                sort_col = pct_col
            elif rank_type == "amount":
                sort_col = amt_col
            elif rank_type == "turnover":
                sort_col = turn_col
            else:
                sort_col = pct_col
                
            # 首先进行粗排，截取前 300 名进入精排池
            if sort_col in df.columns:
                df = df.sort_values(by=sort_col, ascending=False).head(300)
            
            # 使用算法精排，选出最终的 Top N
            df = AkShareClient._distill_ranks(df, top_n)
            
            # 标准化输出列
            res = pd.DataFrame({
                "symbol": df[symbol_col],
                "name": df[name_col],
                "price": df[price_col] if price_col in df.columns else 0.0,
                "change_pct": df[pct_col] if pct_col in df.columns else 0.0,
                "amount": df[amt_col] if amt_col in df.columns else 0.0,
                "turnover_rate": df[turn_col] if turn_col in df.columns else 0.0,
            })
            return res
            
        except Exception as e:
            logger.error(f"Failed to fetch daily top ranks for {market}: {e}")
            return pd.DataFrame()

    @staticmethod
    def _safe_call(func_names: list[str], **kwargs) -> pd.DataFrame:
        for name in func_names:
            func = getattr(ak, name, None)
            if func is None:
                continue
            try:
                return func(**kwargs)
            except Exception as e:
                logger.warning(f"AkShare call {name} failed: {e}")
        return pd.DataFrame()
    
    @staticmethod
    @retry_on_error()
    def get_stock_info_global_cls() -> pd.DataFrame:
        """获取财联社全球电报"""
        logger.info("Fetching CLS Global Telegraph...")
        # Note: akshare API might change, ensure we use the correct one
        try:
            return ak.stock_info_global_cls()
        except Exception as e:
            logger.warning(f"Failed to fetch global news: {e}")
            return pd.DataFrame()



# 全局实例
akshare_client = AkShareClient()
