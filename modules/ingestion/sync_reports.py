
"""
Sync Reports - 研报同步与逻辑提取
"""
import logging
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

from core.db import db_manager, get_collection
from core.llm import get_llm_client
from modules.ingestion.akshare_client import akshare_client
from domain.vector import IndustryKnowledgeMetadata

logger = logging.getLogger(__name__)

class ReportSyncer:
    """
    研报同步器
    """
    def __init__(self):
        self.llm = get_llm_client()
        self.collection = get_collection("industry_knowledge") # 存入知识库

    def fetch_latest_reports(self, limit: int = 50) -> pd.DataFrame:
        """获取最近的研报"""
        logger.info("Fetching latest research reports...")
        try:
            # 东方财富-研报中心-个股研报
            df = akshare_client._safe_call(["stock_report_fund_em"], symbol="全部", date="最近24小时")
            # 如果接口变动，可能需要调整参数或使用其他接口
            # 备选: stock_news_em (个股新闻), stock_report_disclosure (公告)
            # AkShare 研报接口较多，这里尝试通过 report 关键字查找最稳定的
            # 实际上 ak.stock_em_yjyg (业绩预告) 也是一种，但我们更想要 深度研报
            
            # Let's try to get industry reports which contain more chain logic
            # ak.stock_report_industry_em()
            return pd.DataFrame() # Placeholder, wait for test to find correct api
        except Exception as e:
            logger.warning(f"Failed to fetch reports: {e}")
            return pd.DataFrame()

    def extract_logic(self, title: str, content: str) -> dict:
        """
        利用 LLM 从研报中提取产业链逻辑
        """
        prompt = f"""
        请分析以下研报摘要，提取其中的产业链逻辑或关键投资逻辑。
        
        标题: {title}
        摘要: {content}
        
        请提取:
        1. 提及的产业链环节 (如: 光模块, 算力芯片)
        2. 提及的核心公司
        3. 核心逻辑 (一句话总结)
        
        以 JSON 格式输出: {{ "chain_nodes": [], "companies": [], "logic": "" }}
        """
        try:
            # 使用 simple_prompt 快速获取
            res = self.llm.simple_prompt(prompt, temperature=0.1)
            # Simple parsing (robustness needed in prod)
            import json
            if "```json" in res:
                res = res.split("```json")[1].split("```")[0]
            elif "```" in res:
                res = res.split("```")[1].split("```")[0]
            return json.loads(res.strip())
        except Exception:
            return {}

    def sync_industry_reports(self):
        """
        同步行业研报 (含金量最高)
        """
        import akshare as ak
        logger.info("Syncing industry reports...")
        
        try:
            # 获取最近的行业研报
            # 接口: stock_report_industry_em
            # Note: AkShare API might have changed. Checking safe call or alternatives.
            # Using stock_report_fund_em as a generic report interface often works for strategy reports too
            # Or try `ak.stock_em_hyyb` (old name) or check docs
            
            # Let's try `stock_report_fund_em` but filtering for "行业研报" if possible, 
            # or just get latest generic reports.
            # Actually `stock_report_industry_eastmoney` is likely the one.
            
            # Let's try a safer way:
            if hasattr(ak, "stock_report_industry_eastmoney"):
                df = ak.stock_report_industry_eastmoney()
            elif hasattr(ak, "stock_report_fund_em"):
                 # This is "fund" report, might not be industry.
                 # Let's use `stock_zh_a_spot_em`? No.
                 # `stock_news_em` is news.
                 
                 # Fallback: fetch concept board news?
                 # Let's try `stock_report_disclosure` which is official announcements.
                 
                 # Let's try to search for "industry" in ak
                 candidates = [f for f in dir(ak) if "report" in f and "industry" in f]
                 if candidates:
                     func = getattr(ak, candidates[0])
                     df = func()
                 else:
                     logger.warning("No industry report API found.")
                     return
            else:
                 return

            if df.empty:
                logger.warning("No industry reports found.")
                return

            print(f"Found {len(df)} industry reports. Extracting logic...")
            
            for _, row in tqdm(df.iterrows(), total=len(df)):
                title = row.get("报告名称", "")
                industry = row.get("行业", "")
                # 东方财富接口可能不直接返回详细内容，只有摘要或链接
                # 这里假设 title 本身就包含很多信息，或者我们有摘要字段
                # 实际 akshare 返回 columns: 报告名称, 报告日期, 机构名称, 行业, 评级...
                
                if not title: continue
                
                # LLM 提取
                logic_data = self.extract_logic(title, f"行业: {industry}")
                
                if logic_data and logic_data.get("logic"):
                    # 存入向量库
                    self._store_to_vector(industry, title, logic_data)
                    
        except Exception as e:
            logger.error(f"Industry report sync failed: {e}")

    def _store_to_vector(self, industry: str, title: str, data: dict):
        """存入向量库"""
        now = datetime.utcnow().isoformat()
        doc_id = f"report_{hash(title)}"
        
        text = f"【研报逻辑】{industry}: {data['logic']}. 涉及环节: {', '.join(data['chain_nodes'])}. 涉及公司: {', '.join(data['companies'])}"
        
        # 使用 IndustryKnowledgeMetadata
        meta = {
            "knowledge_type": "trend", # 研报通常代表趋势
            "industry": industry or "通用",
            "source": "research_report",
            "source_ref": title,
            "related_keywords": ",".join(data.get("chain_nodes", [])),
            "related_symbols": ",".join(data.get("companies", [])), # 这里应该是名称，后续需转代码
            "updated_at": now
        }
        
        try:
            self.collection.add(
                ids=[doc_id],
                documents=[text],
                metadatas=[meta]
            )
            logger.info(f"Stored report logic: {title[:20]}...")
        except Exception as e:
            logger.warning(f"Vector store failed: {e}")

report_syncer = ReportSyncer()
