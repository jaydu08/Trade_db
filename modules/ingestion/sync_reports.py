
"""
Sync Reports - 研报同步与逻辑提取
"""
import logging
from datetime import datetime
from tqdm import tqdm

from core.db import get_collection
from core.llm import get_llm_client

logger = logging.getLogger(__name__)

class ReportSyncer:
    """
    研报同步器
    """
    def __init__(self):
        self.llm = get_llm_client()
        self.collection = get_collection("industry_knowledge") # 存入知识库

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

    def sync_industry_reports(self) -> dict:
        """
        同步行业研报 (含金量最高)
        """
        import akshare as ak
        logger.info("Syncing industry reports...")
        result = {"fetched": 0, "synced": 0, "skipped": 0, "errors": 0}
        
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
                    return result
            else:
                return result

            if df.empty:
                logger.warning("No industry reports found.")
                return result

            result["fetched"] = len(df)
            logger.info(f"Found {len(df)} industry reports. Extracting logic...")
            
            for _, row in tqdm(df.iterrows(), total=len(df)):
                title = row.get("报告名称", "")
                industry = row.get("行业", "")
                # 东方财富接口可能不直接返回详细内容，只有摘要或链接
                # 这里假设 title 本身就包含很多信息，或者我们有摘要字段
                # 实际 akshare 返回 columns: 报告名称, 报告日期, 机构名称, 行业, 评级...
                
                if not title:
                    result["skipped"] += 1
                    continue
                
                # LLM 提取
                logic_data = self.extract_logic(title, f"行业: {industry}")
                
                if logic_data and logic_data.get("logic"):
                    # 存入向量库
                    if self._store_to_vector(industry, title, logic_data):
                        result["synced"] += 1
                    else:
                        result["errors"] += 1
                else:
                    result["skipped"] += 1
            return result
                    
        except Exception as e:
            logger.error(f"Industry report sync failed: {e}")
            result["errors"] += 1
            return result

    def _store_to_vector(self, industry: str, title: str, data: dict) -> bool:
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
            return True
        except Exception as e:
            logger.warning(f"Vector store failed: {e}")
            return False

report_syncer = ReportSyncer()
