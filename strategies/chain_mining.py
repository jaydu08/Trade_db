"""
Chain Mining Strategy - 产业链挖掘策略

核心流程：
1. LLM 拆解产业链 -> 上下游关键词
2. 向量检索 -> 匹配公司
3. 实时行情过滤 -> 可交易标的
4. 生成信号 -> 写入 Ledger
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from core.llm import get_llm_client
from core.db import db_manager, get_collection
from domain.meta import Concept, AssetConceptLink, Asset
from domain.ledger import Signal
from modules.ingestion.sync_profile import profile_syncer
from modules.probing.market import market_prober

logger = logging.getLogger(__name__)


# ============================================================
# Pydantic 模型 - LLM 结构化输出
# ============================================================

class ChainNode(BaseModel):
    """产业链节点"""
    position: str = Field(description="位置：upstream/midstream/downstream")
    keywords: List[str] = Field(description="关键词列表")
    description: str = Field(description="节点描述")


class ChainDecomposition(BaseModel):
    """产业链拆解结果"""
    industry: str = Field(description="产业名称")
    nodes: List[ChainNode] = Field(description="产业链节点列表")
    reasoning: str = Field(description="拆解推理过程")


class StockMatch(BaseModel):
    """股票匹配结果"""
    symbol: str
    name: str
    match_score: float  # 匹配度 0-1
    match_reason: str  # 匹配原因
    node_position: str  # 所属节点位置


# ============================================================
# 产业链挖掘器
# ============================================================

class ChainMiningStrategy:
    """
    产业链挖掘策略
    
    输入：产业名称（如"AI眼镜"）
    输出：可交易标的列表 + 信号
    """
    
    def __init__(self, strategy_name: str = "ChainMining_v1"):
        self.strategy_name = strategy_name
        self.llm = get_llm_client()
        self.knowledge_col = get_collection("industry_knowledge")
    
    def decompose_chain(self, industry: str) -> ChainDecomposition:
        """
        步骤1: 使用 LLM 拆解产业链
        
        Args:
            industry: 产业名称
        
        Returns:
            产业链拆解结果
        """
        logger.info(f"Decomposing industry chain: {industry}")
        
        if not self.llm.is_available():
            raise RuntimeError("LLM not available. Please set OPENAI_API_KEY in .env")
        
        system_prompt = """你是一个产业链分析专家。
你的任务是将给定的产业拆解为上游、中游、下游三个环节，并提取每个环节的关键词。

关键词要求：
1. 具体的技术、材料、零部件名称
2. 避免过于宽泛的词汇
3. 每个环节 2-5 个关键词

示例：
产业：AI眼镜
- 上游：光波导、Micro-LED、衍射光学元件
- 中游：模组组装、光学设计、系统集成
- 下游：品牌商、渠道商

【参考知识库】
以下是从本地行业知识库检索到的相关信息（如果有）：
{knowledge_context}
"""
        
        # RAG: 检索本地行业知识
        knowledge_context = "暂无直接相关的本地知识。"
        try:
            results = self.knowledge_col.query(
                query_texts=[industry],
                n_results=5
            )
            if results and results.get("documents") and results["documents"][0]:
                docs = results["documents"][0]
                knowledge_context = "\n".join([f"- {doc}" for doc in docs])
                logger.info(f"Retrieved {len(docs)} knowledge chunks for RAG.")
        except Exception as e:
            logger.warning(f"Failed to query knowledge collection: {e}")

        system_prompt = system_prompt.format(knowledge_context=knowledge_context)
        
        user_prompt = f"请拆解产业链：{industry}"
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        
        try:
            result = self.llm.structured_output(
                messages=messages,
                response_model=ChainDecomposition,
            )
            
            logger.info(f"Chain decomposed: {len(result.nodes)} nodes")
            return result
        
        except Exception as e:
            logger.error(f"Chain decomposition failed: {e}")
            raise
    
    def map_to_stocks(
        self,
        chain: ChainDecomposition,
        top_k: int = 5,
    ) -> List[StockMatch]:
        """
        步骤2: 向量检索映射 + 全网新闻校验 (双轨制)
        
        Args:
            chain: 产业链拆解结果
            top_k: 每个关键词返回 top K 个结果
        
        Returns:
            股票匹配列表
        """
        logger.info("Mapping chain nodes to stocks via Local DB and Live Web Search...")
        from modules.ingestion.data_factory import data_manager
        from core.llm import simple_prompt
        import re
        
        matches: List[StockMatch] = []
        seen_symbols = set()
        
        for node in chain.nodes:
            # --- Path A: Local ChromaDB Vector Search ---
            for keyword in node.keywords:
                search_results = profile_syncer.search_companies(
                    query=keyword,
                    n_results=top_k,
                )
                
                for result in search_results:
                    metadata = result.get("metadata", {})
                    symbol = metadata.get("symbol")
                    name = metadata.get("name")
                    distance = result.get("distance", 1.0)
                    
                    if not symbol or symbol in seen_symbols:
                        continue
                    
                    match_score = max(0, 1 - distance)
                    
                    matches.append(StockMatch(
                        symbol=symbol,
                        name=name,
                        match_score=match_score,
                        match_reason=f"[本地知识库] 匹配关键词: {keyword}",
                        node_position=node.position,
                    ))
                    seen_symbols.add(symbol)
            
            # --- Path B: Live Web Search targeted discovery ---
            # Construct query to find leading companies for this node's keywords
            kw_str = " ".join(node.keywords)
            live_query = f"{chain.industry} {node.position} {kw_str} 龙头企业 核心股票名单"
            live_news = data_manager.search(live_query, limit_per_source=3)
            
            # Use LLM to extract stock symbols from the live news
            extraction_prompt = f"""
            以下是关于 "{chain.industry}" 产业链 {node.position} 环节 ({kw_str}) 的最新全网新闻搜索结果。
            请从中提取出被明确提及为受益股、龙头股的上市公司股票代码（只返回代码列表，逗号分隔，不要前缀和多余文字，例如: 600519.SH, AAPL, 00700.HK）。
            如果新闻中没有明确指出股票代码，但提到了公司名称，请尽你所能推断其合法股票代码。如果完全没有，返回 'NONE'。
            
            新闻内容:
            {live_news}
            """
            
            extracted_symbols_raw = simple_prompt(prompt=extraction_prompt, temperature=0.1)
            
            if extracted_symbols_raw and "NONE" not in extracted_symbols_raw.upper():
                # Naive regex to clean up potential formatting and extract potential symbols
                found_symbols = re.findall(r'[A-Za-z0-9]+\.[A-Za-z]+|[A-Za-z]+|\d{5,6}', extracted_symbols_raw)
                for sym in found_symbols:
                    sym = sym.strip()
                    if sym and sym not in seen_symbols and len(sym) >= 2:
                        # Append live matched stock. Note: "name" is generic here since we only have the symbol.
                        matches.append(StockMatch(
                            symbol=sym,
                            name="Live Search Match", 
                            match_score=0.85, # Assign high confidence for live market consensus
                            match_reason=f"[实时全网搜索] 由于 {kw_str} 被媒体集中报道为龙头/受益标的。",
                            node_position=node.position
                        ))
                        seen_symbols.add(sym)
        
        matches.sort(key=lambda x: x.match_score, reverse=True)
        logger.info(f"Mapped to {len(matches)} stocks (Local + Live)")
        return matches


    def cross_validate_with_meta(
        self,
        industry: str,
        matches: List[StockMatch],
        bonus_score: float = 0.2
    ) -> List[StockMatch]:
        """
        步骤2.5: 根据 Meta DB 中的 Concept 数据进行结构化校准
        """
        logger.info("Cross-validating stock matches with meta DB...")
        
        from sqlmodel import select
        
        symbols = [m.symbol for m in matches]
        if not symbols:
            return matches
            
        validated_symbols = set()
        try:
            with db_manager.meta_session() as session:
                # 模糊查找相关概念板块
                concept_stmt = select(Concept.code).where(Concept.name.like(f"%{industry}%"))
                concept_codes = session.exec(concept_stmt).all()
                
                if concept_codes:
                    # 查找哪些 symbol 在这些概念板块里
                    link_stmt = select(AssetConceptLink.symbol).where(
                        AssetConceptLink.concept_code.in_(concept_codes),
                        AssetConceptLink.symbol.in_(symbols)
                    )
                    validated_symbols = set(session.exec(link_stmt).all())
                    logger.info(f"Found {len(validated_symbols)} stocks overlapping with meta definitions.")
        except Exception as e:
            logger.warning(f"Failed to cross-validate with meta DB: {e}")
            
        # 修正打分
        for match in matches:
            if match.symbol in validated_symbols:
                match.match_score = min(1.0, match.match_score + bonus_score)
                match.match_reason += f" (结构化校验通过: 存在相关板块)"
                
        # 重新排序
        matches.sort(key=lambda x: x.match_score, reverse=True)
        return matches
    
    def filter_by_market(
        self,
        matches: List[StockMatch],
        min_amount: float = 10_000_000,
        min_turnover: float = 0.5,
    ) -> List[Dict[str, Any]]:
        """
        步骤3: 实时行情过滤
        
        Args:
            matches: 股票匹配列表
            min_amount: 最小成交额
            min_turnover: 最小换手率
        
        Returns:
            过滤后的股票列表（包含行情）
        """
        logger.info("Filtering by market conditions...")
        
        symbols = [m.symbol for m in matches]
        
        # 获取过滤后的行情
        quotes = market_prober.get_filtered_quotes(
            symbols=symbols,
            min_amount=min_amount,
            min_turnover=min_turnover,
            exclude_limit=True,
        )
        
        # 合并匹配信息和行情
        filtered = []
        for match in matches:
            if match.symbol in quotes:
                quote = quotes[match.symbol]
                filtered.append({
                    "symbol": match.symbol,
                    "name": match.name,
                    "match_score": match.match_score,
                    "match_reason": match.match_reason,
                    "node_position": match.node_position,
                    "price": quote["price"],
                    "change_pct": quote["change_pct"],
                    "amount": quote["amount"],
                    "turnover_rate": quote["turnover_rate"],
                })
        
        logger.info(f"Filtered to {len(filtered)} tradable stocks")
        return filtered
    
    def generate_signals(
        self,
        industry: str,
        stocks: List[Dict[str, Any]],
        chain: ChainDecomposition,
        strength_threshold: float = 0.6,
    ) -> List[Signal]:
        """
        步骤4: 生成交易信号
        
        Args:
            industry: 产业名称
            stocks: 过滤后的股票列表
            chain: 产业链拆解结果
            strength_threshold: 信号强度阈值
        
        Returns:
            信号列表
        """
        logger.info("Generating signals...")
        
        signals = []
        
        for stock in stocks:
            # 信号强度 = 匹配度
            strength = stock["match_score"]
            
            if strength < strength_threshold:
                continue
            
            # 构建推理过程
            reasoning = f"""
产业链挖掘策略 - {industry}

匹配信息：
- 匹配度: {strength:.2f}
- 匹配原因: {stock['match_reason']}
- 产业链位置: {stock['node_position']}

行情信息：
- 当前价格: {stock['price']:.2f}
- 涨跌幅: {stock['change_pct']:.2f}%
- 成交额: {stock['amount']/100000000:.2f}亿
- 换手率: {stock['turnover_rate']:.2f}%

产业链拆解：
{chain.reasoning}
"""
            
            signal = Signal(
                timestamp=datetime.utcnow(),
                strategy=self.strategy_name,
                symbol=stock["symbol"],
                direction="LONG",
                strength=strength,
                reasoning=reasoning.strip(),
                status="PENDING",
            )
            
            signals.append(signal)
        
        logger.info(f"Generated {len(signals)} signals")
        return signals
    
    def save_signals(self, signals: List[Signal]) -> int:
        """
        保存信号到数据库
        
        Args:
            signals: 信号列表
        
        Returns:
            保存的信号数量
        """
        if not signals:
            return 0
        
        with db_manager.ledger_session() as session:
            for signal in signals:
                session.add(signal)
        
        logger.info(f"Saved {len(signals)} signals to database")
        return len(signals)
    
    def run(
        self,
        industry: str,
        top_k: int = 10,
        min_amount: float = 10_000_000,
        strength_threshold: float = 0.6,
        save_to_db: bool = True,
    ) -> Dict[str, Any]:
        """
        执行完整的产业链挖掘流程
        
        Args:
            industry: 产业名称
            top_k: 每个关键词返回 top K 个结果
            min_amount: 最小成交额
            strength_threshold: 信号强度阈值
            save_to_db: 是否保存到数据库
        
        Returns:
            执行结果
        """
        logger.info(f"Running chain mining strategy for: {industry}")
        
        try:
            # 1. 拆解产业链
            chain = self.decompose_chain(industry)
            
            # 2. 映射到股票
            matches = self.map_to_stocks(chain, top_k=top_k)
            
            # 2.5 结构化校验
            matches = self.cross_validate_with_meta(industry, matches)
            
            # 3. 行情过滤
            filtered_stocks = self.filter_by_market(matches, min_amount=min_amount)
            
            # 4. 生成信号
            signals = self.generate_signals(
                industry=industry,
                stocks=filtered_stocks,
                chain=chain,
                strength_threshold=strength_threshold,
            )
            
            # 5. 保存信号
            if save_to_db:
                self.save_signals(signals)
            
            return {
                "industry": industry,
                "chain_nodes": len(chain.nodes),
                "matched_stocks": len(matches),
                "filtered_stocks": len(filtered_stocks),
                "signals_generated": len(signals),
                "chain": chain,
                "stocks": filtered_stocks,
                "signals": signals,
            }
        
        except Exception as e:
            logger.error(f"Chain mining failed: {e}")
            raise


# 全局实例
chain_mining_strategy = ChainMiningStrategy()


# 便捷函数
def mine_industry_chain(
    industry: str,
    top_k: int = 10,
    min_amount: float = 10_000_000,
    strength_threshold: float = 0.6,
) -> Dict[str, Any]:
    """产业链挖掘便捷函数"""
    return chain_mining_strategy.run(
        industry=industry,
        top_k=top_k,
        min_amount=min_amount,
        strength_threshold=strength_threshold,
    )
