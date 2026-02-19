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
from core.db import db_manager
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
"""
        
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
        步骤2: 向量检索映射到股票
        
        Args:
            chain: 产业链拆解结果
            top_k: 每个关键词返回 top K 个结果
        
        Returns:
            股票匹配列表
        """
        logger.info("Mapping chain nodes to stocks...")
        
        matches: List[StockMatch] = []
        seen_symbols = set()
        
        for node in chain.nodes:
            for keyword in node.keywords:
                # 向量检索
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
                    
                    # 计算匹配度（距离越小越好）
                    match_score = max(0, 1 - distance)
                    
                    matches.append(StockMatch(
                        symbol=symbol,
                        name=name,
                        match_score=match_score,
                        match_reason=f"匹配关键词: {keyword}",
                        node_position=node.position,
                    ))
                    
                    seen_symbols.add(symbol)
        
        # 按匹配度排序
        matches.sort(key=lambda x: x.match_score, reverse=True)
        
        logger.info(f"Mapped to {len(matches)} stocks")
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
