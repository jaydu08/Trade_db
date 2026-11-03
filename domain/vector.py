"""
Vector Schema 定义 - ChromaDB 向量库的 Pydantic 模型

包含 4 个 Collection:
1. company_chunks - 公司画像库
2. industry_knowledge - 产业链知识库
3. market_events - 市场事件库
4. entity_relation - 实体关系库
"""
from datetime import date, datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field


# ============================================================
# Collection 1: company_chunks - 公司画像库
# ============================================================
class CompanyChunkMetadata(BaseModel):
    """公司画像 Chunk 元数据"""
    symbol: str = Field(..., description="证券代码")
    name: str = Field(..., description="证券名称")
    market: str = Field(default="CN", description="市场: CN/US/HK")
    chunk_type: Literal[
        "overview",      # 公司概况
        "business",      # 主营业务
        "products",      # 核心产品
        "tech_tags",     # 技术标签 (LLM提取)
        "supply_chain",  # 供应链信息
        "custom"         # 自定义
    ] = Field(..., description="Chunk类型")
    industry: Optional[str] = Field(default=None, description="所属行业")
    source: Literal["akshare", "manual", "llm_extract", "research"] = Field(
        default="akshare", description="数据来源"
    )
    confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="置信度")
    doc_version: int = Field(default=1, description="文档版本")
    updated_at: str = Field(..., description="更新时间")


class CompanyChunkDocument(BaseModel):
    """公司画像文档"""
    id: str = Field(..., description="文档ID: {symbol}_{chunk_type}_v{version}")
    document: str = Field(..., description="文档内容")
    metadata: CompanyChunkMetadata


# ============================================================
# Collection 2: industry_knowledge - 产业链知识库
# ============================================================
class IndustryKnowledgeMetadata(BaseModel):
    """产业链知识元数据"""
    knowledge_type: Literal[
        "chain_node",      # 产业链节点定义
        "chain_relation",  # 产业链结构关系
        "chain_path",      # 产业链传导路径 (带层级)
        "tech_route",      # 技术路线分析
        "company_role",    # 公司产业链定位
        "trend"            # 行业趋势
    ] = Field(..., description="知识类型")
    
    industry: str = Field(..., description="所属产业")
    node_position: Optional[Literal["upstream", "midstream", "downstream"]] = Field(
        default=None, description="产业链位置"
    )
    node_name: Optional[str] = Field(default=None, description="节点名称")
    
    # 传导路径相关 (chain_path 类型使用)
    trigger: Optional[str] = Field(default=None, description="触发事件")
    benefit_level: Optional[int] = Field(default=None, ge=1, le=5, description="受益层级 1-5")
    benefit_delay: Optional[Literal["immediate", "1week", "1month", "3month"]] = Field(
        default=None, description="受益时延"
    )
    transmission_logic: Optional[str] = Field(default=None, description="传导逻辑")
    
    # 关联信息
    related_keywords: list[str] = Field(default_factory=list, description="相关关键词")
    related_symbols: list[str] = Field(default_factory=list, description="关联股票代码")
    
    # 来源
    source: Literal["research", "manual", "llm_generate"] = Field(
        default="manual", description="数据来源"
    )
    source_ref: Optional[str] = Field(default=None, description="来源引用")
    confidence: float = Field(default=0.9, ge=0.0, le=1.0, description="置信度")
    author: str = Field(default="system", description="录入者")
    doc_version: int = Field(default=1, description="文档版本")
    updated_at: str = Field(..., description="更新时间")


class IndustryKnowledgeDocument(BaseModel):
    """产业链知识文档"""
    id: str = Field(..., description="文档ID")
    document: str = Field(..., description="知识内容")
    metadata: IndustryKnowledgeMetadata


# ============================================================
# Collection 3: market_events - 市场事件库
# ============================================================
class MarketEventMetadata(BaseModel):
    """市场事件元数据"""
    event_type: Literal[
        "policy",     # 政策
        "earnings",   # 业绩
        "product",    # 产品发布
        "ma",         # 并购重组
        "market",     # 市场异动
        "news"        # 一般新闻
    ] = Field(..., description="事件类型")
    
    event_date: str = Field(..., description="事件日期 YYYY-MM-DD")
    industries: list[str] = Field(default_factory=list, description="相关产业")
    related_symbols: list[str] = Field(default_factory=list, description="相关股票")
    
    impact: Literal["positive", "negative", "neutral"] = Field(
        default="neutral", description="影响方向"
    )
    impact_score: float = Field(default=0.5, ge=0.0, le=1.0, description="影响程度")
    
    keywords: list[str] = Field(default_factory=list, description="关键词")
    source: str = Field(default="unknown", description="来源媒体")
    source_url: Optional[str] = Field(default=None, description="来源链接")
    
    doc_version: int = Field(default=1, description="文档版本")
    created_at: str = Field(..., description="创建时间")


class MarketEventDocument(BaseModel):
    """市场事件文档"""
    id: str = Field(..., description="文档ID: evt_{date}_{event_hash}")
    document: str = Field(..., description="事件摘要")
    metadata: MarketEventMetadata


# ============================================================
# Collection 4: entity_relation - 实体关系库 (知识图谱)
# ============================================================
class EntityRelationMetadata(BaseModel):
    """实体关系元数据"""
    relation_type: Literal[
        "distribute",   # 发行 (电影→发行商)
        "produce",      # 出品 (电影→出品方)
        "supply",       # 供应 (供应商→客户)
        "invest",       # 投资 (投资方→被投公司)
        "cooperate",    # 合作
        "compete",      # 竞争
        "subsidiary",   # 子公司
        "custom"        # 自定义
    ] = Field(..., description="关系类型")
    
    # 实体1
    entity_1: str = Field(..., description="实体1名称")
    entity_1_type: Literal[
        "company", "movie", "product", "person", "event", "concept", "other"
    ] = Field(..., description="实体1类型")
    
    # 实体2
    entity_2: str = Field(..., description="实体2名称")
    entity_2_type: Literal[
        "company", "movie", "product", "person", "event", "concept", "other"
    ] = Field(..., description="实体2类型")
    
    # 关联股票
    symbol: Optional[str] = Field(default=None, description="关联股票代码")
    
    # 有效期
    valid_from: Optional[str] = Field(default=None, description="生效日期")
    valid_to: Optional[str] = Field(default=None, description="失效日期")
    
    # 来源
    source: Literal["manual", "news", "research", "llm"] = Field(
        default="manual", description="数据来源"
    )
    confidence: float = Field(default=0.9, ge=0.0, le=1.0, description="置信度")
    
    doc_version: int = Field(default=1, description="文档版本")
    created_at: str = Field(..., description="创建时间")


class EntityRelationDocument(BaseModel):
    """实体关系文档"""
    id: str = Field(..., description="文档ID: rel_{entity1}_{relation}_{entity2}")
    document: str = Field(..., description="关系描述")
    metadata: EntityRelationMetadata


# ============================================================
# 通用向量文档类型
# ============================================================
VectorDocument = CompanyChunkDocument | IndustryKnowledgeDocument | MarketEventDocument | EntityRelationDocument
