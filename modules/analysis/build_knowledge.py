
"""
Build Knowledge - 产业链知识库构建
"""
import logging
import sys
import os
from typing import List, Tuple
from datetime import datetime
import json
import uuid
from tqdm import tqdm
from sqlmodel import select, func

# Add project root to path
sys.path.append(os.getcwd())

from core.llm import get_llm_client
from core.db import db_manager, get_collection
from domain.meta import Concept, AssetConceptLink, Asset
from domain.vector import IndustryKnowledgeDocument, IndustryKnowledgeMetadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KnowledgeBuilder:
    """
    知识库构建器
    利用 LLM 将概念板块转化为结构化的产业链知识
    """
    def __init__(self):
        self.llm = get_llm_client()
        self.collection = get_collection("industry_knowledge")

    def get_top_concepts(self, limit: int = 20) -> List[Tuple[str, str, int]]:
        """获取热门概念板块 (按关联股票数排序)"""
        with db_manager.meta_session() as session:
            # Note: We need to group by Concept.code and other fields
            statement = (
                select(Concept.code, Concept.name, func.count(AssetConceptLink.symbol).label("count"))
                .join(AssetConceptLink, Concept.code == AssetConceptLink.concept_code)
                .group_by(Concept.code, Concept.name)
                .order_by(func.count(AssetConceptLink.symbol).desc())
                .limit(limit)
            )
            results = session.exec(statement).all()
            return results

    def decompose_concept(self, concept_name: str, stock_examples: List[str]) -> dict:
        """
        利用 LLM 拆解概念为产业链
        """
        # If no examples, just use concept name
        examples_str = ', '.join(stock_examples[:5]) if stock_examples else "无具体示例"
        
        prompt = f"""
        你是一个资深的产业链分析师。请分析"{concept_name}"这个概念板块。
        
        该板块包含的典型公司有: {examples_str}。

        请将该产业拆解为上游、中游、下游三个环节，并提取每个环节的关键技术/产品关键词。
        
        要求：
        1. 分析要有深度，不要只列出通用的词汇。
        2. 必须包含关键的原材料、核心设备、技术标准。
        3. 对于智能制造、AI等复杂产业，请细分到具体的模块（如光模块、液冷、封装等）。
        
        请严格以 JSON 格式输出，不要包含 Markdown 标记：
        {{
            "definition": "简短定义该产业",
            "chain": [
                {{
                    "position": "upstream",
                    "name": "上游环节名称",
                    "keywords": ["关键词1", "关键词2", "关键词3"],
                    "description": "描述上游主要做什么，包含哪些核心材料或设备"
                }},
                {{
                    "position": "midstream",
                    "name": "中游环节名称",
                    "keywords": ["关键词1", "关键词2"],
                    "description": "描述中游主要做什么，包含哪些制造或封装环节"
                }},
                {{
                    "position": "downstream",
                    "name": "下游环节名称",
                    "keywords": ["关键词1", "关键词2"],
                    "description": "描述下游主要做什么，包含哪些应用场景"
                }}
            ],
            "trends": ["行业趋势1", "行业趋势2"]
        }}
        """
        
        try:
            # Need to mock LLM if not available or use real one
            # Assuming get_llm_client works
            response_text = self.llm.chat(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            # Cleanup json
            content = response_text.strip()
            # If wrapped in markdown code block, remove it
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
                
            return json.loads(content)
        except Exception as e:
            logger.error(f"LLM decomposition failed for {concept_name}: {e}")
            return None

    def store_knowledge(self, concept_code: str, concept_name: str, data: dict):
        """存储到向量库"""
        if not data:
            return

        now = datetime.utcnow().isoformat()
        
        ids = []
        documents = []
        metadatas = []

        # 1. 存定义
        def_id = f"know_{concept_code}_def"
        def_text = f"{concept_name}定义: {data.get('definition', '')}"
        
        ids.append(def_id)
        documents.append(def_text)
        metadatas.append({
            "knowledge_type": "chain_node",
            "industry": concept_name,
            "updated_at": now,
            "doc_version": 1
        })

        # 2. 存产业链环节
        for node in data.get("chain", []):
            pos = node.get("position", "midstream")
            if pos not in ["upstream", "midstream", "downstream"]:
                pos = "midstream"
                
            doc_id = f"know_{concept_code}_{pos}_{uuid.uuid4().hex[:8]}"
            text = f"{concept_name} {node.get('name', '')} ({pos}): {node.get('description', '')}. 关键词: {', '.join(node.get('keywords', []))}"
            
            ids.append(doc_id)
            documents.append(text)
            metadatas.append({
                "knowledge_type": "chain_node",
                "industry": concept_name,
                "node_position": pos,
                "node_name": node.get("name", ""),
                # Chroma metadata must be simple types (str, int, float, bool)
                # List is not supported in metadata usually, need to join
                "related_keywords": ",".join(node.get("keywords", [])),
                "source": "llm_generate",
                "updated_at": now,
                "doc_version": 1
            })

        # 3. 存趋势
        for i, trend in enumerate(data.get("trends", [])):
            doc_id = f"know_{concept_code}_trend_{i}"
            text = f"{concept_name}趋势: {trend}"
            
            ids.append(doc_id)
            documents.append(text)
            metadatas.append({
                "knowledge_type": "trend",
                "industry": concept_name,
                "source": "llm_generate",
                "updated_at": now,
                "doc_version": 1
            })

        # Add to Chroma
        if ids:
            try:
                self.collection.add(
                    ids=ids,
                    documents=documents,
                    metadatas=metadatas
                )
                logger.info(f"Stored {len(ids)} knowledge chunks for {concept_name}")
            except Exception as e:
                logger.error(f"Failed to store to Chroma: {e}")

    def build_for_top_concepts(self, limit: int = 5):
        """为主流概念构建知识库"""
        concepts = self.get_top_concepts(limit)
        print(f"Building knowledge for top {len(concepts)} concepts...")
        
        for code, name, count in tqdm(concepts):
            print(f"\nProcessing {name} ({count} stocks)...")
            
            # Get sample stocks
            try:
                with db_manager.meta_session() as session:
                    statement = (
                        select(Asset.name)
                        .join(AssetConceptLink, Asset.symbol == AssetConceptLink.symbol)
                        .where(AssetConceptLink.concept_code == code)
                        .limit(5)
                    )
                    examples = list(session.exec(statement).all())
                
                # Decompose
                data = self.decompose_concept(name, examples)
                
                # Store
                if data:
                    self.store_knowledge(code, name, data)
            except Exception as e:
                logger.error(f"Error processing {name}: {e}")

if __name__ == "__main__":
    builder = KnowledgeBuilder()
    builder.build_for_top_concepts(limit=3)
