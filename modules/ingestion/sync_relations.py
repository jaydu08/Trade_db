import logging
import json
import datetime
from typing import List, Dict

from sqlmodel import select
from core.db import db_manager, get_collection
from domain.meta import AssetProfile
from core.llm import json_prompt

logger = logging.getLogger(__name__)

class RelationSyncer:
    """
    实体关系同步器 (核心图谱)
    通过 LLM 阅读财报主营业务或简介，抽取前五大客户、前五大供应商及主要竞争对手信息。
    """
    def __init__(self):
        self.collection = get_collection("entity_relation")

    def _generate_doc_id(self, entity1: str, relation: str, entity2: str) -> str:
        # 简化版生成一个去重的ID
        from hashlib import md5
        raw = f"{entity1}_{relation}_{entity2}"
        return f"rel_{md5(raw.encode('utf-8')).hexdigest()[:12]}"

    def _extract_relations(self, symbol: str, name: str, profile_text: str) -> List[Dict]:
        """调用 LLM 抽取关系"""
        sys_prompt = f"""
        你是一个专业的金融分析师。请阅读下面的公司业务描述，提取与该公司({name})相关的前五大客户、前五大供应商、以及核心竞争对手（如果有提及）。
        
        要求返回严格的 JSON 数组格式，禁止任何 Markdown 包装（不要用 ```json ），直接返回数组。
        每个对象必须包含以下字段:
        - entity_1: 固定为 "{name}"
        - relation_type: 必须是 "supply" (被这方供应/作为供应商), "distribute" (这方是客户/买家), "compete" (竞争对手), 或者 "cooperate" (战略合作) 中的一个
        - entity_2: 提取出的公司或实体名称
        - document: 10字以内的简单描述，如“前五大客户”
        
        示例输出:
        [
            {{"entity_1": "{name}", "relation_type": "distribute", "entity_2": "苹果公司", "document": "核心客户"}},
            {{"entity_1": "{name}", "relation_type": "supply", "entity_2": "宁德时代", "document": "电池供应商"}}
        ]
        
        如果文本中确实没有上述关系，请返回空数组 []。
        """
        
        user_prompt = f"公司简介：\n{profile_text}"
        
        try:
            resp = json_prompt(sys_prompt + "\n\n" + user_prompt)
            relations = json.loads(resp)
            if isinstance(relations, list):
                return relations
            return []
        except Exception as e:
            logger.warning(f"Failed to extract relations for {name} ({symbol}): {e}")
            return []

    def sync_relations_for_symbol(self, symbol: str) -> int:
        """为单只股票抽取并同步关系库"""
        with db_manager.meta_session() as session:
            profile = session.get(AssetProfile, symbol)
            if not profile:
                logger.warning(f"No profile found for {symbol}, cannot extract relations.")
                return 0
                
            # 我们主要看介绍和主业
            text_to_analyze = f"简介：{profile.company_profile or ''}\n主营业务：{profile.main_business or ''}"
            if len(text_to_analyze) < 50:
                logger.warning(f"Profile too short to analyze for {symbol}.")
                return 0

            from domain.meta import Asset
            asset = session.get(Asset, symbol)
            name = asset.name if asset else symbol
            
        relations = self._extract_relations(symbol, name, text_to_analyze)
        if not relations:
            logger.info(f"No relationships extracted for {name}.")
            return 0
            
        now = datetime.datetime.utcnow().isoformat()
        chunks_to_add = []
        
        # 构建文档
        for rel in relations:
            try:
                rel_type = rel.get("relation_type")
                ent2 = rel.get("entity_2")
                desc = rel.get("document", "")
                
                # 过滤不合格产出
                if not rel_type or not ent2 or ent2 == name:
                    continue
                    
                doc_id = self._generate_doc_id(name, rel_type, ent2)
                
                metdadata = {
                    "relation_type": rel_type,
                    "entity_1": name,
                    "entity_1_type": "company",
                    "entity_2": ent2,
                    "entity_2_type": "company",
                    "symbol": symbol,
                    "source": "llm",
                    "confidence": 0.8,
                    "doc_version": 1,
                    "created_at": now
                }
                
                chunks_to_add.append({
                    "id": doc_id,
                    "document": f"{name}的{desc}: {ent2}",
                    "metadata": metdadata
                })
            except Exception as e:
                logger.warning(f"Error parsing relation format: {e}")
                
        # 写入 ChromaDB
        if chunks_to_add:
            ids = [c["id"] for c in chunks_to_add]
            docs = [c["document"] for c in chunks_to_add]
            metas = [c["metadata"] for c in chunks_to_add]
            
            # 删除旧关系
            try:
                self.collection.delete(where={"symbol": symbol})
                logger.debug(f"Deleted old relations for {symbol}")
            except Exception:
                pass
                
            self.collection.add(ids=ids, documents=docs, metadatas=metas)
            logger.info(f"Added {len(chunks_to_add)} entity relations for {name}.")
            return len(chunks_to_add)
            
        return 0

relation_syncer = RelationSyncer()
