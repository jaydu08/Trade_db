import logging
import json
import re
from typing import List, Dict, Any

# from duckduckgo_search import DDGS # Removed due to instability
from core.llm import get_llm_client
from modules.ingestion.akshare_client import akshare_client
from modules.ingestion.sync_profile import profile_syncer

logger = logging.getLogger(__name__)

# ============================================================
# Tools Definition
# ============================================================

from modules.ingestion.caixin_client import caixin_client

class Tools:
    @staticmethod
    def web_search(query: str) -> str:
        """联网搜索，整合多数据源 (SearXNG, Tavily, Bocha, Caixin等)"""
        from modules.ingestion.data_factory import data_manager
        return data_manager.search(query)

    @staticmethod
    def database_search(query: str) -> str:
        """查询本地数据库（公司简介、产业链）"""
        logger.info(f"Tool: Searching DB for '{query}'")
        try:
            results = profile_syncer.search_companies(query, n_results=5)
            if not results:
                return "本地数据库未找到相关公司。"
            
            summary = "本地数据库匹配结果:\n"
            for r in results:
                meta = r.get('metadata', {})
                summary += f"- {meta.get('name')} ({meta.get('symbol')}): {meta.get('match_reason', '相关')}\n"
            return summary
        except Exception as e:
            return f"数据库查询失败: {e}"

    @staticmethod
    def get_quote(symbol: str) -> str:
        """获取实时行情 (A股/港股/美股) - 聚合多数据源"""
        market = "CN"
        if len(symbol) == 5: market = "HK"
        elif symbol.isalpha(): market = "US"
        
        from modules.ingestion.data_factory import data_manager
        
        res = data_manager.get_quote(symbol, market)
        if res:
            return f"【实时行情 {symbol} (Via {res.get('provider')})】\n现价: {res.get('price')}\n涨跌: {res.get('change', 'N/A')}\n幅度: {res.get('pct_chg', 'N/A')}%\n时间: {res.get('timestamp')}"
            
        return f"所有数据源均未能获取到 {symbol} 的实时行情。"

# ============================================================
# ReAct Agent (Manual Loop)
# ============================================================

class ReactAgent:
    """
    一个轻量级的 ReAct Agent
    无需 LangChain，手撸 Loop 以保证可控性
    """
    def __init__(self):
        self.llm_client = get_llm_client()
        self.tools = {
            "web_search": Tools.web_search,
            "database_search": Tools.database_search,
            "get_quote": Tools.get_quote
        }
        
    def run(self, user_input: str) -> str:
        """
        执行 Agent 循环
        """
        logger.info(f"Agent started for input: {user_input[:50]}...")
        
        import datetime
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # System Prompt 注入工具描述
        system_prompt = f"""
你是一个专业的金融投研 AI 助手。
当前系统时间: {current_time}

【核心指令】
1. **真实性优先**：回答任何市场/公司问题，必须先调用 `web_search` 或 `get_quote`。严禁使用你内部的过期知识（截止2024年）。
2. **禁止编造**：如果 `web_search` 返回"暂无相关新闻"或空白，你必须明确告知用户"未找到相关最新信息"，**严禁**自行编造原因或数据。
3. **数据融合与验证**：
   - **上市验证协议**：在报告中提到任何公司时，**必须**先尝试调用 `get_quote`。
   - 严禁仅凭记忆判断公司是否上市。如果 `get_quote` 返回有效价格，该公司即为**已上市**；只有当返回"未查询到"时，才可标注为"未上市"。
   - 提到行业时，调用 `database_search` 查找本地知识库。
4. **元推理 (Meta-Reasoning)**：
   - 在得出结论前，强制思考："除了字面关联，还有哪些隐性受益者？"
   - 重点挖掘新晋独角兽，并**务必验证其最新融资或上市状态**。

【可用工具】
1. web_search: 联网搜索 (聚合 Akshare 实时电报 + 全网搜索)。指令格式: SEARCH: 关键词
2. get_quote: 查行情。指令格式: QUOTE: 代码或名称
3. database_search: 查数据库。指令格式: DB: 关键词

【回答规则】
Question: 用户的问题
Thought: 我需要先搜索...
SEARCH: 美术艺术 产业链 2026
Observation: ...
Thought: 我要验证智谱AI是否上市...
QUOTE: 智谱AI
Observation: ...
Final Answer: ...

【输出格式】
严格 Markdown 格式。
**严禁使用表格，请使用列表。**
"""
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_input}
        ]
        
        max_steps = 6
        current_step = 0
        
        try:
            while current_step < max_steps:
                current_step += 1
                logger.info(f"--- Step {current_step} ---")
                
                # 1. LLM 思考
                try:
                    response = self.llm_client.chat(messages, temperature=0.1)
                except Exception as e:
                    logger.error(f"LLM Call Failed: {e}")
                    return f"Agent 思考过程中发生错误: {e}"
                
                logger.info(f"LLM Response: {response[:200]}...")
                messages.append({"role": "assistant", "content": response})
                
                # 2. 解析 Action (Fuzzy Logic)
                if "Final Answer:" in response:
                    final_answer = response.split("Final Answer:")[1].strip()
                    return final_answer if final_answer else response

                # 模糊匹配三种指令
                tool_executed = False
                
                search_match = re.search(r"SEARCH:\s*(.+)", response, re.IGNORECASE)
                quote_match = re.search(r"QUOTE:\s*(.+)", response, re.IGNORECASE)
                db_match = re.search(r"DB:\s*(.+)", response, re.IGNORECASE)
                
                if search_match:
                    query = search_match.group(1).strip().strip('"').strip("'")
                    logger.info(f"Fuzzy Exec: web_search '{query}'")
                    obs = self.tools["web_search"](query)
                    messages.append({"role": "user", "content": f"Observation: {obs}"})
                    tool_executed = True
                    
                elif quote_match:
                    symbol = quote_match.group(1).strip().strip('"').strip("'")
                    logger.info(f"Fuzzy Exec: get_quote '{symbol}'")
                    obs = self.tools["get_quote"](symbol)
                    messages.append({"role": "user", "content": f"Observation: {obs}"})
                    tool_executed = True
                    
                elif db_match:
                    query = db_match.group(1).strip().strip('"').strip("'")
                    logger.info(f"Fuzzy Exec: database_search '{query}'")
                    obs = self.tools["database_search"](query)
                    messages.append({"role": "user", "content": f"Observation: {obs}"})
                    tool_executed = True
                
                if not tool_executed:
                    # 如果 LLM 啥都没调，强制它结束或者提示它
                    if current_step > 1:
                        return response
                    else:
                        # Step 1 没调工具？强制帮它搜一下
                        logger.info("Auto-Search Triggered")
                        obs = self.tools["web_search"](user_input)
                        messages.append({"role": "user", "content": f"System Auto-Search Observation: {obs}"})

            return "抱歉，我思考了太久，没有得出结论。"
            
        except Exception as e:
            logger.error(f"Agent Loop Error: {e}", exc_info=True)
            return f"Agent 运行出错: {e}"

# 全局实例
agent_executor = ReactAgent()
