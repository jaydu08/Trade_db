"""
LLM Client - 大语言模型客户端封装
支持 OpenAI 兼容接口
"""
import os
import logging
from typing import Optional, List, Dict, Any
from functools import lru_cache
from pathlib import Path

from openai import OpenAI
from pydantic import BaseModel
from dotenv import load_dotenv

# 加载 .env 文件
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

logger = logging.getLogger(__name__)


class LLMClient:
    """
    LLM 客户端封装

    支持 OpenAI 兼容接口（OpenAI, DeepSeek, 通义千问等）
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.1,
    ):
        """
        初始化 LLM 客户端

        Args:
            api_key: API Key（默认从环境变量读取）
            base_url: API Base URL（默认从环境变量读取）
            model: 模型名称（默认从环境变量读取）
            temperature: 温度参数
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.base_url = base_url or os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.temperature = temperature

        if not self.api_key:
            logger.warning("OPENAI_API_KEY not set. LLM features will not work.")
            self.client = None
        else:
            import httpx
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                timeout=httpx.Timeout(120.0, read=120.0, connect=30.0),
                default_headers={
                    'User-Agent': 'OpenAI-Python/1.0',
                    'Accept': 'application/json',
                },
            )
            logger.info(f"LLM client initialized: {self.base_url} / {self.model}")

    def is_available(self) -> bool:
        """检查 LLM 是否可用"""
        return self.client is not None

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        response_format: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        聊天补全

        Args:
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大 token 数
            response_format: 响应格式（如 {"type": "json_object"}）

        Returns:
            模型回复内容
        """
        if not self.is_available():
            raise RuntimeError("LLM client not available. Please set OPENAI_API_KEY.")

        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature or self.temperature,
            "stream": False,  # 强制非流式响应
        }

        if max_tokens:
            kwargs["max_tokens"] = max_tokens

        if response_format:
            kwargs["response_format"] = response_format

        try:
            response = self.client.chat.completions.create(**kwargs)
            content = response.choices[0].message.content
            if content is None:
                # 某些上游/网关异常场景会返回空 content，统一回落为 ""
                logger.warning("LLM returned empty content (None).")
                return ""
            return str(content)
        except Exception as e:
            logger.error(f"LLM chat failed: {e}")
            raise

    def structured_output(
        self,
        messages: List[Dict[str, str]],
        response_model: type[BaseModel],
        temperature: Optional[float] = None,
    ) -> BaseModel:
        """
        结构化输出（使用 Pydantic 模型）

        Args:
            messages: 消息列表
            response_model: Pydantic 模型类
            temperature: 温度参数

        Returns:
            解析后的 Pydantic 模型实例
        """
        if not self.is_available():
            raise RuntimeError("LLM client not available. Please set OPENAI_API_KEY.")

        try:
            # 添加 JSON Schema 提示
            schema = response_model.model_json_schema()
            system_msg = {
                "role": "system",
                "content": f"You must respond with valid JSON matching this schema:\n{schema}"
            }

            # 插入到消息列表开头
            enhanced_messages = [system_msg] + messages

            response = self.chat(
                messages=enhanced_messages,
                temperature=temperature,
                response_format={"type": "json_object"},
            )

            # 解析为 Pydantic 模型
            import json
            data = json.loads(response)
            return response_model(**data)

        except Exception as e:
            logger.error(f"Structured output failed: {e}")
            raise

    def simple_prompt(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
    ) -> str:
        """
        简单提示（单轮对话）

        Args:
            prompt: 用户提示
            system: 系统提示
            temperature: 温度参数

        Returns:
            模型回复
        """
        messages = []

        if system:
            messages.append({"role": "system", "content": system})

        messages.append({"role": "user", "content": prompt})

        return self.chat(messages, temperature=temperature)


# 全局单例
_llm_client: Optional[LLMClient] = None


@lru_cache(maxsize=1)
def get_llm_client() -> LLMClient:
    """获取全局 LLM 客户端单例"""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client


# 便捷函数
def chat(messages: List[Dict[str, str]], **kwargs) -> str:
    """便捷聊天函数"""
    return get_llm_client().chat(messages, **kwargs)


def simple_prompt(prompt: str, **kwargs) -> str:
    """便捷提示函数"""
    return get_llm_client().simple_prompt(prompt, **kwargs)


def structured_output(messages: List[Dict[str, str]], response_model: type[BaseModel], **kwargs) -> BaseModel:
    """便捷结构化输出函数"""
    return get_llm_client().structured_output(messages, response_model, **kwargs)
