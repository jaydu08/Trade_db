#!/usr/bin/env python3
"""
测试 LLM 配置
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.llm import get_llm_client


def test_llm():
    """测试 LLM 连接"""
    print("=" * 50)
    print("Testing LLM Configuration")
    print("=" * 50)
    print()
    
    llm = get_llm_client()
    
    # 检查是否可用
    if not llm.is_available():
        print("❌ LLM not available!")
        print()
        print("Please set OPENAI_API_KEY in .env file:")
        print("  1. Copy .env.example to .env")
        print("  2. Set your API key")
        print("  3. Run this test again")
        print()
        print("See SETUP_LLM.md for detailed instructions.")
        sys.exit(1)
    
    print(f"✓ LLM client initialized")
    print(f"  Base URL: {llm.base_url}")
    print(f"  Model: {llm.model}")
    print()
    
    # 测试简单调用
    print("Testing simple prompt...")
    try:
        response = llm.simple_prompt(
            prompt="请用一句话介绍什么是产业链。",
            system="你是一个简洁的助手。",
        )
        print(f"✓ Response: {response[:100]}...")
        print()
    except Exception as e:
        print(f"❌ Simple prompt failed: {e}")
        sys.exit(1)
    
    # 测试结构化输出
    print("Testing structured output...")
    try:
        from pydantic import BaseModel, Field
        
        class TestOutput(BaseModel):
            industry: str = Field(description="产业名称")
            keywords: list[str] = Field(description="关键词列表")
        
        result = llm.structured_output(
            messages=[
                {"role": "user", "content": "请列出新能源汽车产业的3个关键词"}
            ],
            response_model=TestOutput,
        )
        
        print(f"✓ Structured output:")
        print(f"  Industry: {result.industry}")
        print(f"  Keywords: {', '.join(result.keywords)}")
        print()
    except Exception as e:
        print(f"❌ Structured output failed: {e}")
        sys.exit(1)
    
    print("=" * 50)
    print("✅ All tests passed!")
    print("=" * 50)
    print()
    print("You can now run the chain mining strategy:")
    print('  python main.py strategy chain "AI眼镜"')
    print()


if __name__ == "__main__":
    test_llm()
