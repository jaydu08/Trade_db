#!/usr/bin/env python3
"""
测试 LLM 配置是否正常
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from core.llm import get_llm_client

def test_llm_connection():
    """测试 LLM 连接"""
    print("=" * 60)
    print("LLM 配置测试")
    print("=" * 60)
    
    # 显示当前配置
    print(f"\n当前配置:")
    print(f"  API Base: {os.getenv('OPENAI_API_BASE', 'NOT SET')}")
    print(f"  Model: {os.getenv('OPENAI_MODEL', 'NOT SET')}")
    print(f"  API Key: {os.getenv('OPENAI_API_KEY', 'NOT SET')[:10]}...")
    
    try:
        # 获取客户端
        client = get_llm_client()
        
        if not client.is_available():
            print("\n❌ LLM 客户端不可用，请检查 OPENAI_API_KEY 是否设置")
            return False
        
        print(f"\n✓ LLM 客户端初始化成功")
        print(f"  Base URL: {client.base_url}")
        print(f"  Model: {client.model}")
        
        # 发送测试请求
        print("\n正在发送测试请求...")
        response = client.simple_prompt(
            "你好，请简短回复'测试成功'四个字",
            system="你是一个测试助手",
            temperature=0.1
        )
        
        print(f"\n✓ 收到回复: {response}")
        print("\n" + "=" * 60)
        print("✅ LLM 配置测试通过！")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n❌ LLM 调用失败: {e}")
        print("\n可能的原因:")
        print("  1. API Key 无效或已过期")
        print("  2. API Base URL 不可访问")
        print("  3. 模型名称不正确")
        print("  4. 账户余额不足")
        print("\n建议:")
        print("  - 检查 .env 文件中的配置")
        print("  - 联系 API 提供商确认服务状态")
        print("=" * 60)
        return False

if __name__ == "__main__":
    success = test_llm_connection()
    sys.exit(0 if success else 1)
