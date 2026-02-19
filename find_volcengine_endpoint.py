#!/usr/bin/env python3
"""
帮助查找火山引擎端点配置
"""
import requests

API_KEY = "290baa09-58fb-4197-8b82-66031da39d8d"
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"

print("=" * 60)
print("火山引擎 API 配置检查")
print("=" * 60)
print()

# 尝试列出可用模型
print("尝试获取可用模型列表...")
try:
    response = requests.get(
        f"{BASE_URL}/models",
        headers={
            "Authorization": f"Bearer {API_KEY}",
        },
        timeout=10,
    )
    
    if response.status_code == 200:
        data = response.json()
        print("✓ 可用模型：")
        if "data" in data:
            for model in data["data"]:
                print(f"  - {model.get('id', 'N/A')}")
        else:
            print(data)
    else:
        print(f"✗ 请求失败: {response.status_code}")
        print(f"  响应: {response.text[:200]}")
except Exception as e:
    print(f"✗ 请求异常: {e}")

print()
print("=" * 60)
print("配置说明：")
print("=" * 60)
print()
print("火山引擎使用端点 ID 而不是模型名称。")
print("请在火山引擎控制台查找你的端点 ID：")
print()
print("1. 登录火山引擎控制台")
print("2. 进入「模型推理」或「在线推理」页面")
print("3. 找到你创建的 DeepSeek 端点")
print("4. 复制端点 ID（格式：ep-xxxxx-xxxxx）")
print()
print("然后更新 .env 文件：")
print("  OPENAI_MODEL=你的端点ID")
print()
print("或者告诉我你的端点 ID，我帮你配置。")
print()
