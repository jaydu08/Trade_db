#!/usr/bin/env python3
"""
完整功能测试脚本
测试所有核心模块
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def test_llm():
    """测试 LLM"""
    print("\n" + "=" * 60)
    print("1. 测试 LLM 模块")
    print("=" * 60)
    
    from core.llm import get_llm_client
    
    llm = get_llm_client()
    if not llm.is_available():
        print("❌ LLM 不可用，请配置 .env 文件")
        return False
    
    try:
        response = llm.simple_prompt("用一句话介绍产业链")
        print(f"✓ LLM 响应: {response[:100]}...")
        return True
    except Exception as e:
        print(f"❌ LLM 测试失败: {e}")
        return False


def test_profile_sync():
    """测试公司简介同步"""
    print("\n" + "=" * 60)
    print("2. 测试公司简介同步")
    print("=" * 60)
    
    from modules.ingestion.sync_profile import profile_syncer
    from core.db import get_collection
    
    # 测试同步
    print("同步 000001 (平安银行)...")
    result = profile_syncer.sync_profile('000001', market='CN')
    
    if not result:
        print("❌ 同步失败")
        return False
    
    print(f"✓ 同步成功")
    print(f"  主营业务: {result['main_business'][:50]}...")
    print(f"  简介长度: {result['profile_length']}")
    
    # 检查向量库
    collection = get_collection('company_chunks')
    count = collection.count()
    print(f"✓ 向量库数量: {count}")
    
    if count == 0:
        print("❌ 向量库为空")
        return False
    
    # 测试搜索
    results = collection.query(query_texts=['银行'], n_results=3)
    print(f"✓ 搜索测试: 找到 {len(results['documents'][0])} 个结果")
    
    return True


def test_market_prober():
    """测试实时行情探测"""
    print("\n" + "=" * 60)
    print("3. 测试实时行情探测")
    print("=" * 60)
    
    from modules.probing.market import market_prober
    
    # 测试单个行情
    print("获取 000001 实时行情...")
    quote = market_prober.get_realtime_quote('000001')
    
    if not quote:
        print("❌ 获取行情失败")
        return False
    
    print(f"✓ 行情获取成功")
    print(f"  股票: {quote['name']}")
    print(f"  价格: {quote['price']}")
    print(f"  涨跌幅: {quote['change_pct']}%")
    
    # 测试交易状态
    status = market_prober.check_trading_status(quote)
    print(f"✓ 交易状态:")
    print(f"  可交易: {status['is_tradable']}")
    print(f"  涨停: {status['is_limit_up']}")
    print(f"  跌停: {status['is_limit_down']}")
    
    return True


def test_chain_mining():
    """测试产业链挖掘（需要向量库有数据）"""
    print("\n" + "=" * 60)
    print("4. 测试产业链挖掘策略")
    print("=" * 60)
    
    from strategies.chain_mining import chain_mining_strategy
    from core.db import get_collection
    
    # 检查向量库
    collection = get_collection('company_chunks')
    count = collection.count()
    
    if count < 10:
        print(f"⚠️  向量库数据不足 ({count} 条)，跳过产业链挖掘测试")
        print("   请等待 profile 同步完成后再测试")
        return True
    
    print(f"向量库数据: {count} 条")
    print("执行产业链挖掘: 银行")
    
    try:
        # 测试产业链拆解
        chain = chain_mining_strategy.decompose_chain("银行")
        print(f"✓ 产业链拆解成功: {len(chain.nodes)} 个节点")
        for node in chain.nodes:
            print(f"  - {node.position}: {', '.join(node.keywords[:3])}")
        
        # 测试向量映射
        matches = chain_mining_strategy.map_to_stocks(chain, top_k=5)
        print(f"✓ 映射到 {len(matches)} 个股票")
        for match in matches[:5]:
            print(f"  - {match.symbol} {match.name} (匹配度: {match.match_score:.2f})")
        
        return True
    
    except Exception as e:
        print(f"❌ 产业链挖掘失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_database():
    """测试数据库状态"""
    print("\n" + "=" * 60)
    print("5. 测试数据库状态")
    print("=" * 60)
    
    import sqlite3
    
    db_path = "data/meta.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 检查各表数据量
    tables = {
        "asset": "股票列表",
        "asset_profile": "公司简介",
        "concept": "概念板块",
        "industry": "行业分类",
        "asset_concept_link": "概念关联",
        "asset_industry_link": "行业关联",
    }
    
    for table, desc in tables.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"✓ {desc} ({table}): {count:,} 条")
    
    conn.close()
    return True


def main():
    """运行所有测试"""
    print("=" * 60)
    print("AlphaBase 功能测试")
    print("=" * 60)
    
    tests = [
        ("LLM", test_llm),
        ("公司简介同步", test_profile_sync),
        ("实时行情探测", test_market_prober),
        ("产业链挖掘", test_chain_mining),
        ("数据库状态", test_database),
    ]
    
    results = {}
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            print(f"\n❌ {name} 测试异常: {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
    
    # 总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    
    for name, passed in results.items():
        status = "✓ 通过" if passed else "✗ 失败"
        print(f"{status}: {name}")
    
    passed_count = sum(results.values())
    total_count = len(results)
    
    print(f"\n总计: {passed_count}/{total_count} 通过")
    
    if passed_count == total_count:
        print("\n🎉 所有测试通过！")
    else:
        print("\n⚠️  部分测试失败，请检查配置")


if __name__ == "__main__":
    main()
