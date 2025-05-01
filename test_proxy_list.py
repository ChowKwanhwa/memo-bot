import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
import time
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv()

# Telegram API 凭证
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

# 代理列表
PROXY_LIST = [
    {
        'addr': '31.131.167.47',
        'port': 12324,
        'username': '14a91e96097d5',
        'password': 'e48a23adb8'
    },
]

async def test_telegram_proxy(proxy):
    """测试代理与Telegram的连接"""
    try:
        # 配置代理
        proxy_config = {
            'proxy_type': 'socks5',
            'addr': proxy['addr'],
            'port': proxy['port'],
            'username': proxy['username'],
            'password': proxy['password'],
            'rdns': True
        }

        # 创建客户端
        client = TelegramClient(
            StringSession(),
            API_ID,
            API_HASH,
            proxy=proxy_config
        )

        # 测试连接
        start_time = time.time()
        print(f"   正在连接到 {proxy['addr']}:{proxy['port']}...")
        await client.connect()
        
        if not await client.is_user_authorized():
            print("   连接成功，但未授权")
            status = "未授权但可连接"
        else:
            me = await client.get_me()
            print(f"   连接成功，已授权 (@{me.username})")
            status = f"已授权 (@{me.username})"
            
        elapsed = time.time() - start_time
        await client.disconnect()
        return True, elapsed, status
        
    except Exception as e:
        return False, str(e), "连接失败"

async def test_proxy(proxy):
    """测试单个代理"""
    print(f"\n测试代理: {proxy['addr']}:{proxy['port']}")
    
    # 测试Telegram连接
    tg_success, tg_result, status = await test_telegram_proxy(proxy)
    
    if isinstance(tg_result, float):
        print(f"   ✅ 连接成功 (延迟: {tg_result:.2f}秒)")
        print(f"   状态: {status}")
    else:
        print(f"   ❌ 连接失败: {tg_result}")
    
    return {
        'proxy': f"{proxy['addr']}:{proxy['port']}",
        'success': tg_success,
        'result': tg_result,
        'status': status
    }

async def main():
    """主函数"""
    print("开始测试代理列表...")
    results = []
    
    for proxy in PROXY_LIST:
        result = await test_proxy(proxy)
        results.append(result)
    
    # 打印总结报告
    print("\n=== 测试报告 ===")
    print(f"总共测试: {len(results)} 个代理")
    
    # 统计成功的代理
    success_count = sum(1 for r in results if r['success'])
    print(f"成功连接: {success_count}")
    print(f"连接失败: {len(results) - success_count}")
    
    # 打印可用的代理
    if success_count > 0:
        print("\n可用代理列表:")
        for result in results:
            if result['success']:
                print(f"{result['proxy']} - {result['status']}")
                print(f"延迟: {result['result']:.2f}秒")

if __name__ == "__main__":
    asyncio.run(main())
