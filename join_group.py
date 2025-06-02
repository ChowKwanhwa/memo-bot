from telethon import TelegramClient, functions
import os
import random
import asyncio
from dotenv import load_dotenv
from config import PROXY_LIST

# 加载环境变量
load_dotenv()

# 从环境变量获取配置
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')

# 配置
GROUP_USERNAME = 'https://t.me/MemoLabsio'  # 目标群组用户名
SESSIONS_DIR = 'sessions/memolabs'  # session文件目录

async def try_connect_with_proxy(session_file, proxy_config):
    """尝试使用特定代理连接"""
    try:
        # 创建代理配置
        proxy = {
            'proxy_type': 'socks5',
            'addr': proxy_config['addr'],
            'port': proxy_config['port'],
            'username': proxy_config['username'],
            'password': proxy_config['password'],
            'rdns': True
        }
        
        # 创建客户端
        client = TelegramClient(
            session_file,
            api_id,
            api_hash,
            proxy=proxy
        )
        
        print(f"正在尝试代理: {proxy_config['addr']}:{proxy_config['port']}")
        await client.connect()
        
        if await client.is_user_authorized():
            return client
        else:
            await client.disconnect()
            return None
            
    except Exception as e:
        print(f"代理连接失败: {str(e)}")
        return None

async def process_account(session_file):
    """处理单个账号的加群操作"""
    client = None
    
    # 尝试所有代理
    for proxy in PROXY_LIST:
        client = await try_connect_with_proxy(session_file, proxy)
        if client:
            print(f"找到可用代理: {proxy['addr']}:{proxy['port']}")
            break
    
    if not client:
        print(f"❌ 所有代理均连接失败: {session_file}")
        return
        
    try:
        me = await client.get_me()
        print(f"\n使用账号 {me.first_name} (@{me.username}) 处理中...")
        
        # 随机延迟4-6秒
        delay = random.uniform(1, 2)
        print(f"等待 {delay:.2f} 秒...")
        await asyncio.sleep(delay)
        
        try:
            # 加入群组
            await client(functions.channels.JoinChannelRequest(GROUP_USERNAME))
            print(f"✅ 成功加入群组: {GROUP_USERNAME}")
            
        except Exception as e:
            print(f"❌ 加入群组失败: {str(e)}")
            
    except Exception as e:
        print(f"❌ 处理账号时出错: {str(e)}")
        
    finally:
        try:
            await client.disconnect()
        except:
            pass

async def main():
    """主函数"""
    # 确保会话目录存在
    if not os.path.exists(SESSIONS_DIR):
        print(f"错误: 会话目录 {SESSIONS_DIR} 不存在!")
        return
        
    # 获取所有.session文件
    session_files = [
        os.path.join(SESSIONS_DIR, f[:-8])  # 移除.session后缀
        for f in os.listdir(SESSIONS_DIR)
        if f.endswith('.session')
    ]
    
    if not session_files:
        print(f"错误: 在 {SESSIONS_DIR} 目录中没有找到.session文件!")
        return
    
    print(f"找到 {len(session_files)} 个session文件")
    
    # 依次处理每个账号
    for session_file in session_files:
        await process_account(session_file)
        print("-" * 50)

if __name__ == '__main__':
    asyncio.run(main())
