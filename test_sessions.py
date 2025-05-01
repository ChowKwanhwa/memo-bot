import os
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Telegram API 凭证
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

# 会话目录
SESSIONS_DIR = "hopperday2"

# 代理列表
PROXY_LIST = [
    {
        'proxy_type': 'socks5',  # 添加代理类型
        'addr': '45.206.3.125',
        'port': 50101,
        'username': 'zhouhaha',
        'password': '963091790'
    }
]

async def try_connect_with_proxy(session_file, proxy_config):
    """尝试使用代理连接并测试会话"""
    session_path = os.path.join(SESSIONS_DIR, session_file.replace('.session', ''))
    
    try:
        print(f"\n正在使用代理 {proxy_config['addr']}:{proxy_config['port']} 测试 {session_file}...")
        
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
            session_path,
            API_ID,
            API_HASH,
            proxy=proxy
        )
        
        # 尝试连接
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"[成功] 已连接到账号: {me.first_name} (@{me.username})")
            print(f"       电话: {me.phone}")
            return True, client, "已授权"
        else:
            print(f"[失败] {session_file} 未授权")
            await client.disconnect()
            return False, None, "未授权"
            
    except Exception as e:
        print(f"[错误] 测试 {session_file} 时出错: {str(e)}")
        return False, None, str(e)

async def test_session(session_file):
    """使用所有代理尝试测试会话文件"""
    for proxy in PROXY_LIST:
        success, client, status = await try_connect_with_proxy(session_file, proxy)
        if success:
            return client, status
    return None, "所有代理均失败"

async def main():
    """主函数"""
    # 确保会话目录存在
    if not os.path.exists(SESSIONS_DIR):
        print(f"错误: 会话目录 {SESSIONS_DIR} 不存在!")
        return
    
    # 获取所有.session文件
    session_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]
    if not session_files:
        print(f"错误: 在 {SESSIONS_DIR} 目录中没有找到.session文件!")
        return
    
    print(f"找到 {len(session_files)} 个会话文件")
    
    # 测试每个会话文件
    results = []
    for session_file in session_files:
        client, status = await test_session(session_file)
        if client:
            await client.disconnect()
        results.append((session_file, status))
    
    # 打印总结报告
    print("\n=== 测试报告 ===")
    print(f"总共测试: {len(results)} 个会话文件")
    valid = sum(1 for _, status in results if status == "已授权")
    print(f"有效会话: {valid}")
    print(f"无效会话: {len(results) - valid}")
    
    # 打印详细状态
    print("\n详细状态:")
    for session_file, status in results:
        print(f"{session_file}: {status}")

if __name__ == "__main__":
    asyncio.run(main())

