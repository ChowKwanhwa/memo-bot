import os
import asyncio
from telethon import TelegramClient, errors
from dotenv import load_dotenv
import sys

# 加载环境变量
load_dotenv()

# 从环境变量获取API凭据
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

async def try_connect_with_proxy(phone_number, proxy_config):
    """尝试使用代理连接"""
    try:
        print(f"\n正在使用代理 {proxy_config['addr']}:{proxy_config['port']} 尝试连接...")
        
        # 创建session文件路径，保留加号
        phone = phone_number.replace(' ', '')  # 只移除空格，保留加号
        session_path = os.path.join(SESSIONS_DIR, phone)
        
        # 确保会话目录存在
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        
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
        
        # 启动客户端并等待验证码
        await client.connect()
        
        if not await client.is_user_authorized():
            print(f"需要验证电话号码: {phone_number}")
            
            try:
                await client.send_code_request(phone_number)
                verification_code = input("请输入收到的验证码: ")
                
                # 尝试使用验证码登录
                try:
                    await client.sign_in(phone_number, verification_code)
                except errors.SessionPasswordNeededError:
                    # 处理两步验证
                    password = input("请输入两步验证密码: ")
                    await client.sign_in(password=password)
                
                print(f"[成功] {phone_number} 验证成功!")
                
            except Exception as e:
                print(f"[错误] 验证过程中出错: {str(e)}")
                return None
        else:
            print(f"[成功] {phone_number} 已经验证过")
        
        return client
        
    except Exception as e:
        print(f"[失败] 使用代理 {proxy_config['addr']} 时出错: {str(e)}")
        return None

async def process_phone(phone_number):
    """使用所有代理尝试处理一个电话号码"""
    for proxy in PROXY_LIST:
        client = await try_connect_with_proxy(phone_number, proxy)
        if client:
            await client.disconnect()
            return True
    return False

async def main():
    """主函数"""
    # 确保会话目录存在
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    
    # 定义电话号码列表
    phone_numbers = [
        '+13653154174'
    ]
    
    # 批量处理每个电话号码
    for phone_number in phone_numbers:
        print(f"\n开始处理电话号码: {phone_number}")
        success = await process_phone(phone_number)
        if not success:
            print(f"[失败] {phone_number} 所有代理均连接失败")

if __name__ == "__main__":
    asyncio.run(main())