import os
from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio
from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate
from telethon.errors import *
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Telegram API 凭证
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

# MTProto代理配置
PROXY_CONFIG = {
    'server': '185.162.130.86',
    'port': 10082,
    'secret': 'RzWuVl6lEOR6COra4d9lRNW78Fm5'  # 合并两个密钥
}

def format_secret(secret):
    """格式化MTProto密钥"""
    # 移除所有空格
    secret = secret.replace(' ', '')
    
    # 如果密钥以dd结尾，说明是直接密钥
    if secret.endswith('dd'):
        return secret
    
    # 否则添加dd后缀（表示随机填充）
    return secret + 'dd'

async def test_mtproto_proxy():
    """测试MTProto代理连接"""
    print(f"\n开始测试MTProto代理配置:")
    print(f"服务器: {PROXY_CONFIG['server']}")
    print(f"端口: {PROXY_CONFIG['port']}")
    print(f"密钥长度: {len(PROXY_CONFIG['secret'])} 字符")
    
    # 格式化密钥
    formatted_secret = format_secret(PROXY_CONFIG['secret'])
    print(f"格式化后的密钥长度: {len(formatted_secret)} 字符")
    
    try:
        # 创建使用MTProto代理的客户端
        client = TelegramClient(
            StringSession(),
            API_ID,
            API_HASH,
            connection=ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=(PROXY_CONFIG['server'], PROXY_CONFIG['port'], formatted_secret)
        )

        print("\n正在连接到Telegram...")
        await client.connect()
        
        if await client.is_user_authorized():
            print("成功连接到Telegram（已授权）")
        else:
            print("成功连接到Telegram（未授权）")
            
        # 测试基本功能
        print("正在获取Telegram服务器时间...")
        time = await client.get_date()
        print(f"Telegram服务器时间: {time}")
        
        await client.disconnect()
        print("测试完成，代理可用")
        return True
        
    except ConnectionError as e:
        print("\n连接错误:")
        print(f"错误类型: {type(e).__name__}")
        print(f"错误信息: {str(e)}")
        if "proxy closed the connection" in str(e).lower():
            print("\n可能的原因:")
            print("1. 代理服务器已关闭或不可用")
            print("2. 密钥格式不正确")
            print("3. 代理服务器拒绝连接（可能是因为密钥错误）")
        return False
    except ProxyConnectionError as e:
        print("\n代理连接错误:")
        print(f"错误类型: {type(e).__name__}")
        print(f"错误信息: {str(e)}")
        return False
    except Exception as e:
        print(f"\n其他错误:")
        print(f"错误类型: {type(e).__name__}")
        print(f"错误信息: {str(e)}")
        return False

if __name__ == "__main__":
    # 运行测试
    asyncio.run(test_mtproto_proxy())