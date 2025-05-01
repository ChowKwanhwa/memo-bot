# 如果是topic，使用flag --topic，并指定topic-id，如python sender.py --topic --topic-id 3

import os
import pandas as pd
from telethon import TelegramClient
import asyncio
import random
from telethon.tl.types import InputPeerChannel, ReactionEmoji, InputDocument
from telethon.tl.functions.messages import GetHistoryRequest, SendReactionRequest
import emoji
from dotenv import load_dotenv
from telethon.tl.functions.channels import JoinChannelRequest
import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
import datetime
from config import PROXY_LIST  # Import proxy list from config.py

# 配置日志
def setup_logging():
    """设置日志配置"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_file = os.path.join(log_dir, f"memolabs_{datetime.datetime.now().strftime('%Y%m%d')}.log")
    
    # 创建 RotatingFileHandler，限制单个日志文件大小为 1MB，最多保留 5 个备份
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=1024*1024,  # 1MB
        backupCount=5,
        encoding='utf-8'
    )
    
    # 设置日志格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # 配置根日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    
    # 减少一些不必要的日志
    logging.getLogger('telethon').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    return logger

# 初始化日志
logger = setup_logging()

# 加载.env文件
load_dotenv()

# 从环境变量获取API凭据
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

# 其他配置
TARGET_GROUP = "https://t.me/MemoLabsio"
TOPIC_ID = 1
SESSIONS_DIR = "sessions/memolabs"
MESSAGES_FILE = "MemoLabs/MemoLabs_messages.csv"  # Using forward slashes instead of backslash
MEDIA_DIR = os.path.join("MemoLabs", "media")  # Add media directory path

# 读取消息数据
try:
    # 先读取CSV文件的标题行来确定列数
    df = pd.read_csv(MESSAGES_FILE, nrows=1)
    expected_columns = len(df.columns)
    
    # 使用指定的列名重新读取文件，跳过错误行
    df = pd.read_csv(MESSAGES_FILE, 
                     names=['id', 'date', 'type', 'content', 'media_file'],
                     on_bad_lines='skip')
    
    # 清理数据：删除可能的空行和无效行
    df = df.dropna(subset=['id', 'type'])  # 确保至少有id和type列有值
    
    # 反转DataFrame，使最早的消息（底部）在前面
    df = df.iloc[::-1]
    messages = df.to_dict('records')
    logger.info(f"Successfully loaded {len(messages)} messages from CSV file")
except Exception as e:
    logger.error(f"Error reading CSV file: {e}")
    messages = []

# 表情符号列表用于reactions
REACTION_EMOJIS = ['👍',  '🔥', '🎉', '🔥']

async def try_connect_with_proxy(session_file, proxy_config):
    """尝试使用特定代理连接"""
    session_path = os.path.join(SESSIONS_DIR, session_file.replace('.session', ''))
    client = TelegramClient(session_path, API_ID, API_HASH, proxy=proxy_config)
    
    try:
        logger.info(f"正在尝试使用代理 {proxy_config['addr']}:{proxy_config['port']} 连接...")
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            logger.info(f"[成功] 使用代理 {proxy_config['addr']} 连接成功!")
            logger.info(f"       账号: {me.first_name} (@{me.username})")
            return client
        
        await client.disconnect()
        logger.warning(f"[失败] 使用代理 {proxy_config['addr']} 连接失败: 未授权")
        return None
        
    except Exception as e:
        error_str = str(e)
        logger.error(f"[失败] 使用代理 {proxy_config['addr']} 连接失败: {error_str}")
        if "Proxy connection timed out" in error_str:
            logger.info("代理超时，将尝试下一个代理...")
        try:
            await client.disconnect()
        except:
            pass
        return None

async def try_join_channel(client, channel_url):
    """尝试加入频道"""
    try:
        channel = await client.get_entity(channel_url)
        await client(JoinChannelRequest(channel))
        logger.info(f"成功加入频道: {channel_url}")
        return True
    except Exception as e:
        logger.error(f"加入频道失败: {str(e)}")
        return False

async def init_clients():
    """初始化所有客户端，使用代理轮换机制"""
    session_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]
    clients = []
    
    for session_file in session_files:
        client = None
        proxy_list = PROXY_LIST.copy()
        random.shuffle(proxy_list)  # 随机打乱代理列表顺序
        
        # 尝试所有代理直到找到一个可用的
        for proxy in proxy_list:
            client = await try_connect_with_proxy(session_file, proxy)
            if client:
                # 尝试加入目标频道
                if await try_join_channel(client, TARGET_GROUP):
                    clients.append(client)
                    break
                else:
                    await client.disconnect()
                    client = None
        
        if not client:
            logger.warning(f"警告: {session_file} 所有代理均连接失败或无法加入频道!")
    
    return clients

async def reconnect_client(client, session_file):
    """使用新的代理重新连接客户端"""
    try:
        await client.disconnect()
    except:
        pass
    
    proxy_list = PROXY_LIST.copy()
    random.shuffle(proxy_list)  # 随机打乱代理列表顺序
    
    for proxy in proxy_list:
        new_client = await try_connect_with_proxy(session_file, proxy)
        if new_client:
            return new_client
    
    return None

async def process_action(client, message_data, recent_messages, use_topic, topic_id):
    try:
        channel = await client.get_entity(TARGET_GROUP)
        
        if not recent_messages:  # 如果没有最近消息，直接发送新消息
            kwargs = {}
            if use_topic:
                kwargs['reply_to'] = topic_id
                
            msg_type = message_data['type']
            media_path = message_data.get('media_file', '')
            
            if media_path and msg_type in ['sticker', 'video', 'photo', 'file']:
                full_path = os.path.join(os.getcwd(), MEDIA_DIR, os.path.basename(media_path))
                if msg_type in ['sticker', 'video', 'photo']:
                    try:
                        success = await send_media_with_metadata(client, channel, full_path, msg_type, **kwargs)
                        if not success:
                            logger.debug(f"Falling back to direct file send for: {full_path}")
                            if msg_type == 'sticker':
                                logger.debug("Skipping direct file send for sticker")
                            else:
                                await client.send_file(channel, full_path, **kwargs)
                    except Exception as e:
                        logger.error(f"Error sending media: {str(e)}")
                        if msg_type != 'sticker':
                            await client.send_file(channel, full_path, **kwargs)
                else:
                    await client.send_file(channel, full_path, **kwargs)
            elif message_data['content'] and message_data['content'] not in ['[PHOTO]', '[FILE]', '[STICKER]']:
                await client.send_message(channel, message_data['content'], **kwargs)
            return

        random_value = random.random()
        
        if random_value < 0.15:  # 15% 概率发送表情反应
            target_message = random.choice(recent_messages)
            chosen_emoji = random.choice(REACTION_EMOJIS)
            reaction = [ReactionEmoji(emoticon=chosen_emoji)]
            reaction_text = '点赞' if chosen_emoji == '👍' else f'表情({chosen_emoji})'
            
            await client(SendReactionRequest(
                peer=channel,
                msg_id=target_message.id,
                reaction=reaction
            ))
            me = await client.get_me()
            username = f"@{me.username}" if me.username else me.id
            logger.info(f"{username} 对消息ID {target_message.id} 进行了{reaction_text}反应")
            
        elif random_value < 0.40:  # 25% 概率回复消息
            target_message = random.choice(recent_messages)
            kwargs = {'reply_to': target_message.id}
            if use_topic:
                kwargs['reply_to'] = topic_id
                
            content = message_data['content']
            if content and content not in ['[PHOTO]', '[FILE]', '[STICKER]']:
                await client.send_message(channel, content, **kwargs)
            
        else:  # 剩余 60% 概率直接发送消息
            kwargs = {}
            if use_topic:
                kwargs['reply_to'] = topic_id
                
            msg_type = message_data['type']
            media_path = message_data.get('media_file', '')
            
            if media_path and msg_type in ['sticker', 'video', 'photo', 'file']:
                full_path = os.path.join(os.getcwd(), MEDIA_DIR, os.path.basename(media_path))
                if msg_type in ['sticker', 'video', 'photo']:
                    try:
                        success = await send_media_with_metadata(client, channel, full_path, msg_type, **kwargs)
                        if not success:
                            logger.debug(f"Falling back to direct file send for: {full_path}")
                            if msg_type == 'sticker':
                                logger.debug("Skipping direct file send for sticker")
                            else:
                                await client.send_file(channel, full_path, **kwargs)
                    except Exception as e:
                        logger.error(f"Error sending media: {str(e)}")
                        if msg_type != 'sticker':
                            await client.send_file(channel, full_path, **kwargs)
                else:
                    await client.send_file(channel, full_path, **kwargs)
            elif message_data['content'] and message_data['content'] not in ['[PHOTO]', '[FILE]', '[STICKER]']:
                await client.send_message(channel, message_data['content'], **kwargs)
                
    except Exception as e:
        logger.error(f"Error processing action: {str(e)}")

async def process_action_with_retry(client, message_data, recent_messages, use_topic, topic_id, session_file):
    """处理动作，如果出现连接错误则尝试重新连接"""
    try:
        await process_action(client, message_data, recent_messages, use_topic, topic_id)
    except Exception as e:
        error_str = str(e)
        if "Proxy connection timed out" in error_str:
            logger.info(f"代理连接超时，尝试重新连接...")
            new_client = await reconnect_client(client, session_file)
            if new_client:
                logger.info("重新连接成功，重试操作...")
                await process_action(new_client, message_data, recent_messages, use_topic, topic_id)
                return new_client
        else:
            logger.error(f"Error processing action: {error_str}")
    return client

async def send_media_with_metadata(client, channel, media_path, msg_type, **kwargs):
    """Send media using its JSON metadata"""
    # Get the corresponding JSON file
    json_path = os.path.splitext(media_path)[0] + '.json'
    try:
        with open(json_path, 'r') as f:
            media_data = json.load(f)
            
        # For stickers, we need to handle them specially
        if msg_type == 'sticker':
            document = InputDocument(
                id=int(media_data['id']),  # Make sure we convert to int
                access_hash=int(media_data['access_hash']),  # Make sure we convert to int
                file_reference=bytes.fromhex(media_data['file_reference'].replace('0500', '', 1))
            )
            await client.send_file(channel, document, **kwargs)
            return True
        else:  # For other media types
            document = InputDocument(
                id=media_data['id'],
                access_hash=media_data['access_hash'],
                file_reference=bytes.fromhex(media_data['file_reference'].replace('0500', '', 1))
            )
            await client.send_file(channel, document, **kwargs)
            return True
    except Exception as e:
        logger.error(f"Error sending media with metadata: {str(e)}")
        return False

async def get_recent_messages(client, limit=5, use_topic=False, topic_id=None):
    channel = await client.get_entity(TARGET_GROUP)
    messages = []
    kwargs = {}
    if use_topic:
        kwargs['reply_to'] = topic_id
    async for message in client.iter_messages(channel, limit=limit, **kwargs):
        messages.append(message)
    return messages[::-1]  # 反转消息列表，使最早的消息在前面

async def main():
    args = parse_args()
    topic_id = args.topic_id if args.topic else None
    logger.info(f"Using topic mode: {args.topic}, topic ID: {topic_id}")
    logger.info(f"Loop mode: {args.loop}")
    
    # 使用新的初始化方法
    clients = await init_clients()
    
    if not clients:
        logger.error("错误: 没有成功连接的客户端!")
        return
    
    logger.info(f"成功初始化 {len(clients)} 个客户端")
    
    # 保存session文件名与客户端的映射
    session_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]
    client_sessions = dict(zip(clients, session_files))
    
    while True:  # 添加无限循环
        # 处理消息发送
        for i in range(0, len(messages), len(clients)):
            # 获取最近的消息
            try:
                recent_messages = await get_recent_messages(clients[0], limit=5, 
                                                          use_topic=args.topic, 
                                                          topic_id=topic_id)
            except Exception as e:
                logger.error(f"获取最近消息失败: {str(e)}")
                recent_messages = []
            
            batch_messages = messages[i:i + len(clients)]
            if not batch_messages:
                break
                
            available_clients = clients.copy()
            random.shuffle(available_clients)
            
            for msg, client in zip(batch_messages, available_clients):
                # 使用带重试的处理函数
                new_client = await process_action_with_retry(
                    client, msg, recent_messages, args.topic, topic_id, 
                    client_sessions[client]
                )
                if new_client and new_client != client:
                    # 更新客户端列表中的客户端
                    clients[clients.index(client)] = new_client
                    client_sessions[new_client] = client_sessions.pop(client)
                
                await asyncio.sleep(random.uniform(9, 20))
        
        if not args.loop:  # 如果不是循环模式，跳出循环
            break
        logger.info("所有消息发送完成，开始新一轮发送...")
        await asyncio.sleep(1)  # 在重新开始前稍作暂停
    
    # 关闭所有客户端
    for client in clients:
        await client.disconnect()

def parse_args():
    parser = argparse.ArgumentParser(description='Telegram message sender')
    parser.add_argument('--topic', action='store_true', 
                       help='Enable topic mode for forum channels')
    parser.add_argument('--topic-id', type=int,
                       help=f'Topic ID for forum channels (default: {TOPIC_ID})')
    parser.add_argument('--loop', action='store_true',
                       help='Enable continuous message sending mode')
    args = parser.parse_args()
    
    # 如果启用了topic模式但没有指定topic-id，使用默认的TOPIC_ID
    if args.topic and args.topic_id is None:
        args.topic_id = TOPIC_ID
        
    return args

if __name__ == "__main__":
    asyncio.run(main())