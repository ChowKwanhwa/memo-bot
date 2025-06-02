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
from config import PROXY_LIST  # Import proxy list from config.py
from group_configs import get_group_config, GROUP_CONFIGS
import time

# 加载.env文件
load_dotenv()

# 从环境变量获取API凭据
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

# 表情符号列表用于reactions
REACTION_EMOJIS = ['👍',  '🔥', '🎉', '🔥']

async def try_connect_with_proxy(sessions_dir, session_file, proxy_config):
    """尝试使用特定代理连接"""
    session_path = os.path.join(sessions_dir, session_file.replace('.session', ''))
    client = TelegramClient(session_path, API_ID, API_HASH, proxy=proxy_config)
    
    try:
        await client.connect()
        if await client.is_user_authorized():
            return client
        await client.disconnect()
        return None
    except Exception as e:
        try:
            await client.disconnect()
        except:
            pass
        return None

async def try_join_channel(client, channel_url):
    """尝试加入频道"""
    try:
        channel = await client.get_entity(channel_url)
        return True
    except Exception as e:
        print(f"Error getting channel info: {str(e)}")
        return False

async def init_clients(sessions_dir, target_group):
    """初始化所有客户端，使用代理轮换机制"""
    session_files = [f for f in os.listdir(sessions_dir) if f.endswith('.session')]
    clients = []
    
    for session_file in session_files:
        proxy = random.choice(PROXY_LIST)
        client = await try_connect_with_proxy(sessions_dir, session_file, proxy)
        if client:
            clients.append(client)
    
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
        new_client = await try_connect_with_proxy("sessions", session_file, proxy)
        if new_client:
            return new_client
    
    return None

async def get_recent_messages(client, limit=5, use_topic=False, topic_id=None, target_group=None):
    """获取最近的消息"""
    try:
        channel = await client.get_entity(target_group)
        messages = []
        kwargs = {}
        if use_topic:
            kwargs['reply_to'] = topic_id
        
        # 确保客户端在这个群组中
        try:
            participant = await client.get_participants(channel, limit=1)
        except Exception as e:
            print(f"Client is not in the group {target_group}: {str(e)}")
            return []
            
        async for message in client.iter_messages(channel, limit=limit, **kwargs):
            messages.append(message)
        return messages[::-1]  # 反转消息列表，使最早的消息在前面
    except Exception as e:
        print(f"Error getting recent messages from {target_group}: {str(e)}")
        return []

async def process_action(client, message_data, recent_messages, use_topic, topic_id, target_group, media_dir):
    """处理一个动作"""
    try:
        channel = await client.get_entity(target_group)
        kwargs = {}
        if use_topic:
            kwargs['reply_to'] = topic_id
            
        content = message_data['content']
        if content and content not in ['[PHOTO]', '[FILE]', '[STICKER]']:
            await client.send_message(channel, content, **kwargs)
            return
            
        msg_type = message_data['type']
        media_path = message_data.get('media_file', '')
        if media_path and msg_type in ['sticker', 'video', 'photo', 'file']:
            full_path = os.path.join(os.getcwd(), media_dir, os.path.basename(media_path))
            try:
                if msg_type == 'sticker':
                    success = await send_media_with_metadata(client, channel, full_path, msg_type, **kwargs)
                    if not success:
                        return
                else:
                    await client.send_file(channel, full_path, **kwargs)
            except Exception as e:
                print(f"Error sending media: {str(e)}")
                
    except Exception as e:
        print(f"Error processing action: {str(e)}")

async def process_action_with_retry(client, message_data, recent_messages, use_topic, topic_id, session_file, target_group, media_dir):
    """处理动作，如果出现连接错误则尝试重新连接"""
    try:
        await process_action(client, message_data, recent_messages, use_topic, topic_id, target_group, media_dir)
    except Exception as e:
        error_str = str(e)
        if "Proxy connection timed out" in error_str:
            print(f"代理连接超时，尝试重新连接...")
            new_client = await reconnect_client(client, session_file)
            if new_client:
                print("重新连接成功，重试操作...")
                await process_action(new_client, message_data, recent_messages, use_topic, topic_id, target_group, media_dir)
                return new_client
        else:
            print(f"Error processing action: {error_str}")
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
        print(f"Error sending media with metadata: {str(e)}")
        return False

async def load_messages(messages_file):
    """读取消息数据"""
    try:
        # 先读取CSV文件的标题行来确定列数
        df = pd.read_csv(messages_file, nrows=1)
        expected_columns = len(df.columns)
        
        # 使用指定的列名重新读取文件，跳过错误行
        df = pd.read_csv(messages_file, 
                         names=['id', 'date', 'type', 'content', 'media_file'],
                         on_bad_lines='skip')
        
        # 清理数据：删除可能的空行和无效行
        df = df.dropna(subset=['id', 'type'])  # 确保至少有id和type列有值
        
        # 反转DataFrame，使最早的消息（底部）在前面
        df = df.iloc[::-1]
        messages = df.to_dict('records')
        print(f"Successfully loaded {len(messages)} messages from {messages_file}")
        return messages
    except Exception as e:
        print(f"Error reading CSV file {messages_file}: {e}")
        return []

async def run_group(group_name, config, loop_mode):
    print(f"\nStarting group: {group_name}")
    print(f"Target group: {config['target_group']}")
    print(f"Using topic mode: {config['use_topic']}, topic ID: {config['topic_id']}")
    print(f"Loop mode: {loop_mode}")
    
    # 读取消息数据
    try:
        messages = await load_messages(config['messages_file'])
        if not messages:
            print(f"No messages found for group {group_name}")
            return
    except Exception as e:
        print(f"Error loading messages for group {group_name}: {e}")
        return
    
    # 初始化客户端
    clients = await init_clients(config['sessions_dir'], config['target_group'])
    
    if not clients:
        print(f"Error: No successful client connections for group {group_name}!")
        return
    
    print(f"Successfully initialized {len(clients)} clients for {group_name}")
    
    # 保存session文件名与客户端的映射
    session_files = [f for f in os.listdir(config['sessions_dir']) if f.endswith('.session')]
    client_sessions = dict(zip(clients, session_files))
    
    while True:  # 主循环
        for i in range(0, len(messages), len(clients)):
            batch_messages = messages[i:i + len(clients)]
            if not batch_messages:
                break
                
            available_clients = clients.copy()
            random.shuffle(available_clients)
            
            for msg, client in zip(batch_messages, available_clients):
                try:
                    await process_action_with_retry(
                        client, msg, [],
                        config['use_topic'], config['topic_id'],
                        os.path.join(config['sessions_dir'], client_sessions[client]),
                        config['target_group'],
                        config['media_dir']
                    )
                    
                    sleep_time = random.uniform(
                        config['sleep_time']['min'],
                        config['sleep_time']['max']
                    )
                    await asyncio.sleep(sleep_time)
                except Exception as e:
                    print(f"Error processing message in {group_name}: {str(e)}")
                    continue
        
        if not loop_mode:
            break
        print(f"All messages sent for {group_name}, starting new round...")
        await asyncio.sleep(1)
    
    # 关闭所有客户端
    for client in clients:
        try:
            await client.disconnect()
        except:
            pass

async def main():
    args = parse_args()
    
    # 确定哪些组需要运行
    groups_to_run = []
    if args.groups:
        for group in args.groups:
            if group not in GROUP_CONFIGS:
                print(f"Error: Group '{group}' not found in configuration")
                continue
            groups_to_run.append(group)
    else:
        # 只运行有session目录的组
        for group_name, config in GROUP_CONFIGS.items():
            sessions_dir = config['sessions_dir']
            if os.path.exists(sessions_dir):
                groups_to_run.append(group_name)
            else:
                print(f"Skipping group '{group_name}': session directory not found at {sessions_dir}")
    
    if not groups_to_run:
        print("No valid groups to run!")
        return
    
    # 为每个组创建任务
    tasks = []
    for group_name in groups_to_run:
        try:
            config = get_group_config(group_name)
            
            # 验证必要的目录和文件是否存在
            if not os.path.exists(config['sessions_dir']):
                print(f"Error: Session directory not found for {group_name}: {config['sessions_dir']}")
                continue
                
            if not os.path.exists(config['messages_file']):
                print(f"Error: Messages file not found for {group_name}: {config['messages_file']}")
                continue
                
            if not os.path.exists(config['media_dir']):
                print(f"Warning: Media directory not found for {group_name}: {config['media_dir']}")
                # 不停止，因为有些消息可能不需要媒体
            
            task = asyncio.create_task(run_group(group_name, config, args.loop))
            tasks.append(task)
            print(f"Created task for group: {group_name}")
            print(f"  - Sessions: {config['sessions_dir']}")
            print(f"  - Messages: {config['messages_file']}")
            print(f"  - Media: {config['media_dir']}")
            print(f"  - Target: {config['target_group']}")
        except ValueError as e:
            print(f"Error setting up {group_name}: {e}")
            continue
    
    if not tasks:
        print("No tasks created - check your configuration and directories")
        return
        
    # 并发运行所有任务
    await asyncio.gather(*tasks)

def parse_args():
    parser = argparse.ArgumentParser(description='Telegram message sender')
    parser.add_argument('--topic', action='store_true',
                       help='Enable topic mode for forum channels')
    parser.add_argument('--topic-id', type=int,
                       help='Topic ID for forum channels')
    parser.add_argument('--loop', action='store_true',
                       help='Enable continuous message sending mode')
    parser.add_argument('--groups', nargs='+', type=str,
                       help='Specify groups to run (e.g., --groups memolabs superexcn). If not specified, all groups will run.')
    args = parser.parse_args()
    
    # 命令行参数覆盖组配置
    if args.topic:
        print("Warning: --topic flag is deprecated. Using group-specific topic settings instead.")
    if args.topic_id:
        print("Warning: --topic-id flag is deprecated. Using group-specific topic IDs instead.")
    
    return args

if __name__ == "__main__":
    asyncio.run(main())