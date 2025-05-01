# 默认配置
DEFAULT_CONFIG = {
    'topic_id': None,  # 默认不使用 topic
    'use_topic': False,  # 默认不启用 topic 模式
    'sleep_time': {
        'min': 9,
        'max': 20
    }
}

# 群组特定配置
GROUP_CONFIGS = {
    'memolabs': {
        'target_group': 'https://t.me/MemoLabsio',
        'sessions_dir': 'sessions/memolabs',
        'messages_file': 'Messages and Media/MemoLabs/MemoLabs_messages.csv',
        'media_dir': 'Messages and Media/MemoLabs/media',
        'use_topic': False,  # 启用 topic 模式
        'topic_id': 1,      # 指定 topic ID
        'sleep_time': {
            'min': 9,
            'max': 20
        }
    },
    'Hopper': {
        'target_group': 'https://t.me/hopper_global',
        'sessions_dir': 'sessions/hopper',
        'messages_file': 'Messages and Media/Hopper/Hopper_messages.csv',
        'media_dir': 'Messages and Media/Hopper/media',
        'use_topic': False,  # 不使用 topic 模式
        'sleep_time': {
            'min': 15,
            'max': 30
        }
    },
    # 新群组配置示例
    'SuperEx': {
        'target_group': 'https://t.me/SuperExOfficial',  # 替换为新群组的链接
        'sessions_dir': 'sessions/superex',           # session文件目录
        'messages_file': 'Messages and Media/SuperEx/SuperEx_messages.csv',  # 消息CSV文件
        'media_dir': 'Messages and Media/SuperEx/media',            # 媒体文件目录
        'use_topic': False,  # 是否使用话题模式
        'sleep_time': {
            'min': 70,
            'max': 120
        }
    }
    # 添加更多群组配置...
}

def get_group_config(group_name):
    """获取群组配置，合并默认配置"""
    if group_name not in GROUP_CONFIGS:
        raise ValueError(f"未找到群组配置: {group_name}")
    
    # 创建默认配置的副本
    config = DEFAULT_CONFIG.copy()
    # 更新群组特定配置
    config.update(GROUP_CONFIGS[group_name])
    return config
