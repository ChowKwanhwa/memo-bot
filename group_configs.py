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
        'messages_file': 'Messages and Media/MemoLabs2/Memolabs_messages.csv',
        'media_dir': 'Messages and Media/MemoLabs/media',
        'use_topic': False,  # 启用 topic 模式
        
        'topic_id': 1,      # 指定 topic ID
        'sleep_time': {
            'min': 60,
            'max': 120
        }
    },
    'superex': {
        'target_group': 'https://t.me/SuperExOfficial',  # 替换为新群组的链接
        'sessions_dir': 'sessions/superex',           # session文件目录
        'messages_file': 'Messages and Media/SuperEx/SuperEx_messages.csv',  # 消息CSV文件
        'media_dir': 'Messages and Media/SuperEx/media',            # 媒体文件目录
        'use_topic': False,  # 是否使用话题模式
        'sleep_time': {
            'min': 600,
            'max': 650
        }
    },
    # 添加更多群组配置...
    'superexcn': {
        'target_group': 'https://t.me/SuperExOfficial_CN',
        'sessions_dir': 'sessions/superexcn',
        'messages_file': 'Messages and Media/SuperExCN/SuperExCN_messages.csv',
        'media_dir': 'Messages and Media/SuperExCN/media',
        'use_topic': False,  # 不使用 topic 模式
        'sleep_time': {
            'min': 600,
            'max': 650
        }
    },    
    'superexViet': {
        'target_group': 'https://t.me/SuperEx_Viet',  # 替换为新群组的链接
        'sessions_dir': 'sessions/Superex_Viet',           # session文件目录
        'messages_file': 'Messages and Media/SuperexViet/SuperViet_messages.csv',  # 消息CSV文件
        'media_dir': 'Messages and Media/SuperexViet/media',            # 媒体文件目录
        'use_topic': False,  # 是否使用话题模式
        'sleep_time': {
            'min': 600,
            'max': 650
        }
    },
    'superexru': {
        'target_group': 'https://t.me/superexrussia',
        'sessions_dir': 'sessions/superexru',
        'messages_file': 'Messages and Media/superexru/SuperExRussian_messages.csv',
        'media_dir': 'Messages and Media/MemoLabs/media',
        'use_topic': False,  # 启用 topic 模式
        
        'topic_id': 1,      # 指定 topic ID
        'sleep_time': {
            'min': 9,
            'max': 20
        }
    },
    'superexID': {
        'target_group': 'https://t.me/SuperexID',
        'sessions_dir': 'sessions/SuperexID',
        'messages_file': 'Messages and Media/SuperexID/Superex_id_messages.csv',
        'media_dir': 'Messages and Media/SuperexID/media',
        'use_topic': False,  # 启用 topic 模式
        
        'topic_id': 1,      # 指定 topic ID
        'sleep_time': {
            'min': 9,
            'max': 20
        }
    },
    
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
