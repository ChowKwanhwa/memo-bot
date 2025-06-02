import pandas as pd

# 读取CSV文件
input_file = 'Messages and Media/MemoLabs3/MemoLabs_messages.csv'
df = pd.read_csv(input_file)

# 过滤掉sticker、图片、media和video相关的消息
# 检查type列和content/media_file列是否包含media或video相关内容
df_filtered = df[
    (~df['type'].isin(['sticker', 'photo', 'media', 'video', 'file'])) & 
    (~df['content'].str.contains('media/video_', na=False)) &
    (~df['media_file'].str.contains('media/video_', na=False))
]

# 保存过滤后的结果到原文件
df_filtered.to_csv(input_file, index=False)
print(f"已从 {input_file} 中删除所有sticker、图片、media和video相关消息")
