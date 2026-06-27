"""
build_dataset.py — 抖音直播流量预测 特征工程脚本 (v3.0)
==========================================================
从 MongoDB 日志集合中提取弹幕、礼物、热度数据，
经清洗、分钟级重采样、场次切割、滑动窗口特征构建后，
导出为机器学习训练集 CSV 文件。

v3.0 变更：
  - 新增"断流切割器"：自动识别主播下播空白期（>15分钟），切分为独立 session
  - ffill / rolling / shift 全部基于 groupby('session_id') 隔离，杜绝跨场次数据污染
  - 适配 3-5 天无人值守挂机采集场景

v2.0 变更：
  - 目标变量改为 5 分钟后在线人数变化量 (target_delta_5m)
  - 输出中移除 online_count_avg，切断模型"用历史均值猜未来"的捷径

用法：
    python build_dataset.py <room_id> [--output training_data.csv]
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

# ============================================================
# 0. 配置常量
# ============================================================

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'douyin_live'

# 情感分类阈值（与 consumer.py 中 SnowNLP 的分段逻辑保持一致）
SENTIMENT_POS_THRESHOLD = 0.6   # > 0.6 为正向
SENTIMENT_NEG_THRESHOLD = 0.4   # < 0.4 为负向

# 滑动窗口与预测步长
ROLLING_WINDOW_MINUTES = 3      # 过去 N 分钟的滑动窗口
TARGET_SHIFT_MINUTES = 5        # 预测未来 N 分钟后的在线人数变化量

# 场次切割阈值：相邻两条数据时间差超过此值视为新的一场直播
SESSION_GAP_THRESHOLD_MINUTES = 15


# ============================================================
# 1. 数据加载
# ============================================================

def connect_mongo() -> MongoClient:
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    client.admin.command('ping')
    print(f"[INFO] MongoDB 连接成功: {MONGO_HOST}:{MONGO_PORT}")
    return client


def load_collection_to_df(client: MongoClient, collection_name: str,
                          room_id: str) -> pd.DataFrame:
    db = client[MONGO_DB]
    collection = db[collection_name]
    cursor = collection.find(
        {'room_id': room_id},
        {'_id': 0}
    ).sort('timestamp', 1)
    df = pd.DataFrame(list(cursor))
    if df.empty:
        print(f"[WARN] 集合 '{collection_name}' 中 room_id='{room_id}' 无数据")
    return df


def load_all_data(client: MongoClient, room_id: str) -> tuple:
    print(f"\n[INFO] 正在加载 room_id='{room_id}' 的数据...")
    df_hot = load_collection_to_df(client, 'hot_trend', room_id)
    print(f"  +-- hot_trend:  {len(df_hot):>8} 条")
    df_danmu = load_collection_to_df(client, 'danmu_log', room_id)
    print(f"  +-- danmu_log:  {len(df_danmu):>8} 条")
    df_gift = load_collection_to_df(client, 'gift_log', room_id)
    print(f"  +-- gift_log:   {len(df_gift):>8} 条")
    return df_hot, df_danmu, df_gift


# ============================================================
# 2. 时间戳转换与索引设定
# ============================================================

def convert_timestamp(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.set_index('datetime')
    df = df.sort_index()
    df = df[~df.index.duplicated(keep='first')]
    print(f"[INFO] {label}: 时间范围 {df.index.min()} ~ {df.index.max()}, "
          f"跨度 {df.index.max() - df.index.min()}")
    return df


# ============================================================
# 3. 数据清洗
# ============================================================

def clean_hot_trend(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df['online_count'] = pd.to_numeric(df['online_count'], errors='coerce')
    df.loc[df['online_count'] <= 0, 'online_count'] = pd.NA
    null_before = df['online_count'].isna().sum()
    df['online_count'] = df['online_count'].ffill()
    null_after = df['online_count'].isna().sum()
    print(f"[INFO] hot_trend 清洗: ffill 填补了 {null_before - null_after} 个空值, "
          f"剩余 NaN: {null_after}")
    df['online_count'] = df['online_count'].fillna(0)
    return df


# ============================================================
# 4. 分钟级重采样
# ============================================================

def resample_hot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['online_count'])
    resampled = df['online_count'].resample('1min').last()
    # 不在此处 ffill：保留 NaN 空洞供断流切割器识别主播下播空白期
    # ffill 将在 merge_and_build_features 中按 session_id 分组执行
    return resampled.to_frame(name='online_count')


def resample_danmu(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            'danmu_total', 'danmu_positive', 'danmu_negative',
            'danmu_avg_sentiment'
        ])
    resampled = df.resample('1min').agg(
        danmu_total=('sentiment', 'count'),
        danmu_positive=('sentiment', lambda x: (x > SENTIMENT_POS_THRESHOLD).sum()),
        danmu_negative=('sentiment', lambda x: (x < SENTIMENT_NEG_THRESHOLD).sum()),
        danmu_avg_sentiment=('sentiment', 'mean'),
    )
    resampled = resampled.fillna({
        'danmu_total': 0,
        'danmu_positive': 0,
        'danmu_negative': 0,
        'danmu_avg_sentiment': 0.5,
    })
    return resampled


def resample_gift(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['gift_total_value', 'gift_count'])
    resampled = df.resample('1min').agg(
        gift_total_value=('value_yinlang', 'sum'),
        gift_count=('username', 'count'),
    )
    resampled = resampled.fillna({
        'gift_total_value': 0,
        'gift_count': 0,
    })
    return resampled


# ============================================================
# 5. 场次切割：识别主播下播空白期，防止跨场次数据污染
# ============================================================

def segment_sessions(df: pd.DataFrame,
                     gap_threshold_minutes: int = SESSION_GAP_THRESHOLD_MINUTES) -> pd.DataFrame:
    """
    根据相邻两行数据的时间间隔自动切分直播场次。

    逻辑：
      计算 DataFrame 索引（分钟级时间戳）相邻两行的差值。
      若差值 > gap_threshold_minutes（默认 15 分钟），则认为主播在此期间下播，
      自增 session_id，将数据切分为独立场次。

    参数:
      df: 已合并的分钟级 DataFrame，索引为 datetime
      gap_threshold_minutes: 断流判定阈值（分钟）

    返回:
      增加 'session_id' 列的 DataFrame（int, 从 0 开始自增）
    """
    if df.empty:
        df['session_id'] = 0
        return df

    # 计算相邻时间差（首次为 NaT → 填充为 0，属于第一场）
    time_delta = df.index.to_series().diff()
    gap_threshold = pd.Timedelta(minutes=gap_threshold_minutes)

    # 时间差超过阈值 → 新场次开始，cumsum 生成自增 session_id
    df['session_id'] = (time_delta > gap_threshold).cumsum()
    session_count = df['session_id'].nunique()

    print(f"\n[INFO] 场次切割完成: 共识别 {session_count} 个独立直播场次")
    print(f"[INFO] 断流阈值: {gap_threshold_minutes} 分钟")
    for sid, group in df.groupby('session_id'):
        print(f"  Session {int(sid)}: {group.index.min()} ~ {group.index.max()}, "
              f"持续 {group.index.max() - group.index.min()}, "
              f"{len(group)} 分钟")

    return df


# ============================================================
# 6. 合并、滑动窗口特征、Delta 目标构建
# ============================================================

def merge_and_build_features(df_hot_resampled: pd.DataFrame,
                              df_danmu_resampled: pd.DataFrame,
                              df_gift_resampled: pd.DataFrame) -> pd.DataFrame:
    """
    合并三张重采样表 → 场次切割 → 按 session 隔离构建特征与标签。

    特征 (X) — 过去 3 分钟窗口内（场次内隔离）：
      - danmu_total_rolling          弹幕总量
      - danmu_positive_rolling       正向弹幕量
      - danmu_negative_rolling       负向弹幕量
      - danmu_avg_sentiment_rolling  平均情感得分
      - gift_value_rolling           礼物总价值（音浪）
      - gift_count_rolling           送礼人次

    目标 (Y)：
      - target_delta_5m              5 分钟后在线人数变化量（场次内，跨场次为 NaN）

    关键变更 (v3.0)：
      ffill / rolling / shift 全部在 groupby('session_id') 内执行，
      杜绝跨场次数据污染。
    """
    # ---------- Step 1: 沿时间轴外连接 ----------
    df_merged = df_hot_resampled.join(df_danmu_resampled, how='outer')
    df_merged = df_merged.join(df_gift_resampled, how='outer')

    # 弹幕/礼物缺失 → 视为 0（无互动），online_count 缺失 → 保持 NaN（后续按场次填充）
    df_merged = df_merged.fillna({
        'danmu_total': 0,
        'danmu_positive': 0,
        'danmu_negative': 0,
        'danmu_avg_sentiment': 0.5,
        'gift_total_value': 0,
        'gift_count': 0,
    })

    print(f"\n[INFO] 合并后时间跨度: {df_merged.index.min()} ~ {df_merged.index.max()}")
    print(f"[INFO] 合并后总分钟数: {len(df_merged)}")

    # ---------- Step 2: 断流切割 —— 识别主播下播空白期 ----------
    df_merged = segment_sessions(df_merged)

    # ---------- Step 3: 按场次隔离执行所有时序相关操作 ----------
    window = ROLLING_WINDOW_MINUTES

    def _process_one_session(group: pd.DataFrame) -> pd.DataFrame:
        """
        对单个场次内部执行：
          1. ffill 填补 online_count 的分钟级空洞
          2. 滑动窗口 (rolling) 构建互动特征
          3. shift(-5) 构造未来 5 分钟 Delta 标签

        场次第一行之前的 NaN 和最后 5 行之后无法计算的目标，
        将在最终 dropna() 中自动剔除。
        """
        # --- 3.1 在线人数前向填充（场次内部容错，不跨越下播空白期）---
        group['online_count'] = group['online_count'].ffill()

        # --- 3.2 滑动窗口特征（仅弹幕 + 礼物维度）---
        group['danmu_total_rolling'] = (
            group['danmu_total']
            .rolling(window=window, min_periods=1)
            .sum()
        )
        group['danmu_positive_rolling'] = (
            group['danmu_positive']
            .rolling(window=window, min_periods=1)
            .sum()
        )
        group['danmu_negative_rolling'] = (
            group['danmu_negative']
            .rolling(window=window, min_periods=1)
            .sum()
        )
        group['danmu_avg_sentiment_rolling'] = (
            group['danmu_avg_sentiment']
            .rolling(window=window, min_periods=1)
            .mean()
        )
        group['gift_value_rolling'] = (
            group['gift_total_value']
            .rolling(window=window, min_periods=1)
            .sum()
        )
        group['gift_count_rolling'] = (
            group['gift_count']
            .rolling(window=window, min_periods=1)
            .sum()
        )

        # --- 3.3 目标变量：5 分钟后在线人数变化量（场次内）---
        group['online_count_future'] = group['online_count'].shift(-TARGET_SHIFT_MINUTES)
        group['target_delta_5m'] = (
            group['online_count_future'] - group['online_count']
        )

        return group

    # 仅对有效场次执行时序操作（session_id >= 0 的数据块）
    valid_mask = df_merged['session_id'] >= 0
    df_valid = df_merged.loc[valid_mask].copy()
    df_invalid = df_merged.loc[~valid_mask]

    if not df_valid.empty:
        df_valid = df_valid.groupby('session_id', group_keys=False).apply(
            _process_one_session
        )
        df_merged = pd.concat([df_valid, df_invalid]).sort_index()
    else:
        print("[WARN] merge_and_build_features: 无有效场次数据")

    return df_merged


# ============================================================
# 6. 导出
# ============================================================

def export_dataset(df: pd.DataFrame, output_path: str):
    """
    剔除含 NaN 的行，并采用【追加模式】输出为 CSV。
    这样连续处理多个 room_id 时，数据会自动融合在一起。
    """
    feature_cols = [
        'danmu_total_rolling',
        'danmu_positive_rolling',
        'danmu_negative_rolling',
        'danmu_avg_sentiment_rolling',
        'gift_value_rolling',
        'gift_count_rolling',
    ]
    target_col = 'target_delta_5m'
    meta_cols = ['session_id']
    output_cols = feature_cols + [target_col] + meta_cols

    df_out = df[output_cols].copy()

    before = len(df_out)
    df_out = df_out.dropna()
    after = len(df_out)

    print(f"\n[INFO] 剔除 NaN 行: {before} -> {after} (丢弃 {before - after} 行)")

    # ===== 阿爸加的追加写入核心逻辑 =====
    if not os.path.exists(output_path):
        # 第一次跑：文件不存在，写入表头 (header=True)
        df_out.to_csv(output_path, index=True, index_label='datetime',
                      encoding='utf-8-sig', float_format='%.4f')
        print(f"[INFO] 首次创建并导出训练集: {output_path}")
    else:
        # 后续跑：文件已存在，追加数据 (mode='a') 且不写表头 (header=False)
        df_out.to_csv(output_path, mode='a', header=False, index=True, index_label='datetime',
                      encoding='utf-8-sig', float_format='%.4f')
        print(f"[INFO] 数据已成功【追加】到训练集: {output_path}")
    # ===================================

    print(f"[INFO] 本次加入样本数: {after} 行")

    print(f"\n{'='*60}")
    print("数据集摘要：")
    print(f"{'='*60}")
    print(df_out.describe().to_string())


# ============================================================
# 7. 主流程
# ============================================================

def build_dataset(room_id: str, output_path: str = 'training_data.csv'):
    print(f"{'='*60}")
    print(f"  抖音直播流量预测 — 特征工程 (v3.0 - 场次切割 + Delta 目标)")
    print(f"  Room ID: {room_id}")
    print(f"  滑动窗口: {ROLLING_WINDOW_MINUTES} 分钟")
    print(f"  预测目标: {TARGET_SHIFT_MINUTES} 分钟后在线人数变化量")
    print(f"  场次切割阈值: {SESSION_GAP_THRESHOLD_MINUTES} 分钟")
    print(f"{'='*60}")

    # Step 1: 连接并加载
    client = connect_mongo()
    df_hot, df_danmu, df_gift = load_all_data(client, room_id)
    client.close()

    if df_hot.empty:
        print("[ERROR] hot_trend 无数据，无法继续（缺少目标变量数据源）。")
        sys.exit(1)
    if df_danmu.empty:
        print("[WARN] danmu_log 无数据，弹幕特征将全部为 0。")
    if df_gift.empty:
        print("[WARN] gift_log 无数据，礼物特征将全部为 0。")

    # Step 2: 时间戳转换
    print(f"\n[INFO] === 时间戳转换 ===")
    df_hot = convert_timestamp(df_hot, 'hot_trend')
    df_danmu = convert_timestamp(df_danmu, 'danmu_log')
    df_gift = convert_timestamp(df_gift, 'gift_log')

    # Step 3: 数据清洗
    print(f"\n[INFO] === 数据清洗 ===")
    df_hot = clean_hot_trend(df_hot)

    # Step 4: 分钟级重采样
    print(f"\n[INFO] === 分钟级重采样 ===")
    df_hot_1min = resample_hot(df_hot)
    df_danmu_1min = resample_danmu(df_danmu)
    df_gift_1min = resample_gift(df_gift)
    print(f"  hot_trend 重采样后:  {len(df_hot_1min):>6} 分钟")
    print(f"  danmu_log 重采样后:  {len(df_danmu_1min):>6} 分钟")
    print(f"  gift_log 重采样后:   {len(df_gift_1min):>6} 分钟")

    # Step 5: 合并 + 场次切割 + 会话隔离滑动窗口特征 + Delta 目标
    print(f"\n[INFO] === 合并 & 场次切割 & 滑动窗口特征构建 ===")
    df_dataset = merge_and_build_features(df_hot_1min, df_danmu_1min, df_gift_1min)

    # Step 6: 导出
    print(f"\n[INFO] === 导出数据集 ===")
    export_dataset(df_dataset, output_path)

    print(f"\n{'='*60}")
    print(f"  特征工程完成!")
    print(f"{'='*60}")


# ============================================================
# 入口
# ============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='抖音直播流量预测 — 特征工程脚本 (v3.0)'
    )
    parser.add_argument(
        'room_id', type=str, nargs='?', default=None,
        help='目标直播间 ID（位置参数）'
    )
    parser.add_argument(
        '--room_id', type=str, dest='room_id_flag', default=None,
        help='目标直播间 ID（可选标志）'
    )
    parser.add_argument(
        '--output', type=str, default='training_data.csv',
        help='输出 CSV 文件路径（默认: training_data.csv）'
    )
    args = parser.parse_args()

    room_id = args.room_id or args.room_id_flag
    if not room_id:
        parser.error('必须指定 room_id')

    build_dataset(room_id=room_id, output_path=args.output)
