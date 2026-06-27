"""
inference.py — 实时流量预测推理引擎
=====================================
在 Django 服务启动时预加载 RandomForest 模型，
提供 predict_next_5m(room_id) 函数，
实时查询 MongoDB 过去 3 分钟日志 → 特征对齐 → 模型推理 → 返回 Delta 预测值。

特征顺序与 train_model.py v2.0 严格一致（6 列，无 online_count_avg）。
"""

import logging
import os
from datetime import datetime, timedelta

import joblib
import redis
from django.conf import settings

from mongo_client import get_mongo_client

logger = logging.getLogger(__name__)

# ============================================================
# 模型预加载（服务启动时执行一次）
# ============================================================

_MODEL = None
_MODEL_PATH = os.path.join(settings.BASE_DIR, 'traffic_rf_model.pkl')

# 特征列顺序 — 必须与 train_model.py 中 FEATURE_COLS 完全一致
FEATURE_ORDER = [
    'danmu_total_rolling',
    'danmu_positive_rolling',
    'danmu_negative_rolling',
    'danmu_avg_sentiment_rolling',
    'gift_value_rolling',
    'gift_count_rolling',
]

# 情感阈值 — 与 build_dataset.py 及 consumer.py 保持一致
POS_THRESHOLD = 0.6
NEG_THRESHOLD = 0.4

# Redis 缓存配置
REDIS_PREDICTION_TTL = 10  # 预测结果缓存 10 秒

# 柔性降级占位响应：模型未加载 / 数据积累中时统一返回此结构
GRACEFUL_FALLBACK = {
    'predicted_delta': 0,
    'prediction_status': 'stable',
    'msg': '数据积累中...',
    'is_placeholder': True,
    'cached': False,
    'features': {},
}


def _load_model():
    """惰性加载模型文件（joblib 反序列化）"""
    global _MODEL
    if _MODEL is None:
        if not os.path.exists(_MODEL_PATH):
            logger.warning(f"模型文件不存在: {_MODEL_PATH}，预测功能不可用")
            return None
        try:
            _MODEL = joblib.load(_MODEL_PATH)
            logger.info(f"流量预测模型已加载: {_MODEL_PATH} "
                        f"({os.path.getsize(_MODEL_PATH) / 1024:.1f} KB)")
        except Exception as e:
            logger.error(f"模型加载失败: {e}")
            return None
    return _MODEL


def _extract_features(room_id: str) -> list:
    """
    实时查询 MongoDB 过去 3 分钟数据，提取与训练集对齐的 6 维特征向量。

    返回: [danmu_total, danmu_positive, danmu_negative,
           danmu_avg_sentiment, gift_value, gift_count]
    若无数据则返回全零向量。
    """
    mongo = get_mongo_client()
    now_ms = int(datetime.now().timestamp() * 1000)
    three_min_ago = now_ms - 3 * 60 * 1000

    time_filter = {'$gte': three_min_ago, '$lte': now_ms}

    # ---- 弹幕特征 ----
    danmu_col = mongo.get_danmu_collection()
    danmu_cursor = danmu_col.find(
        {'room_id': room_id, 'timestamp': time_filter},
        {'sentiment': 1}
    )
    danmu_list = list(danmu_cursor)
    danmu_total = len(danmu_list)

    if danmu_total > 0:
        sentiments = [d.get('sentiment', 0.5) for d in danmu_list]
        danmu_positive = sum(1 for s in sentiments if s > POS_THRESHOLD)
        danmu_negative = sum(1 for s in sentiments if s < NEG_THRESHOLD)
        danmu_avg_sentiment = sum(sentiments) / danmu_total
    else:
        danmu_positive = 0
        danmu_negative = 0
        danmu_avg_sentiment = 0.5  # 中性默认值

    # ---- 礼物特征 ----
    gift_col = mongo.get_gift_collection()
    gift_cursor = gift_col.find(
        {'room_id': room_id, 'timestamp': time_filter},
        {'value_yinlang': 1}
    )
    gift_list = list(gift_cursor)
    gift_count = len(gift_list)
    gift_value = sum(g.get('value_yinlang', 0) for g in gift_list)

    return [
        danmu_total,
        danmu_positive,
        danmu_negative,
        danmu_avg_sentiment,
        gift_value,
        gift_count,
    ]


def _get_redis():
    """获取 Redis 连接（带超时保护）"""
    try:
        return redis.Redis(
            host=settings.REDIS_CONFIG['host'],
            port=settings.REDIS_CONFIG['port'],
            db=settings.REDIS_CONFIG['db'],
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    except Exception:
        return None


# ============================================================
# 核心推理函数
# ============================================================

def predict_next_5m(room_id: str, use_cache: bool = True) -> dict:
    """
    预测指定直播间未来 5 分钟的在线人数变化量。
    内置柔性降级：模型缺失 / 数据积累中均返回占位 JSON，绝不抛异常。

    Args:
        room_id: 直播间 ID
        use_cache: 是否使用 Redis 缓存（默认开启，TTL 10 秒）

    Returns:
        {
            'predicted_delta': float,      # 预测人数变化量
            'prediction_status': str,      # 'up' / 'down' / 'stable'
            'msg': str,                    # 状态说明（数据积累中 / 正常预测）
            'is_placeholder': bool,        # 是否占位响应
            'cached': bool,
            'features': dict,
        }
        永不返回 None，异常时统一返回 GRACEFUL_FALLBACK 结构。
    """
    import json

    # ---- 防线 1：模型文件缺失或加载失败 → 柔性降级 ----
    model = _load_model()
    if model is None:
        logger.warning(f"predict_next_5m: 模型未就绪，返回占位响应 (room_id={room_id})")
        return dict(GRACEFUL_FALLBACK)

    # ---- Redis 缓存检查 ----
    r = _get_redis() if use_cache else None
    cache_key = f'prediction:{room_id}'

    if r:
        try:
            cached = r.get(cache_key)
            if cached:
                result = json.loads(cached)
                result['cached'] = True
                r.close()
                return result
        except Exception:
            pass  # Redis 异常时降级为直接推理

    # ---- 实时特征提取 ----
    try:
        features = _extract_features(room_id)
    except Exception as e:
        logger.error(f"predict_next_5m: 特征提取异常 (room_id={room_id}): {e}")
        return dict(GRACEFUL_FALLBACK)

    # ---- 防线 2：数据库无数据（冷启动 / 刚清空 / 刚开播）→ 柔性降级 ----
    # danmu_total=0 且 gift_value=0 且 gift_count=0 → 过去 3 分钟无任何互动
    if features[0] == 0 and features[4] == 0 and features[5] == 0:
        logger.info(f"predict_next_5m: 过去 3 分钟无数据，数据积累中 (room_id={room_id})")
        return dict(GRACEFUL_FALLBACK)

    # ---- 模型推理 ----
    try:
        delta = float(model.predict([features])[0])
    except Exception as e:
        logger.error(f"predict_next_5m: 模型推理异常 (room_id={room_id}): {e}")
        return dict(GRACEFUL_FALLBACK)

    # ---- 判定状态 ----
    if delta > 10:
        status = 'up'
    elif delta < -10:
        status = 'down'
    else:
        status = 'stable'

    result = {
        'predicted_delta': round(delta, 1),
        'prediction_status': status,
        'msg': '正常预测',
        'is_placeholder': False,
        'cached': False,
        'features': {
            'danmu_total_rolling': features[0],
            'danmu_positive_rolling': features[1],
            'danmu_negative_rolling': features[2],
            'danmu_avg_sentiment_rolling': round(features[3], 4),
            'gift_value_rolling': features[4],
            'gift_count_rolling': features[5],
        },
    }

    # ---- 写入缓存（占位响应不缓存，确保数据到来后立即更新）----
    if r:
        try:
            r.setex(cache_key, REDIS_PREDICTION_TTL, json.dumps(result))
        except Exception:
            pass
        finally:
            r.close()

    return result
