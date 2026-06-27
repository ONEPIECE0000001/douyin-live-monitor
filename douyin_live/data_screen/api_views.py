#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据大屏 API 服务层 — 只读 JSON 接口
提供 stats / trend / gift_rank / sentiment 四个查询端点
"""

from datetime import datetime, timedelta
from django.http import JsonResponse
from django.utils import timezone
from django.conf import settings
from bson import ObjectId
import redis

from data_screen.models import LiveRoom
from data_screen.inference import predict_next_5m
from data_screen.auth_views import api_login_required
from mongo_client import get_mongo_client


# ============================================
# 工具：MongoDB ObjectId 序列化 + 异常响应
# ============================================
def serialize_mongo(obj):
    """递归转换 MongoDB ObjectId 和 datetime 为标准 JSON 可序列化类型"""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: serialize_mongo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize_mongo(i) for i in obj]
    return obj


def api_error(msg: str, status: int = 400) -> JsonResponse:
    return JsonResponse({'error': True, 'message': msg}, status=status)


def _get_danmu_collection():
    return get_mongo_client().get_danmu_collection()


def _get_gift_collection():
    return get_mongo_client().get_gift_collection()


# ============================================
# 接口 1：直播间宏观统计 (MySQL)
# ============================================
@api_login_required
def stats(request, room_id: str) -> JsonResponse:
    """GET /api/stats/<room_id>/"""
    try:
        room = LiveRoom.objects.get(room_id=room_id)
    except LiveRoom.DoesNotExist:
        return api_error(f'直播间 {room_id} 不存在', status=404)

    sessions = room.livesession_set.all()

    # 使用模型属性计算真实累计监控时长（仅累加各场次内实际开播时间，与 Admin 一致）
    monitor_duration_str = room.total_duration_str
    # 同时计算秒数，供前端扩展使用
    total_seconds = 0
    now = timezone.now()
    for s in sessions:
        end = s.end_time if s.end_time else now
        total_seconds += int((end - s.start_time).total_seconds())

    # ---- AI 流量预测（附加到 stats 响应中）----
    prediction = None
    try:
        prediction = predict_next_5m(room_id)
    except Exception:
        pass  # 预测失败不阻塞主接口

    response_data = {
        'room_id': room.room_id,
        'host_name': room.host_name,
        'current_online': room.current_online,
        'total_likes': room.total_likes,
        'total_gifts_value': str(room.total_gifts_value),   # Decimal → string 防精度丢失
        'session_count': sessions.count(),
        'active_sessions': sessions.filter(end_time__isnull=True).count(),
        'monitor_duration': monitor_duration_str,
        'monitor_duration_seconds': total_seconds,
    }

    if prediction:
        response_data['prediction'] = {
            'predicted_delta': prediction['predicted_delta'],
            'prediction_status': prediction['prediction_status'],
            'msg': prediction.get('msg', ''),
            'is_placeholder': prediction.get('is_placeholder', False),
        }

    return JsonResponse(response_data)


# ============================================
# 接口 2：弹幕分钟级趋势 (MongoDB 聚合)
# ============================================
@api_login_required
def trend(request, room_id: str) -> JsonResponse:
    """GET /api/charts/trend/<room_id>/
    返回过去 30 分钟按分钟分桶的弹幕数量 + 礼物价值时序数组（双轴联动）
    """
    danmu_col = _get_danmu_collection()
    gift_col = _get_gift_collection()
    thirty_min_ago = int((datetime.now() - timedelta(minutes=30)).timestamp() * 1000)

    # 弹幕分钟级聚合
    danmu_pipeline = [
        {'$match': {
            'room_id': room_id,
            'timestamp': {'$gte': thirty_min_ago},
        }},
        {'$group': {
            '_id': {'$floor': {'$divide': ['$timestamp', 60000]}},
            'count': {'$sum': 1},
        }},
        {'$sort': {'_id': 1}},
        {'$project': {
            '_id': 0,
            'time': {'$multiply': ['$_id', 60000]},
            'count': 1,
        }},
    ]

    # 礼物分钟级价值聚合（音浪 → 元）
    gift_pipeline = [
        {'$match': {
            'room_id': room_id,
            'timestamp': {'$gte': thirty_min_ago},
        }},
        {'$group': {
            '_id': {'$floor': {'$divide': ['$timestamp', 60000]}},
            'gift_value_yuan': {'$sum': '$value_yinlang'},
        }},
        {'$sort': {'_id': 1}},
    ]

    try:
        danmu_results = list(danmu_col.aggregate(danmu_pipeline))
        gift_results = list(gift_col.aggregate(gift_pipeline))

        # 构建礼物分钟索引 → 价值（元）查找表
        gift_map = {r['_id']: round(r['gift_value_yuan'] / 10, 2) for r in gift_results}

        # 合并弹幕与礼物数据
        merged = []
        for d in danmu_results:
            minute_key = d['time'] // 60000
            merged.append({
                'time': d['time'],
                'count': d['count'],
                'gift_value': gift_map.get(minute_key, 0),
            })

        return JsonResponse(serialize_mongo(merged), safe=False)
    except Exception as e:
        return api_error(f'MongoDB 查询失败: {str(e)}', status=500)


# ============================================
# 接口 3：打赏排行榜 Top 10 (Redis 有序集合实时读取)
# ============================================
@api_login_required
def gift_rank(request, room_id: str) -> JsonResponse:
    """GET /api/charts/gift_rank/<room_id>/
    从 Redis 有序集合实时读取 Top 10 打赏排行（毫秒级响应）
    数据由 consumer.py 的 persist_gift 实时 ZINCRBY 维护
    """
    try:
        r = redis.Redis(
            host=settings.REDIS_CONFIG['host'],
            port=settings.REDIS_CONFIG['port'],
            db=settings.REDIS_CONFIG['db'],
            decode_responses=True,
            socket_connect_timeout=3,
        )

        rank_key = f'live_gift_rank:{room_id}'
        count_key = f'live_gift_count:{room_id}'

        # ZREVRANGE 按音浪总分倒序取 Top 10，WITHSCORES 返回分数
        scores = r.zrevrange(rank_key, 0, 9, withscores=True)

        results = []
        for username, total_yinlang in scores:
            gift_count = int(r.hget(count_key, username) or 0)
            results.append({
                'username': username,
                'total_yinlang': int(total_yinlang),
                'total_value_yuan': round(total_yinlang / 10, 2),
                'gift_count': gift_count,
            })

        r.close()
        return JsonResponse(results, safe=False)
    except redis.ConnectionError as e:
        return api_error(f'Redis 连接失败: {str(e)}', status=500)


# ============================================
# 接口 4：情感分析分布 (MongoDB 聚合)
# ============================================
@api_login_required
def sentiment(request, room_id: str) -> JsonResponse:
    """GET /api/charts/sentiment/<room_id>/
    取最近 500 条弹幕，计算正/中/负面分布比例
    """
    collection = _get_danmu_collection()

    pipeline = [
        {'$match': {'room_id': room_id}},
        {'$sort': {'_id': -1}},                     # 取最新文档
        {'$limit': 500},
        {'$group': {
            '_id': None,
            'total': {'$sum': 1},
            'positive': {'$sum': {'$cond': [{'$gt': ['$sentiment', 0.6]}, 1, 0]}},
            'neutral':  {'$sum': {'$cond': [
                {'$and': [
                    {'$gte': ['$sentiment', 0.4]},
                    {'$lte': ['$sentiment', 0.6]},
                ]}, 1, 0
            ]}},
            'negative': {'$sum': {'$cond': [{'$lt': ['$sentiment', 0.4]}, 1, 0]}},
        }},
        {'$project': {
            '_id': 0,
            'total': 1,
            'positive': 1,
            'neutral': 1,
            'negative': 1,
            'positive_pct': {'$round': [
                {'$cond': [
                    {'$gt': ['$total', 0]},
                    {'$multiply': [{'$divide': ['$positive', '$total']}, 100]},
                    0,
                ]}, 1
            ]},
            'neutral_pct': {'$round': [
                {'$cond': [
                    {'$gt': ['$total', 0]},
                    {'$multiply': [{'$divide': ['$neutral', '$total']}, 100]},
                    0,
                ]}, 1
            ]},
            'negative_pct': {'$round': [
                {'$cond': [
                    {'$gt': ['$total', 0]},
                    {'$multiply': [{'$divide': ['$negative', '$total']}, 100]},
                    0,
                ]}, 1
            ]},
        }},
    ]

    try:
        results = list(collection.aggregate(pipeline))
        if results:
            return JsonResponse(serialize_mongo(results[0]))
        else:
            return JsonResponse({
                'total': 0, 'positive': 0, 'neutral': 0, 'negative': 0,
                'positive_pct': 0, 'neutral_pct': 0, 'negative_pct': 0,
            })
    except Exception as e:
        return api_error(f'MongoDB 查询失败: {str(e)}', status=500)


# ============================================
# 接口 5：AI 流量预测 (ML 模型推理 + Redis 缓存)
# ============================================
@api_login_required
def predict(request, room_id: str) -> JsonResponse:
    """GET /api/predict/<room_id>/
    返回未来 5 分钟在线人数变化量预测值。
    内置三层柔性降级：模型缺失 / 数据空洞 / 推理异常均返回 HTTP 200 + 占位 JSON。
    正常结果缓存在 Redis 中 10 秒，避免高频轮询触发重复计算。
    """
    try:
        result = predict_next_5m(room_id)
        # predict_next_5m 永不返回 None，异常已内部降级为占位响应
    except Exception as e:
        # 极端兜底：万一 predict_next_5m 本身抛了未捕获异常
        # 记录日志但前端不受影响，返回统一占位 JSON
        result = {
            'predicted_delta': 0,
            'prediction_status': 'stable',
            'msg': f'数据积累中...',
            'is_placeholder': True,
            'cached': False,
            'features': {},
        }

    return JsonResponse({
        'room_id': room_id,
        'predicted_delta': result['predicted_delta'],
        'prediction_status': result['prediction_status'],
        'msg': result.get('msg', ''),
        'cached': result.get('cached', False),
        'features': result.get('features', {}),
    })
