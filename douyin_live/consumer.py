#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
抖音直播数据消费者 (Consumer) — 第三阶段核心脚本
职责: 从 Redis 三队列 BLPOP 取出数据 → NLP情感分析 → 双写 MySQL + MongoDB

架构:
  - 3个独立线程分别消费 danmu / gift / hot 队列
  - 主线程负责信号监听与优雅退出
  - 所有 DB 操作具备断线重连 + 指数退避机制

启动方式:
  python consumer.py --room-id 123456789
"""

import os
import sys
import json
import time
import signal
import threading
import argparse
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any

import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

import redis
from django.utils import timezone
from data_screen.models import LiveRoom, LiveSession
from mongo_client import get_mongo_client

# ============================================
# 雪花 NLP 导入（容错）
# ============================================
try:
    from snownlp import SnowNLP
    SNOWNLP_AVAILABLE = True
except ImportError:
    SNOWNLP_AVAILABLE = False
    print("[WARNING] SnowNLP 未安装，情感分析将返回中性值 0.5")

# ============================================
# 配置常量
# ============================================
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
}

QUEUE_DANMU = 'douyin_danmu_queue'
QUEUE_GIFT  = 'douyin_gift_queue'
QUEUE_HOT   = 'douyin_hot_queue'

# BLPOP 超时（秒），超时后检查退出标志
BLPOP_TIMEOUT = 5

# 热度数据批量写入阈值
HOT_BATCH_SIZE = 20
HOT_BATCH_INTERVAL = 30  # 秒

# 重连最大等待时间（秒）
MAX_RECONNECT_DELAY = 60

# ============================================
# ============================================
# 礼物映射表：图片哈希 ID → {name, diamond}
# diamond = 音浪值（10音浪=1元）
# 未知礼物默认 diamond=1，终端打印 [警告] 提示补充
# ============================================
GIFT_MAP: Dict[str, Dict[str, object]] = {
    # ── 已确认哈希的常见礼物（前缀匹配，取前15位特征码）──
    '7ef47758a435313': {'name': '小心心',       'diamond': 1},
    '5ddfcd51beaa7ca': {'name': '金色小心心',   'diamond': 1},
    '96e9bc9717d9267': {'name': '玫瑰',         'diamond': 1},
    'adf2ee6bf03d10d': {'name': '666',         'diamond': 1},
    'bfe9acf1a1f07d1': {'name': '520快乐',      'diamond': 1},
    'e9b77db267d0501': {'name': '人气票',       'diamond': 1},
    '722e56b42551d64': {'name': '粉丝团灯牌',   'diamond': 1},
    '9b9e3e1008d0579': {'name': '人气TOP1',     'diamond': 1},
    '4960c39f645d524': {'name': '你最好看',     'diamond': 2},
    '71801c53df3977b': {'name': '大啤酒',       'diamond': 2},
    '2756f07818a73a8': {'name': '棒棒糖',       'diamond': 9},
    '51dca3b621326b4': {'name': '520助力票',    'diamond': 9},
    'bdfcfd83a390974': {'name': '闪耀星辰',     'diamond': 9},
    'ffb9f9afca8fb9e': {'name': '星光闪耀',     'diamond': 9},
    'e9b7db267d0501b': {'name': '为你闪耀',     'diamond': 9},
    '42d4cd329e5c01b': {'name': '鲜花',         'diamond': 10},
    '30018ee1172fc69': {'name': '上车票',       'diamond': 10},
    'c169b7ff42cb389': {'name': '加油鸭',       'diamond': 15},
    '7fa9f120024d2df': {'name': '赢麻了',       'diamond': 19},
    '803b1d3dfe66b89': {'name': '爱你哟',       'diamond': 52},
    'eee04e798ad7f08': {'name': '墨镜',         'diamond': 99},
    '9a953f4898342c8': {'name': '比心兔兔',     'diamond': 299},
    'd4006cb190c47ae': {'name': '金色小心心2',  'diamond': 1}, 
    '9a515c9e4e0a264': {'name': '暮光星辰',     'diamond': 99},
    'b591475c6de2d20': {'name': '甜心糖果',     'diamond': 10},
    '942569391c38563': {'name': '告白气球',     'diamond': 99},
    'a44104d2d61cc18': {'name': '跑车',         'diamond':1200},
    '0e176c2d0ac040a': {'name': '热气球',       'diamond':520},
    'a7fd71b617f0770': {'name': '私人飞机',     'diamond':3000},
    'f70d3bdfaf62446': {'name': '单身但快乐',   'diamond': 399},
    '81579f16ce3fe3e': {'name': '小恶魔',       'diamond': 99},
    '9ecb32b0e3b0640': {'name': '抖音一号',     'diamond': 10001},
    '3338b8a583a2878': {'name': '海上升明月',     'diamond': 4166},
    'd9473afea9acbe1': {'name': '宝象醒世',     'diamond': 10999},
    'eeca4b0dbe9716b': {'name': '520 限定・抖音一号',     'diamond': 10001},
    '906a6c6371474ea': {'name': '无尽宝藏',     'diamond': 19999},
    '611643a940354b9': {'name': '520 限定跑车',     'diamond': 5200},
    'd65ecd2b2283cc14': {'name': '浪漫飞驰',     'diamond': 1200},
    '8d6b0009f96f32b': {'name': '荣誉之匙',     'diamond': 388 },
    '09a65effcc63d62': {'name': 'PK 宝箱',     'diamond': 600 },
    '906a6c6371474ea': {'name': '无尽浪漫',     'diamond': 19999},
    'a4f6324dd856045': {'name': '最亮灯泡',     'diamond': 1},

    

}


# ============================================
# 全局状态控制
# ============================================
shutdown_flag = threading.Event()

# 线程安全的批量写缓冲
hot_batch_lock = threading.Lock()
hot_batch_buffer: list = []

# 动态路由池：room_id → DataPersister，支持任意数量直播间并发
persister_pool: Dict[str, 'DataPersister'] = {}
pool_lock = threading.Lock()

# 礼物去重缓存：防止聊天区 + 特效区多 DOM 区域重复上报
# key = f"{username}_{gift_image_id}_{count}", value = 最后上报时间戳
gift_dedup_cache: Dict[str, float] = {}
gift_dedup_lock = threading.Lock()
DEDUP_WINDOW = 3  # 滑动窗口秒数：同一指纹 3 秒内只计一次


def get_persister(room_id: str) -> 'DataPersister':
    """线程安全地从池中获取或创建对应房间的 DataPersister"""
    if room_id in persister_pool:
        return persister_pool[room_id]
    with pool_lock:
        if room_id in persister_pool:
            return persister_pool[room_id]
        print(f"[POOL] 动态创建 DataPersister: room_id={room_id}")
        persister_pool[room_id] = DataPersister(room_id)
        return persister_pool[room_id]


# ============================================
# 工具函数
# ============================================
def get_sentiment(text: str) -> float:
    """使用 SnowNLP 计算中文情感极性，返回 0~1（0=负面, 1=正面）"""
    if not SNOWNLP_AVAILABLE or not text or not text.strip():
        return 0.5
    try:
        s = SnowNLP(text.strip())
        return round(s.sentiments, 4)
    except Exception:
        return 0.5


def is_duplicate_gift(username: str, gift_image_id: str, count: int) -> bool:
    """
    滑动时间窗口去重：同一用户 + 同一礼物图片 + 同一连击数，DEDUP_WINDOW 秒内只计一次。
    防止聊天区轮询和特效区 MutationObserver 重复上报同一笔送礼。
    返回 True 表示重复，应丢弃。
    """
    fingerprint = f"{username}_{gift_image_id}_{count}"
    now = time.time()
    with gift_dedup_lock:
        last_seen = gift_dedup_cache.get(fingerprint)
        if last_seen is not None and (now - last_seen) < DEDUP_WINDOW:
            return True
        gift_dedup_cache[fingerprint] = now

        # 定期清理：缓存超过 1000 条时，移除 60 秒未出现的过期条目
        if len(gift_dedup_cache) > 1000:
            stale = [k for k, v in gift_dedup_cache.items() if now - v > 60]
            for k in stale:
                del gift_dedup_cache[k]
    return False


def _record_unknown_gift(fingerprint: str):
    """
    将未识别的礼物特征码自动收录到 unknown_gifts.json。
    fingerprint = gift_image_id 的前15位字符。
    """
    import os as _os
    json_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'unknown_gifts.json')

    data = {}
    if _os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            pass

    now_ts = int(time.time())
    if fingerprint in data:
        data[fingerprint]['count'] += 1
        data[fingerprint]['last_seen'] = now_ts
    else:
        data[fingerprint] = {
            'count': 1,
            'first_seen': now_ts,
            'last_seen': now_ts,
        }

    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            sorted_data = dict(
                sorted(data.items(), key=lambda item: item[1]['count'], reverse=True)
            )
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] 无法写入 unknown_gifts.json: {e}")


def lookup_gift(gift_image_id: str):
    """
    根据图片哈希 ID 查找礼物信息（前缀包容匹配 + 自动收录）。

    匹配策略:
      1. 精确匹配 GIFT_MAP key
      2. 遍历字典，gift_image_id.startswith(key) 即视为命中
      3. 仍未命中 → 自动收录前15位特征码到 unknown_gifts.json

    返回 (name: str, diamond: int)
    """
    # 1. 精确匹配
    entry = GIFT_MAP.get(gift_image_id)
    if entry:
        return entry['name'], entry['diamond']

    # 2. 前缀包容匹配：双方各取前15位特征码对比
    #    无论 key 是 33 位全哈希还是 15 位特征码，均能正确命中
    for key, entry in GIFT_MAP.items():
        prefix_len = min(len(key), 15)
        if len(gift_image_id) >= prefix_len and gift_image_id[:prefix_len] == key[:prefix_len]:
            print(f"[FUZZY] 🎯 前缀包容匹配: incoming={gift_image_id[:24]}...")
            print(f"[FUZZY]                  stored={key[:24] if len(key)>=24 else key}...")
            print(f"[FUZZY]    识别为 '{entry['name']}', diamond={entry['diamond']}")
            return entry['name'], entry['diamond']

    # 3. 完全未知 — 自动收录 + 警告
    fingerprint = gift_image_id[:15]
    _record_unknown_gift(fingerprint)

    unknown_name = f'未知礼物_{gift_image_id[:6]}'
    print(f"[WARN] ⚠️  未收录礼物图片 ID: {gift_image_id}")
    print(f"[WARN]    前15位特征码: {fingerprint} 已自动录入 unknown_gifts.json")
    print(f"[WARN]    已使用占位名 '{unknown_name}', diamond=99 (估值 9.9 元)")
    return unknown_name, 99


def generate_session_id(room_id: str) -> str:
    now = datetime.now()
    return f"{room_id}_{now.strftime('%Y%m%d%H%M%S')}"


def ensure_live_room(room_id: str, host_name: Optional[str] = None) -> LiveRoom:
    """
    查找或创建直播间记录。
    支持自动纠正：当数据库中是占位名（'直播间XXXX'）而 spider 传来真实主播名时，自动更新。
    """
    room, created = LiveRoom.objects.get_or_create(
        room_id=room_id,
        defaults={
            'host_name': host_name if host_name else f'直播间{room_id}',
        }
    )
    if not created:
        needs_save = False
        # 如果数据库里是占位名称而传入了真实主播名，自动纠正
        if host_name and room.host_name.startswith('直播间'):
            room.host_name = host_name
            needs_save = True
            print(f"[SYNC] 主播名称已纠正: '直播间{room_id}' → '{host_name}'")
        if needs_save:
            room.save(update_fields=['host_name', 'updated_at'])
    return room


def ensure_active_session(room: LiveRoom) -> LiveSession:
    """查找该房间当前进行中的场次，没有则创建新场次"""
    active_session = LiveSession.objects.filter(
        room=room,
        end_time__isnull=True
    ).order_by('-start_time').first()

    if active_session:
        return active_session

    # 创建新场次
    session_id = generate_session_id(room.room_id)
    session = LiveSession.objects.create(
        session_id=session_id,
        room=room,
        start_time=timezone.now(),
        peak_online=0,
        total_danmu=0,
    )
    return session


# ============================================
# Redis 连接管理（带重连机制）
# ============================================
def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_CONFIG['host'],
        port=REDIS_CONFIG['port'],
        db=REDIS_CONFIG['db'],
        decode_responses=False,
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )


def reconnect_redis(attempt: int) -> Optional[redis.Redis]:
    """指数退避重连，返回 None 表示放弃"""
    delay = min(2 ** attempt, MAX_RECONNECT_DELAY)
    print(f"[RECONNECT] 第 {attempt} 次重试，等待 {delay}s...")
    time.sleep(delay)
    try:
        client = create_redis_client()
        client.ping()
        print("[RECONNECT] Redis 重连成功")
        return client
    except Exception:
        return None


# ============================================
# 数据清洗模块
# ============================================
def clean_danmu_data(raw: dict) -> Optional[dict]:
    """清洗弹幕数据，过滤无效内容"""
    nickname = (raw.get('nickname') or '').strip()
    content = (raw.get('content') or '').strip()

    if not nickname or not content:
        return None
    if len(content) > 200:
        content = content[:200]

    return {
        'room_id': raw.get('room_id', ''),        # JS _enrich 注入的房间号（路由必需）
        'nickname': nickname,
        'content': content,
        'host_name': raw.get('host_name', ''),     # spider 注入的主播名
        'timestamp': raw.get('timestamp', int(time.time() * 1000)),
    }


def clean_gift_data(raw: dict) -> Optional[dict]:
    """
    清洗礼物数据 — v3.0 基于 gift_image_id 哈希识别。
    spider 不再传 gift_name，改传 gift_image_id。
    """
    username = (raw.get('username') or '').strip()
    gift_image_id = (raw.get('gift_image_id') or '').strip()

    if not username or not gift_image_id:
        if gift_image_id:
            print(f"[GIFT-DROP] 缺少用户名, gift_image_id={gift_image_id[:16]}")
        return None

    # 哈希长度校验（放宽至 12 位，兼容不同 CDN 格式）
    if len(gift_image_id) < 12:
        print(f"[GIFT-DROP] 图片ID过短(<12): id='{gift_image_id}', username='{username}'")
        return None

    # 仅允许十六进制字符
    if not all(c in '0123456789abcdef' for c in gift_image_id.lower()):
        print(f"[GIFT-DROP] 图片ID含非十六进制字符: id='{gift_image_id[:16]}...', username='{username}'")
        return None

    return {
        'room_id': raw.get('room_id', ''),
        'username': username,
        'gift_image_id': gift_image_id,
        'count': int(raw.get('count', 1)),
        'host_name': raw.get('host_name', ''),
        'timestamp': raw.get('timestamp', int(time.time() * 1000)),
    }


def clean_hot_data(raw: dict) -> Optional[dict]:
    """清洗热度数据"""
    online = raw.get('online_count')
    likes = raw.get('like_count')

    if likes is not None and likes < 0:
        return None

    result = {
        'room_id': raw.get('room_id', ''),
        'like_count': int(likes) if likes is not None else 0,
        'host_name': raw.get('host_name', ''),
        'timestamp': raw.get('timestamp', int(time.time() * 1000)),
    }

    # 防御：仅 online_count 为正数时才纳入，否则保持 null
    if online is not None and isinstance(online, (int, float)) and online > 0:
        result['online_count'] = int(online)
    else:
        result['online_count'] = None

    return result


# ============================================
# 数据持久化模块
# ============================================
class DataPersister:
    """
    负责双写 MongoDB（日志流水）和 MySQL（业务统计）
    所有写入操作具备独立的 try/except 防护，互不干扰
    """

    def __init__(self, room_id: str):
        self.room_id = room_id
        self.mongo = get_mongo_client()
        self._room: Optional[LiveRoom] = None
        self._session: Optional[LiveSession] = None
        self._last_session_check = 0
        # 排行榜专用 Redis 连接（与队列消费者独立，避免 blpop 阻塞冲突）
        self.redis_rank = create_redis_client()
        self._latest_host_name: Optional[str] = None  # spider 最新传来的主播名
        self._hot_buffer: list = []                     # per-room 热度批量缓冲

    def _sync_host_name(self, host_name: str):
        """收到 spider 传来的真实主播名时，确保 MySQL 记录随之更新"""
        if host_name and host_name.strip() and host_name != self._latest_host_name:
            self._latest_host_name = host_name.strip()
            self._room = ensure_live_room(self.room_id, self._latest_host_name)

    @property
    def room(self) -> LiveRoom:
        """懒加载 + 定期刷新直播间对象"""
        now = time.time()
        if self._room is None or now - self._last_session_check > 60:
            self._room = ensure_live_room(self.room_id, self._latest_host_name)
            self._last_session_check = now
        return self._room

    @property
    def session(self) -> LiveSession:
        """获取当前活跃场次"""
        if self._session is None or self._session.end_time is not None:
            self._session = ensure_active_session(self.room)
        return self._session

    # ---------- 弹幕持久化 ----------
    def persist_danmu(self, data: dict):
        nickname = data['nickname']
        content = data['content']
        sentiment = get_sentiment(content)

        # 0. 同步主播名（spider 注入的 host_name 自动纠正 MySQL 占位名）
        host_name = data.get('host_name', '')
        if host_name:
            self._sync_host_name(host_name)

        # 1. 写入 MongoDB 弹幕日志
        try:
            mongo_doc = {
                'room_id': self.room_id,
                'host_name': host_name,
                'nickname': nickname,
                'content': content,
                'sentiment': sentiment,
                'timestamp': data['timestamp'],
            }
            self.mongo.insert_danmu(mongo_doc)
        except Exception as e:
            print(f"[ERROR] MongoDB 弹幕写入失败: {e}")

        # 2. 更新 MySQL 统计
        try:
            session = self.session
            session.total_danmu = int(session.total_danmu) + 1
            session.save(update_fields=['total_danmu'])
        except Exception as e:
            print(f"[ERROR] MySQL 弹幕统计更新失败: {e}")

        # 控制台回显：情感极性
        emoji = '😊' if sentiment > 0.6 else ('😞' if sentiment < 0.4 else '😐')
        print(f"[DANMU] 👤 {nickname} | 💬 {content} | 情感 {emoji}({sentiment:.2f})")

    # ---------- 礼物持久化 v3.0 ----------
    def persist_gift(self, data: dict):
        username = data['username']
        gift_image_id = data['gift_image_id']
        count = data['count']

        # 滑动窗口去重：防止聊天区 + 特效区双重上报
        if is_duplicate_gift(username, gift_image_id, count):
            return

        # 通过图片哈希 ID 查找真实礼物名与音浪单价
        gift_name, diamond = lookup_gift(gift_image_id)

        total_yinlang = diamond * count
        # 换算为元: 10音浪 = 1元
        total_value_yuan = Decimal(str(total_yinlang)) / Decimal('10')

        # 0. 同步主播名
        host_name = data.get('host_name', '')
        if host_name:
            self._sync_host_name(host_name)

        # 1. 写入 MongoDB 礼物日志
        try:
            mongo_doc = {
                'room_id': self.room_id,
                'host_name': host_name,
                'username': username,
                'gift_name': gift_name,
                'gift_image_id': gift_image_id,
                'count': count,
                'diamond': diamond,
                'value_yinlang': total_yinlang,
                'timestamp': data['timestamp'],
            }
            self.mongo.insert_gift(mongo_doc)
        except Exception as e:
            print(f"[ERROR] MongoDB 礼物写入失败: {e}")

        # 2. 更新 MySQL 直播间累计礼物价值
        try:
            room = self.room
            current_value = Decimal(str(room.total_gifts_value))
            room.total_gifts_value = current_value + total_value_yuan
            room.save(update_fields=['total_gifts_value', 'updated_at'])
        except Exception as e:
            print(f"[ERROR] MySQL 礼物统计更新失败: {e}")

        # 3. 更新 Redis 实时排行榜（有序集合 + 哈希计数，pipeline 原子写入）
        try:
            rank_key = f'live_gift_rank:{self.room_id}'
            count_key = f'live_gift_count:{self.room_id}'
            pipe = self.redis_rank.pipeline()
            pipe.zincrby(rank_key, total_yinlang, username)
            pipe.hincrby(count_key, username, count)
            pipe.expire(rank_key, 21600)   # 6 小时 TTL，每次写入刷新
            pipe.expire(count_key, 21600)
            pipe.execute()
        except Exception as e:
            print(f"[ERROR] Redis 排行榜更新失败: {e}")

        print(f"[GIFT] 🎁 {username} 送出 {gift_name} x{count} | " +
              f"diamond={diamond} | 估值 ¥{total_value_yuan:.2f}")

    # ---------- 热度 MySQL 实时更新 ----------
    def persist_hot_mysql(self, data: dict):
        """实时更新 MySQL：在线人数 + 点赞"""
        online_count = data.get('online_count')
        like_count = data.get('like_count', 0)

        host_name = data.get('host_name', '')
        if host_name:
            self._sync_host_name(host_name)

        online_valid = online_count is not None and online_count > 0

        try:
            room = self.room
            if online_valid:
                room.current_online = online_count
            if like_count > room.total_likes:
                room.total_likes = like_count
            update_fields = ['total_likes', 'updated_at']
            if online_valid:
                update_fields.append('current_online')
            room.save(update_fields=update_fields)
        except Exception as e:
            print(f"[ERROR] MySQL 热度统计更新失败: {e}")

        if online_valid:
            try:
                session = self.session
                if online_count > session.peak_online:
                    session.peak_online = online_count
                    session.save(update_fields=['peak_online'])
            except Exception:
                pass

    # ---------- 批量热度写入 ----------
    def persist_hot_batch(self, batch: list):
        """批量写入 MongoDB 热度快照，减少网络往返"""
        if not batch:
            return
        try:
            docs = []
            for data in batch:
                docs.append({
                    'room_id': self.room_id,
                    'host_name': data.get('host_name', ''),
                    'online_count': data.get('online_count'),
                    'like_count': data.get('like_count', 0),
                    'timestamp': data['timestamp'],
                })
            self.mongo.bulk_insert_hot_snapshots(docs)
        except Exception as e:
            print(f"[ERROR] MongoDB 批量热度写入失败: {e}")
            for data in batch:
                try:
                    self.mongo.insert_hot_snapshot({
                        'room_id': self.room_id,
                        'host_name': data.get('host_name', ''),
                        'online_count': data.get('online_count'),
                        'like_count': data.get('like_count', 0),
                        'timestamp': data['timestamp'],
                    })
                except Exception:
                    pass


# ============================================
# 队列消费者线程
# ============================================
class QueueConsumer(threading.Thread):
    """
    通用队列消费者：从指定 Redis 队列 blpop，清洗后交给回调处理
    """

    def __init__(self, queue_name: str, processor, cleaner, label: str):
        super().__init__(daemon=True)
        self.queue_name = queue_name
        self.processor = processor
        self.cleaner = cleaner
        self.label = label
        self.redis_client: Optional[redis.Redis] = None

    def run(self):
        print(f"[CONSUMER] {self.label} 线程启动，监听队列: {self.queue_name}")
        self.redis_client = create_redis_client()
        reconnect_attempt = 0

        while not shutdown_flag.is_set():
            try:
                result = self.redis_client.blpop(
                    self.queue_name, timeout=BLPOP_TIMEOUT
                )
                reconnect_attempt = 0  # 成功消费，重置重连计数

                if result is None:
                    continue  # 超时，无数据

                _, raw_message = result
                data = json.loads(raw_message.decode('utf-8'))

                cleaned = self.cleaner(data)
                if cleaned is None:
                    continue

                self.processor(cleaned)

            except (redis.ConnectionError, redis.TimeoutError, ConnectionResetError,
                    BrokenPipeError, OSError) as e:
                print(f"[{self.label}] Redis 连接断开: {e}")
                reconnect_attempt += 1
                new_client = reconnect_redis(reconnect_attempt)
                if new_client:
                    self.redis_client = new_client
                else:
                    print(f"[{self.label}] 重连失败次数过多，退出线程")
                    break

            except json.JSONDecodeError as e:
                print(f"[{self.label}] JSON 解析异常: {e}")

            except Exception as e:
                print(f"[{self.label}] 未预期的消费异常: {type(e).__name__}: {e}")
                time.sleep(1)  # 防止未知错误导致的疯狂重试

        # 退出前清理
        try:
            self.redis_client.close()
        except Exception:
            pass
        print(f"[CONSUMER] {self.label} 线程已安全退出")


# ============================================
# 热度批量刷新守护线程
# ============================================
class HotBatchFlusher(threading.Thread):
    """
    定时刷新所有房间热度批量缓冲区的守护线程
    遍历 persister_pool，将每个房间的积压热度数据批量写入 MongoDB
    """

    def __init__(self):
        super().__init__(daemon=True)

    def run(self):
        print("[FLUSHER] 多房间热度批量刷新线程启动")
        while not shutdown_flag.is_set():
            shutdown_flag.wait(timeout=HOT_BATCH_INTERVAL)
            with pool_lock:
                for persister in list(persister_pool.values()):
                    if persister._hot_buffer:
                        batch = persister._hot_buffer
                        persister._hot_buffer = []
                        persister.persist_hot_batch(batch)


# ============================================
# 信号处理器
# ============================================
def setup_signal_handlers():
    def handle_signal(signum, frame):
        print(f"\n[SHUTDOWN] 收到信号 {signum}，准备安全退出...")
        shutdown_flag.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


# ============================================
# 主入口
# ============================================
def main():
    parser = argparse.ArgumentParser(description='抖音直播数据消费者 — 多房间动态路由版 v2.0')
    parser.add_argument(
        '--room-id', type=str, required=False, default=None,
        help='（可选）预绑定单个直播间 ID。不传则由 spider 数据中的 room_id 动态路由'
    )
    args = parser.parse_args()

    setup_signal_handlers()

    # 前置检查：Redis 连通性
    try:
        test_client = create_redis_client()
        test_client.ping()
        test_client.close()
        print("[INIT] Redis 连接正常")
    except Exception as e:
        print(f"[FATAL] Redis 无法连接: {e}")
        sys.exit(1)

    # 如果命令行指定了 room_id，预建对应 persister
    if args.room_id:
        with pool_lock:
            persister_pool[args.room_id] = DataPersister(args.room_id)
        init_persister = persister_pool[args.room_id]
        init_room = init_persister.room
        init_session = init_persister.session
        print(f"[INIT] 预绑定: room_id={args.room_id} | {init_room.host_name} | 场次: {init_session.session_id}")

    print("=" * 60)
    print("     抖音直播数据消费者 v2.0 (多房间矩阵)")
    print(f"     弹幕队列: {QUEUE_DANMU}")
    print(f"     礼物队列: {QUEUE_GIFT}")
    print(f"     热度队列: {QUEUE_HOT}")
    print(f"     路由模式: {'预绑定 ' + args.room_id if args.room_id else '动态分发'}")
    print("=" * 60)

    # 启动多房间热度批量刷新守护线程
    flusher = HotBatchFlusher()
    flusher.start()

    # ---------- 动态路由处理器 ----------
    def danmu_router(data: dict):
        """根据 data['room_id'] 路由到对应房间的 persist_danmu"""
        room_id = data.get('room_id', '')
        if not room_id:
            return
        persister = get_persister(room_id)
        persister.persist_danmu(data)

    def gift_router(data: dict):
        """根据 data['room_id'] 路由到对应房间的 persist_gift"""
        room_id = data.get('room_id', '')
        if not room_id:
            return
        persister = get_persister(room_id)
        persister.persist_gift(data)

    def hot_router(data: dict):
        """热度数据：MySQL 实时更新 + per-room MongoDB 批量缓冲"""
        room_id = data.get('room_id', '')
        if not room_id:
            return
        persister = get_persister(room_id)
        # MySQL 实时更新
        persister.persist_hot_mysql(data)
        # Per-room 批量缓冲（MongoDB）
        persister._hot_buffer.append(data)
        if len(persister._hot_buffer) >= HOT_BATCH_SIZE:
            batch = persister._hot_buffer
            persister._hot_buffer = []
            t = threading.Thread(
                target=persister.persist_hot_batch,
                args=(batch,),
                daemon=True
            )
            t.start()

    # 启动三个消费者线程（回调改为路由器）
    consumers = [
        QueueConsumer(
            queue_name=QUEUE_DANMU,
            processor=danmu_router,
            cleaner=clean_danmu_data,
            label='DANMU',
        ),
        QueueConsumer(
            queue_name=QUEUE_GIFT,
            processor=gift_router,
            cleaner=clean_gift_data,
            label='GIFT',
        ),
        QueueConsumer(
            queue_name=QUEUE_HOT,
            processor=hot_router,
            cleaner=clean_hot_data,
            label='HOT',
        ),
    ]

    for c in consumers:
        c.start()

    # 主线程等待退出信号
    try:
        while not shutdown_flag.is_set():
            shutdown_flag.wait(timeout=1)
    except KeyboardInterrupt:
        pass

    print("[SHUTDOWN] 正在等待所有消费者线程退出...")

    # 刷新所有房间的热度缓冲区
    with pool_lock:
        for rid, persister in persister_pool.items():
            if persister._hot_buffer:
                print(f"[SHUTDOWN] 刷新 room_id={rid} 热度 {len(persister._hot_buffer)} 条")
                persister.persist_hot_batch(persister._hot_buffer)
                persister._hot_buffer = []

    # 等待线程结束
    for c in consumers:
        c.join(timeout=10)

    # 标记所有活跃场次结束
    with pool_lock:
        for rid, persister in persister_pool.items():
            try:
                session = persister.session
                if session.end_time is None:
                    session.end_time = timezone.now()
                    session.save(update_fields=['end_time'])
                    print(f"[SHUTDOWN] 场次 {session.session_id} (room={rid}) 已结束")
            except Exception as e:
                print(f"[SHUTDOWN] 场次结束标记失败 room={rid}: {e}")

    print(f"[SHUTDOWN] 消费者已完全退出，共服务 {len(persister_pool)} 个直播间")


if __name__ == '__main__':
    main()
