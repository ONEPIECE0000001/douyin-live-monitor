#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 验收探针脚本 (只读，无副作用)
用途：快速验证 MongoDB 中海量日志的入库情况与 NLP 情感数据质量
"""

import sys

from mongo_client import get_mongo_client


def main():
    print("=" * 60)
    print("       MongoDB 数据验收探针")
    print("=" * 60)

    # 连接
    try:
        mongo = get_mongo_client()
        print("[OK] MongoDB 连接成功\n")
    except Exception as e:
        print(f"[FAIL] MongoDB 连接失败: {e}")
        sys.exit(1)

    # 获取三个集合
    danmu_col = mongo.get_danmu_collection()
    gift_col  = mongo.get_gift_collection()
    hot_col   = mongo.get_hot_trend_collection()

    # ---- 1. 文档总数统计 ----
    danmu_count = danmu_col.count_documents({})
    gift_count  = gift_col.count_documents({})
    hot_count   = hot_col.count_documents({})

    print("【集合文档统计】")
    print(f"  📝 danmu_log     : {danmu_count:>8,} 条")
    print(f"  🎁 gift_log      : {gift_count:>8,} 条")
    print(f"  🔥 hot_trend     : {hot_count:>8,} 条")
    print(f"  ─────────────────────────────")
    print(f"  📊 总计           : {danmu_count + gift_count + hot_count:>8,} 条")
    print()

    # ---- 2. 按直播间分组统计 (最近活跃的) ----
    print("【按直播间分组 (Top 5)】")
    pipeline = [
        {"$group": {"_id": "$room_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5},
    ]
    for item in danmu_col.aggregate(pipeline):
        print(f"  room_id={item['_id']} : {item['count']:,} 条弹幕")

    if danmu_count == 0:
        print("  (暂无弹幕数据)")
    print()

    # ---- 3. 随机抽取最新弹幕 × 5，验证情感分析质量 ----
    print("【弹幕情感分析抽样 (最新 5 条)】")

    latest_danmu = list(danmu_col.find().sort("_id", -1).limit(5))

    if not latest_danmu:
        print("  ⚠️  弹幕集合为空，请确认 spider 和 consumer 正在运行")
    else:
        for i, doc in enumerate(latest_danmu, 1):
            nickname   = doc.get('nickname', '?')
            content    = doc.get('content', '?')
            sentiment  = doc.get('sentiment', None)
            room_id    = doc.get('room_id', '?')

            # 情感打分可视化
            if sentiment is not None:
                if sentiment > 0.6:
                    emoji = '😊'
                    label = '正面'
                elif sentiment < 0.4:
                    emoji = '😞'
                    label = '负面'
                else:
                    emoji = '😐'
                    label = '中性'
                score_str = f"{emoji} {label} ({sentiment:.4f})"
            else:
                score_str = "❌ 缺失 (SnowNLP 未生效)"

            print(f"\n  [{i}] room={room_id} | 👤 {nickname}")
            print(f"      内容: {content}")
            print(f"      情感: {score_str}")

    print()
    print("=" * 60)
    print("  验收完成 — 数据链路正常 🤖")
    print("=" * 60)


if __name__ == '__main__':
    main()
