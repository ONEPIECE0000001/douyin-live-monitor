"""
train_model.py — 抖音直播流量预测 模型训练脚本 (v3.0)
========================================================
读取 build_dataset.py (v3.0) 生成的含场次切割 CSV，
训练 RandomForestRegressor 预测在线人数变化量（Delta），
并分层评估（全量 + 活跃子集）。

v3.0 变更：
  - 支持命令行位置参数指定数据集文件，无需 --data 标志
  - 无参数时自动降级读取当前目录 training_data.csv

用法：
    python train_model.py [数据集.csv] [--model traffic_rf_model.pkl]
    python train_model.py test_gus_data.csv
    python train_model.py                     # 默认 training_data.csv
"""

import argparse
import os
import sys

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

# ============================================================
# 0. 配置常量
# ============================================================

RANDOM_STATE = 42
TEST_SIZE = 0.2

RF_PARAMS = {
    'n_estimators': 200,
    'max_depth': 10,
    'random_state': RANDOM_STATE,
    'n_jobs': -1,
    'min_samples_leaf': 3,
}

# v2.0: 纯互动特征，不含 online_count_avg
FEATURE_COLS = [
    'danmu_total_rolling',
    'danmu_positive_rolling',
    'danmu_negative_rolling',
    'danmu_avg_sentiment_rolling',
    'gift_value_rolling',
    'gift_count_rolling',
]

# v2.0: 目标为变化量 Delta
TARGET_COL = 'target_delta_5m'


# ============================================================
# 1. 数据加载与预处理
# ============================================================

def load_and_split(data_path: str) -> tuple:
    if not os.path.exists(data_path):
        print(f"[ERROR] 数据文件不存在: {data_path}")
        sys.exit(1)

    df = pd.read_csv(data_path, index_col=0, parse_dates=True)
    print(f"[INFO] 读取数据: {len(df)} 行 x {len(df.columns)} 列")

    # 校验必要列存在
    missing_features = [c for c in FEATURE_COLS if c not in df.columns]
    if missing_features:
        print(f"[ERROR] 缺少特征列: {missing_features}")
        sys.exit(1)
    if TARGET_COL not in df.columns:
        print(f"[ERROR] 缺少目标列: {TARGET_COL}")
        sys.exit(1)

    X = df[FEATURE_COLS].values
    y = df[TARGET_COL].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    # 输出目标变量的分布特征，帮助理解 Delta 任务
    print(f"[INFO] 目标变量 target_delta_5m 分布:")
    print(f"        均值: {y.mean():+.2f}  中位数: {np.median(y):+.2f}")
    print(f"        标准差: {y.std():.2f}  范围: [{y.min():+.0f}, {y.max():+.0f}]")
    print(f"        正值占比: {(y > 0).sum()/len(y)*100:.1f}%  (上涨分钟)")
    print(f"        零值占比: {(y == 0).sum()/len(y)*100:.1f}%  (不变分钟)")
    print(f"        负值占比: {(y < 0).sum()/len(y)*100:.1f}%  (下跌分钟)")
    print(f"[INFO] 训练集: {len(X_train)} 样本, 测试集: {len(X_test)} 样本")
    return X_train, X_test, y_train, y_test


# ============================================================
# 2. 模型训练
# ============================================================

def train_model(X_train: np.ndarray, y_train: np.ndarray) -> RandomForestRegressor:
    model = RandomForestRegressor(**RF_PARAMS)
    model.fit(X_train, y_train)
    print(f"[INFO] RandomForest 训练完成 "
          f"(n_estimators={RF_PARAMS['n_estimators']}, "
          f"max_depth={RF_PARAMS['max_depth']})")
    return model


# ============================================================
# 3. 分层评估
# ============================================================

def evaluate_model(model: RandomForestRegressor,
                   X_train: np.ndarray, y_train: np.ndarray,
                   X_test: np.ndarray, y_test: np.ndarray):
    """
    三层评估 + Delta 专用分析：
      1. 训练集
      2. 全量测试集
      3. 活跃样本子集（danmu > 0 或 gift > 0）
      4. 方向准确率：预测涨跌方向是否正确
    """

    def compute_metrics(y_true, y_pred, tag: str) -> dict:
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)
        print(f"\n  [{tag}]")
        print(f"    MAE  : {mae:.2f}   (预测变化量的平均绝对偏差，单位：人)")
        print(f"    RMSE : {rmse:.2f}  (对大偏差更敏感的均方根误差)")
        print(f"    R^2  : {r2:.4f}  (1=完美, 0=猜均值, <0=不如猜均值)")
        return {'mae': mae, 'rmse': rmse, 'r2': r2}

    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)

    print(f"\n{'='*60}")
    print(f"  模型评估报告 — 预测目标: 在线人数变化量 (Delta)")
    print(f"{'='*60}")

    # 3.1 训练集
    train_metrics = compute_metrics(y_train, y_train_pred, '训练集')

    # 3.2 全量测试集
    test_metrics = compute_metrics(y_test, y_test_pred, '全量测试集')

    # 3.3 活跃样本子集
    danmu_idx = FEATURE_COLS.index('danmu_total_rolling')
    gift_val_idx = FEATURE_COLS.index('gift_value_rolling')
    active_mask = (X_test[:, danmu_idx] > 0) | (X_test[:, gift_val_idx] > 0)
    active_count = active_mask.sum()
    total_count = len(X_test)

    if active_count > 0:
        X_active = X_test[active_mask]
        y_active_true = y_test[active_mask]
        y_active_pred = model.predict(X_active)

        print(f"\n  -- 活跃样本子集 (弹幕>0 或 礼物>0) --")
        print(f"  样本数: {active_count} / {total_count} ({active_count/total_count*100:.1f}%)")
        active_metrics = compute_metrics(y_active_true, y_active_pred, '活跃子集')

        mae_gap = active_metrics['mae'] - test_metrics['mae']
        rmse_gap = active_metrics['rmse'] - test_metrics['rmse']
        print(f"\n  -- 活跃 vs 全量 偏差对比 --")
        print(f"    MAE 差值  : {mae_gap:+.2f}  "
              f"({'活跃时段预测更难' if mae_gap > 0 else '活跃时段预测更易'})")
        print(f"    RMSE 差值 : {rmse_gap:+.2f}  "
              f"({'活跃时段预测更难' if rmse_gap > 0 else '活跃时段预测更易'})")
    else:
        print(f"\n  [WARN] 测试集中无活跃样本")

    # 3.4 方向准确率：预测涨/跌/平的符号是否与真实一致
    print(f"\n  -- 方向准确率 (涨跌方向预测) --")
    y_test_sign = np.sign(y_test)
    y_pred_sign = np.sign(y_test_pred)
    direction_correct = (y_test_sign == y_pred_sign).sum()
    direction_acc = direction_correct / len(y_test) * 100
    print(f"    正确预测涨跌方向: {direction_correct} / {len(y_test)} ({direction_acc:.1f}%)")

    # 分别统计涨、跌、平的方向召回率
    for label, condition in [('上涨', y_test > 0), ('下跌', y_test < 0), ('持平', y_test == 0)]:
        if condition.sum() > 0:
            sub_acc = (y_test_sign[condition] == y_pred_sign[condition]).sum() / condition.sum() * 100
            print(f"    {label}方向召回率: {sub_acc:.1f}% ({condition.sum()} 个样本)")


# ============================================================
# 4. 特征重要性
# ============================================================

def print_feature_importance(model: RandomForestRegressor):
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]

    print(f"\n{'='*60}")
    print(f"  特征重要性排名 (纯互动特征，无 online_count 捷径)")
    print(f"{'='*60}")
    print(f"  {'排名':<6}{'特征名':<35}{'重要度':<12}{'占比'}")
    print(f"  {'-'*60}")

    total = importances.sum()
    for rank, idx in enumerate(indices, 1):
        pct = importances[idx] / total * 100
        bar = '#' * int(pct / 2)
        print(f"  {rank:<6}{FEATURE_COLS[idx]:<35}{importances[idx]:<12.4f}{pct:>5.1f}%  {bar}")

    top_n = min(3, len(FEATURE_COLS))
    top_sum = importances[indices[:top_n]].sum() / total * 100
    print(f"\n  Top {top_n} 特征合计: {top_sum:.1f}% 的总信息量")

    # 阵营对比
    danmu_importance = sum(
        importances[FEATURE_COLS.index(c)]
        for c in ['danmu_total_rolling', 'danmu_positive_rolling',
                   'danmu_negative_rolling', 'danmu_avg_sentiment_rolling']
    )
    gift_importance = sum(
        importances[FEATURE_COLS.index(c)]
        for c in ['gift_value_rolling', 'gift_count_rolling']
    )
    print(f"\n  阵营对比:")
    print(f"    弹幕阵营 (互动): {danmu_importance/total*100:.1f}%")
    print(f"    礼物阵营 (氪金): {gift_importance/total*100:.1f}%")
    print(f"    弹幕/礼物比值 : {danmu_importance/gift_importance:.1f}x"
          if gift_importance > 0 else "    弹幕/礼物比值 : 无穷大 (礼物特征无贡献)")


# ============================================================
# 5. 模型持久化
# ============================================================

def save_model(model: RandomForestRegressor, model_path: str):
    joblib.dump(model, model_path)
    abs_path = os.path.abspath(model_path)
    print(f"\n[INFO] 模型已保存至: {abs_path}")
    print(f"[INFO] 文件大小: {os.path.getsize(abs_path) / 1024:.1f} KB")


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='抖音直播流量预测 — 模型训练脚本 (v3.0)'
    )
    parser.add_argument(
        'data_file', type=str, nargs='?', default='training_data.csv',
        help='训练集 CSV 路径（位置参数，默认: training_data.csv）'
    )
    parser.add_argument(
        '--model', type=str, default='traffic_rf_model.pkl',
        help='模型保存路径（默认: traffic_rf_model.pkl）'
    )
    args = parser.parse_args()

    print(f"{'='*60}")
    print(f"  抖音直播流量预测 — RandomForest 模型训练 (v3.0)")
    print(f"  目标: 预测 5 分钟后在线人数变化量 (Delta)")
    print(f"  数据集: {args.data_file}")
    print(f"{'='*60}")

    # Step 1: 加载与划分
    print(f"\n[STEP 1] 数据加载与划分")
    print(f"[INFO] 正在读取训练数据: {args.data_file}")
    X_train, X_test, y_train, y_test = load_and_split(args.data_file)

    # Step 2: 训练
    print(f"\n[STEP 2] 模型训练")
    model = train_model(X_train, y_train)

    # Step 3: 分层评估
    print(f"\n[STEP 3] 模型评估")
    evaluate_model(model, X_train, y_train, X_test, y_test)

    # Step 4: 特征重要性
    print(f"\n[STEP 4] 特征重要性分析")
    print_feature_importance(model)

    # Step 5: 保存模型
    print(f"\n[STEP 5] 模型持久化")
    save_model(model, args.model)

    print(f"\n{'='*60}")
    print(f"  训练流程结束")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
