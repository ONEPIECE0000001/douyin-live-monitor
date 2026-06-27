# 抖音直播全链路数据监控与流量预测平台 — 架构说明书

> **版本**：v7.1 (审计修订版)  
> **最后更新**：2026-06-04  
> **用途**：毕业设计论文参考 + 开发维护索引 + 新成员上手指南

> **前端代码**：本仓库仅包含 Python 后端与爬虫模块。前端采用 Vue3 + ECharts 开发，代码托管于独立仓库：
> 🔗 *（待补充链接）*

---

## 目录

- [一、系统架构全景](#一系统架构全景)
- [二、工程目录](#二工程目录)
- [三、URL 路由表与用户动线](#三url-路由表与用户动线)
- [四、核心模块详解](#四核心模块详解)
- [五、双库分工与数据模型](#五双库分工与数据模型)
- [六、启动流程](#六启动流程)
- [七、关键技术决策](#七关键技术决策)
- [八、技术栈](#八技术栈)
- [九、模块依赖关系图](#九模块依赖关系图)
- [十、已知问题与维护备忘](#十已知问题与维护备忘)

---

## 一、系统架构全景

```
┌─────────────────────────────────────────────────────────────┐
│                    抖音直播服务器 (CDN)                        │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTPS / WebSocket
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  沙盒 Chrome (--remote-debugging-port=9222)                  │
│  用户手动登录，绕过抖音反爬机制                                  │
└────────────────────────┬────────────────────────────────────┘
                         │ Chrome DevTools Protocol (CDP)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  spider.py  ——  数据采集层 (Playwright CDP 寄生模式)            │
│                                                             │
│  • 通过 CDP 连接已启动的沙盒 Chrome                             │
│  • 自动扫描所有 live.douyin.com 标签页 (多房间并发)               │
│  • 注入 INJECTED_JS 原生引擎 (500ms 雷达轮询)                   │
│  • 弹幕/礼物 DOM 解析 + data-e2e 在线人数精准锚定                │
│  • expose_function("reportData") 跨层回传 JSON                 │
│  • Anti-AFK 防休眠心跳 (拟人鼠标移动 + 滚轮 + 弹框检测)            │
└────────────────────────┬────────────────────────────────────┘
                         │ redis.Redis.rpush()
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Redis  ——  消息缓冲层 (三队列解耦)                             │
│                                                             │
│  douyin_danmu_queue / douyin_gift_queue / douyin_hot_queue    │
└────────────────────────┬────────────────────────────────────┘
                         │ redis.Redis.blpop()
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  consumer.py  ——  异步消费与持久层 (Django Standalone)          │
│                                                             │
│  3 个 QueueConsumer 线程 + 1 个 HotBatchFlusher 守护线程       │
│  • 数据清洗 → SnowNLP 情感分析 → MySQL / MongoDB 双写           │
│  • 动态 DataPersister 池支持多直播间并发                        │
│  • 礼物哈希 → 名称映射 (GIFT_MAP) + 自动收录未知礼物              │
│  • 指数退避重连 + 优雅退出 (SIGINT/SIGTERM)                    │
│  • 滑动窗口去重：聊天区与特效区重复礼物上报                        │
└────────────────────────┬────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                              ▼
┌──────────────────┐        ┌──────────────────────┐
│  MySQL (关系型)    │        │  MongoDB (文档型)     │
│  业务统计 + 外键   │        │  高频流水日志 + 时序快照 │
└──────────────────┘        └──────────────────────┘
          │                              │
          └──────────────┬──────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Django + Vue3 + ECharts  —  可视化与交互层                    │
│                                                             │
│  着陆页 → 登录/注册 → 管理门户 → 数据大屏                        │
│  • 工业级深色顶栏 + 动态时钟 + 呼吸状态指示灯 + 智能导航           │
│  • 聚光灯操作台 + 最近监控卡片                                  │
│  • 5 个 REST API + AI 流量预测 (Random Forest)                 │
│  • 4 个 ECharts 图表：趋势/排行/情感/AI 预测                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、工程目录

```
毕设/
├── .env                          # 🔐 本地环境变量 (Git 排除)
├── .env.example                  # 📋 环境变量模板
├── .gitignore                    # 🚫 Git 忽略规则
├── requirements.txt              # 📦 Python 依赖清单 (12 个包)
├── README_ARCHITECTURE.md        # 📖 本文件
│
├── archive/                      # 📦 历史归档 (不参与运行时)
│   ├── create_test_data.py       #   测试数据生成器 (⚠️ 路径已过时)
│   ├── list_users.py             #   用户列表辅助脚本 (⚠️ 路径已过时)
│   ├── plot_importance.py        #   特征重要性绘图脚本
│   ├── feature_importance.png    #   论文配图 — 特征重要性柱状图
│   ├── training_data.csv         #   历史训练集快照
│   └── unknown_gifts.json        #   ⚠️ 陈旧副本，活跃文件在 douyin_live/
│
└── douyin_live/                  # 🏗️ Django 项目根目录
    │
    ├── manage.py                 # Django CLI 入口
    ├── spider.py                 # 🕷️ 数据采集引擎 (679 行)
    ├── consumer.py               # 🔄 Redis 消费者引擎 (878 行)
    ├── mongo_client.py           # 🍃 MongoDB 单例客户端 (含索引创建)
    ├── check_services.py         # 🔧 数据库服务检测 (Windows 服务管理)
    ├── verify_mongo.py           # ✅ MongoDB 验收探针 (只读，无副作用)
    ├── build_dataset.py          # 📊 特征工程 (MongoDB → CSV, 463 行)
    ├── train_model.py            # 🤖 模型训练 (CSV → pkl, 292 行)
    ├── traffic_rf_model.pkl      # 🧠 已训练 RF 模型 (二进制)
    ├── unknown_gifts.json        # 📝 未识别礼物自动收录日志
    │
    ├── config/                   # ⚙️ Django 配置包
    │   ├── settings.py           #   MySQL / Redis / MongoDB / Channels
    │   ├── urls.py               #   URL 路由表 (11 条)
    │   └── asgi.py               #   ASGI 入口 (WebSocket 预留)
    │
    └── data_screen/              # 📊 数据大屏 App
        ├── models.py             #   LiveRoom + LiveSession ORM (2 表)
        ├── admin.py              #   SimpleUI 后台管理配置
        ├── api_views.py          #   5 个 REST API 端点 (stats/trend/gift_rank/sentiment/predict)
        ├── auth_views.py         #   API 登录保护装饰器 + ⚠️ 死代码 register_view
        ├── inference.py          #   AI 流量预测推理引擎 (RF 模型加载 + 特征提取)
        ├── views.py              #   统一认证视图 (6 个: landing/login/register/logout/portal/dashboard)
        ├── apps.py               #   App 元信息 (verbose_name='直播监控大盘')
        ├── templates/
        │   ├── registration/
        │   │   ├── landing.html  #   着陆页 (星空粒子 + Hero + CTA)
        │   │   ├── login.html    #   登录 (左品牌区 60% + 右表单 40%)
        │   │   ├── register.html #   注册 (居中卡片设计)
        │   │   └── portal.html   #   管理门户 (深色玻璃拟态 + 双卡片)
        │   └── data_screen/
        │       └── dashboard.html #  数据大屏 (1268 行: Vue3 + ECharts + 聚光灯操作台)
        └── migrations/           #   数据库迁移 (0001 ~ 0006)
```

---

## 三、URL 路由表与用户动线

### 3.1 路由表

| URL | 视图 | 认证 | 说明 |
|---|---|---|---|
| `/` | `landing_view` | 否 | 着陆页 (星空 Hero) |
| `/login/` | `user_login` | 否 | 登录 (分栏 SaaS 设计) |
| `/accounts/register/` | `user_register` | 否 | 用户注册 (自动登录) |
| `/accounts/logout/` | `user_logout` | 是 | 退出登录 |
| `/portal/` | `portal_index` | 是 | 管理门户 (中转页) |
| `/admin/` | Django Admin | 是 | 后台管理 (SimpleUI 美化) |
| `/dashboard/` | `dashboard_view` | 是 | 数据大屏 (Vue3 SPA) |
| `/api/stats/<room_id>/` | `stats` | API | 直播间宏观统计 + AI 预测 |
| `/api/charts/trend/<room_id>/` | `trend` | API | 近 30 分钟弹幕/礼物分钟级趋势 |
| `/api/charts/gift_rank/<room_id>/` | `gift_rank` | API | 打赏 Top 10 (Redis ZSET 实时) |
| `/api/charts/sentiment/<room_id>/` | `sentiment` | API | 近 500 条弹幕情感分布 |
| `/api/predict/<room_id>/` | `predict` | API | AI 流量预测 (Delta 变化量) |

### 3.2 用户动线

```
/ (着陆页)
  │
  └─ [进入控制台] → /login/ (分栏登录)
        │
        ├─ 新用户 → /accounts/register/ → 自动登录 → /dashboard/
        │
        └─ 登录成功 → /portal/ (管理门户)
              │
              ├─ 📊 数据大屏 → /dashboard/ (新标签页)
              │     ├─ 聚光灯操作台 (输入 Room ID)
              │     ├─ KPI 卡片 (4 个: 在线/点赞/礼物/时长)
              │     ├─ AI 流量预报卡片
              │     └─ 实时图表 (3 个: 趋势/排行/情感)
              │
              └─ ⚙️ 管理后台 → /admin/ (仅 staff 用户可见)
```

---

## 四、核心模块详解

### 4.1 认证体系 (`views.py` + `auth_views.py`)

| 视图 (views.py) | 功能 |
|---|---|
| `landing_view` | 着陆页渲染，已登录用户自动跳转 `/portal/` |
| `user_login` | GET 渲染登录页，POST 认证后统一跳转 `/portal/` |
| `user_register` | GET 渲染注册页，POST 创建普通用户 (`is_staff=False`)，自动登录跳转 `/dashboard/` |
| `user_logout` | 清除 session，返回登录页 |
| `portal_index` | 管理门户：双卡片导航 — 数据大屏 + 管理后台 (普通用户后台卡片锁定) |
| `dashboard_view` | 数据大屏入口，附带最近 5 个监控的直播间 |

**`auth_views.py` 提供的公共组件：**

| 组件 | 功能 |
|---|---|
| `api_login_required` | API 装饰器：未登录返回 `{"error":true, "message":"请先登录"}` + HTTP 401，前端 catch 自动跳转 |

> ⚠️ **注意**：`auth_views.py` 中的 `register_view` 函数是死代码，未被任何路由引用。实际注册逻辑在 `views.py:user_register` 中。

### 4.2 数据采集引擎 (`spider.py`)

**架构**：Playwright CDP 寄生模式，连接用户已登录的 Chrome 沙盒，通过多路并发接管所有 `live.douyin.com` 标签页。

| 特性 | 实现 |
|---|---|
| 采集频率 | 500ms 雷达轮询 (弹幕/礼物) + 5s 热度上报 |
| 弹幕解析 | DOM class 模糊匹配 + 冒号/句号分隔提取昵称与内容 |
| 礼物识别 | 三重回退定位 `<img>` + URL 哈希提取 (33位十六进制) + 连击数正则 |
| 在线人数 | `[data-e2e="live-room-audience"]` 精准锚定 + 万单位换算 |
| 点赞数 | 全页 `<span>/<div>` 遍历 + `本场点赞` 关键词正则 |
| 连击礼物 | `div[class*="GiftTray"]` MutationObserver 监听 + `_extractComboGiftData` 宽泛匹配 |
| 特效礼物 | `#GiftEffectLayout` MutationObserver 监听 + 递归子节点提取 |
| 防休眠 | Anti-AFK 心跳：安全区域拟人鼠标移动 + 滚轮翻阅 + "继续观看"弹框检测 |
| 不死鸟 | 外层 `while watchdog_running` 循环 + 指数退避重连 + 分段 sleep 响应 Ctrl+C |
| 多房间 | 自动扫描所有标签页，每页独立 `expose_function` + 独立注入 JS 引擎 |

**数据分发**：`DataDispatcher` 将 JSON 按 type 字段路由到 Redis 三队列：
- `danmu` → `douyin_danmu_queue`
- `gift` → `douyin_gift_queue`
- `hot` → `douyin_hot_queue`

Redis 不可用时自动降级为终端日志模式。

### 4.3 消费者引擎 (`consumer.py`)

**架构**：3 个独立 `QueueConsumer` 线程 (daemon) + 1 个 `HotBatchFlusher` 守护线程 + 动态 `DataPersister` 池。

| 组件 | 职责 |
|---|---|
| `QueueConsumer(DANMU)` | BLPOP 弹幕队列 → 清洗 → 情感分析 → DataPersister.persist_danmu |
| `QueueConsumer(GIFT)` | BLPOP 礼物队列 → 清洗 → 哈希查表 → 去重 → DataPersister.persist_gift |
| `QueueConsumer(HOT)` | BLPOP 热度队列 → 清洗 → MySQL 实时更新 + Per-room MongoDB 批量缓冲 |
| `HotBatchFlusher` | 每 30s 遍历所有 DataPersister 的热度缓冲区 → 批量写入 MongoDB |
| `DataPersister` | 双写协调器：MongoDB 日志写入 + MySQL ORM 统计更新 + Redis 排行榜 ZINCRBY |

**礼物识别流水线**：
1. 精确匹配 GIFT_MAP key
2. 前缀包容匹配 (双方各取前 15 位哈希特征码)
3. 未知 → 自动收录到 `unknown_gifts.json` + 钻石数默认 99

> ⚠️ **已知 BUG**：GIFT_MAP 中 `906a6c6371474ea` 键重复（'无尽宝藏' 被 '无尽浪漫' 覆盖），详见第十章。

**滑动窗口去重**：同一 `username_giftImageId_count` 指纹在 3 秒内只计一次，防止聊天区轮询和特效区 MutationObserver 重复上报。

### 4.4 数据大屏 (`dashboard.html`)

1268 行单文件 SPA，基于 Vue 3 + ECharts 5 + TailwindCSS + Axios：

| 区块 | 说明 |
|---|---|
| 工业级深色顶栏 | 系统名称 + 动态数字时钟 + 呼吸状态指示灯 + 智能前进/后退按钮 + 用户名 + 退出 |
| 聚光灯操作台 | 毛玻璃卡片 + 胶囊输入框 + "⚡启动引擎"按钮 + 最近监控卡片 (后端 `recent_rooms` 真实数据) |
| 监控状态栏 | 主播名 Badge + "监控中"呼吸标签 + 停止按钮 |
| KPI 卡片 ×4 | 在线人数 / 累计点赞 / 礼物价值 / 监控时长，水印图标 + 级联入场动画 (`fadeInUp` + 延迟) |
| AI 预报卡片 | 趋势箭头 + Delta 数值 + 状态圆点 (up=绿/down=红/stable=灰) + 缓存标签 |
| ECharts ×3 | 趋势图 (双 Y 轴: 弹幕条数 + 礼物价值元) / 排行图 (横向柱状渐变) / 情感环形饼图 |

**数据刷新**：5 秒轮询，5 路 Axios 并发请求，ECharts 复用实例 `setOption(option, true)` 增量更新。

### 4.5 后端 API (`api_views.py`)

| 端点 | 数据源 | 说明 |
|---|---|---|
| `stats` | MySQL ORM + 推理引擎 | 房间宏观统计 (在线/点赞/礼物/时长/场次) + AI 预测附加 |
| `trend` | MongoDB 聚合管道 | `$floor($divide(timestamp,60000))` 分钟分桶 + 弹幕计数 + 礼物价值求和 |
| `gift_rank` | Redis 有序集合 | `ZREVRANGE` Top 10 + `HGET` 礼物数量 (consumer 实时维护) |
| `sentiment` | MongoDB 聚合管道 | 取最新 500 条，`$cond` 三分段聚合 (正面>0.6/中性/负面<0.4) |
| `predict` | ML 模型推理 + Redis 缓存 | 10 秒 TTL 缓存，三层柔性降级 (模型缺失/数据空洞/推理异常) |

### 4.6 机器学习流水线

```
数据采集 (consumer.py)
  │  MongoDB 三集合实时写入
  ▼
特征工程 (build_dataset.py <room_id>)
  │  加载 → 时间戳转换 → 清洗(ffill) → 分钟重采样
  │  → 三表合并 → 断流切割 (>15分钟=新场次)
  │  → 按 session_id 隔离: 3分钟滑动窗口滚动特征 + shift(-5) Delta 标签
  │  → 追加写入 CSV
  ▼
模型训练 (train_model.py [data.csv])
  │  RandomForestRegressor (n=200, depth=10, min_samples_leaf=3)
  │  → 训练/测试 80/20 分层评估 (全量 + 活跃子集 + 方向准确率)
  │  → 特征重要性排名 → joblib 持久化 pkl
  ▼
实时推理 (inference.py)
  │  服务启动时 lazy load pkl
  │  → API 调用时实时查 MongoDB 过去 3 分钟数据
  │  → 6 维特征对齐 → model.predict() → Delta
  │  → Redis 缓存 10s (占位响应不缓存)
```

**特征** (6 维，均来自过去 3 分钟窗口)：
1. `danmu_total_rolling` — 弹幕总量
2. `danmu_positive_rolling` — 正向弹幕 (sentiment > 0.6)
3. `danmu_negative_rolling` — 负向弹幕 (sentiment < 0.4)
4. `danmu_avg_sentiment_rolling` — 平均情感得分
5. `gift_value_rolling` — 礼物总价值 (音浪)
6. `gift_count_rolling` — 送礼人次

**目标变量**：`target_delta_5m` — 5 分钟后在线人数变化量 (正值=上涨，负值=下跌)

---

## 五、双库分工与数据模型

### 5.1 MySQL — `douyin_live` 库 (关系型业务统计)

#### `live_room` (直播间宏观档案)

| 字段 | 类型 | 说明 |
|---|---|---|
| `room_id` (PK) | VARCHAR(50) | 抖音直播间 ID |
| `host_name` | VARCHAR(100) | 主播昵称 (consumer 自动纠正占位名) |
| `host_id` | VARCHAR(50) | 主播 ID (预留，当前未使用) |
| `current_online` | INT | 当前在线人数 (hot 数据实时更新) |
| `total_likes` | BIGINT | 累计点赞 (只升不降) |
| `total_gifts_value` | DECIMAL(15,2) | 累计礼物价值 (元)，gift 消费实时累加 |
| `viewer_count` / `like_count` | INT / BIGINT | ⚠️ 遗留字段，当前未被代码写入 |
| `created_at` / `updated_at` | DATETIME | auto_now_add / auto_now |

#### `live_session` (直播场次流水)

| 字段 | 类型 | 说明 |
|---|---|---|
| `session_id` (PK) | VARCHAR(50) | 场次唯一 ID (room_id_YYYYMMDDHHmmss) |
| `room` (FK) | LiveRoom | 外键关联直播间 |
| `start_time` / `end_time` | DATETIME | 开始/结束时间 (end_time=NULL=进行中) |
| `peak_online` | INT | 峰值在线人数 (hot 数据实时挑战) |
| `total_danmu` | BIGINT | 弹幕总量 (danmu 消费实时累加) |

### 5.2 MongoDB — `douyin_live` 库 (文档型流水日志)

| 集合 | 典型文档字段 | 索引 |
|---|---|---|
| `danmu_log` | room_id, host_name, nickname, content, sentiment, timestamp | `room_id`, `timestamp`, `(room_id,timestamp)` |
| `gift_log` | room_id, host_name, username, gift_name, gift_image_id, count, diamond, value_yinlang, timestamp | `room_id`, `timestamp`, `username`, `(room_id,timestamp)` |
| `hot_trend` | room_id, host_name, online_count, like_count, timestamp | `room_id`, `timestamp`, `(room_id,timestamp)` |

### 5.3 Redis 键空间

| Key 模式 | 类型 | 用途 | TTL |
|---|---|---|---|
| `douyin_danmu_queue` | List | 弹幕消息队列 | 无 (消费即删) |
| `douyin_gift_queue` | List | 礼物消息队列 | 无 |
| `douyin_hot_queue` | List | 热度消息队列 | 无 |
| `live_gift_rank:{room_id}` | ZSET | 打赏排行 (username → 音浪总分) | 6 小时 |
| `live_gift_count:{room_id}` | Hash | 打赏次数 (username → count) | 6 小时 |
| `prediction:{room_id}` | String (JSON) | AI 预测结果缓存 | 10 秒 |

---

## 六、启动流程

### 6.1 前置条件

1. **MySQL 8.0**、**MongoDB 7.x**、**Redis 7.x** 服务已启动
2. **Chrome** 以调试模式启动：
   ```powershell
   "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222
   ```
   然后用这个 Chrome 窗口手动登录抖音账号，进入任意直播间
3. Python 虚拟环境已激活且依赖已安装

### 6.2 首次部署

```powershell
# 1. 克隆项目 & 创建虚拟环境
python -m venv .venv
.venv\Scripts\activate

# 2. 安装依赖
pip install -r requirements.txt

# 3. 安装 Playwright 浏览器驱动
playwright install chromium

# 4. 配置环境变量
cp .env.example .env
# 编辑 .env 填入数据库密码

# 5. 初始化 MySQL 数据库
python douyin_live\manage.py migrate
python douyin_live\manage.py createsuperuser

# 6. 检查数据库连通性
python douyin_live\check_services.py

# 7. 启动 Django Web 服务
python douyin_live\manage.py runserver
```

### 6.3 启动数据采集

需要 **3 个终端窗口**：

```powershell
# 终端 1 — Django Web 服务器
python douyin_live\manage.py runserver

# 终端 2 — 消费者 (先启动，等待数据)
python douyin_live\consumer.py

# 终端 3 — 采集引擎 (Chrome 9222 端口已就绪后启动)
python douyin_live\spider.py
```

### 6.4 数据验收

```powershell
# 验证 MongoDB 入库情况
python douyin_live\verify_mongo.py
```

### 6.5 访问

| 页面 | URL | 说明 |
|---|---|---|
| 着陆页 | `http://127.0.0.1:8000/` | 星空 Hero 页，引导登录 |
| 登录 | `http://127.0.0.1:8000/login/` | 分栏 SaaS 设计 |
| 注册 | `http://127.0.0.1:8000/accounts/register/` | 创建普通用户 |
| 管理门户 | `http://127.0.0.1:8000/portal/` | 大屏 + 后台双卡片 |
| 数据大屏 | `http://127.0.0.1:8000/dashboard/` | 输入 Room ID 启动监控 |
| 管理后台 | `http://127.0.0.1:8000/admin/` | SimpleUI 美化，需 staff 权限 |

---

## 七、关键技术决策

| # | 决策 | 理由 |
|---|------|------|
| 1 | **CDP 寄生模式** | 绕过抖音 X-Bogus/X-Gorgon 签名加密，直接操作真实浏览器 DOM，无需破解协议 |
| 2 | **Redis 三队列分离** | 避免 head-of-line blocking — 一条慢礼物不会阻塞高频弹幕，支持独立水平扩展 |
| 3 | **MongoDB 批量写入** | 热度数据每 20 条或 30 秒批量写入，减少网络往返开销 |
| 4 | **`data-e2e` 选择器** | 抖音 CSS class 名动态混淆，`[data-e2e="live-room-audience"]` 是唯一稳定锚点 |
| 5 | **null-safe 防御** | 在线人数提取失败传 `null` 而不传 `0`，避免覆盖 MySQL 中的真实值 |
| 6 | **Delta 目标变量** | 预测在线人数**变化量**而非绝对值，模型学习"加速/减速"信号而非记忆基线 |
| 7 | **按场次隔离** | ffill / rolling / shift 全部在 `groupby('session_id')` 内执行，杜绝跨场次数据污染 |
| 8 | **滑动窗口去重** | 同一用户+同一礼物+同一连击数 3 秒内只计一次，消除聊天区/特效区双 DOM 区域重复 |
| 9 | **三层柔性降级** | 模型缺失 / 数据空洞 / 推理异常均返回 HTTP 200 + `delta=0, status=stable`，前端不报错 |
| 10 | **API 装饰器认证** | 未登录返回 `401 JSON`（非 302 重定向），前端 Vue3 catch 块自动 `window.location` 跳转 |
| 11 | **礼物哈希前缀匹配** | 抖音 CDN 可能微调图片 URL，15 位特征码前缀匹配提供容错性 |
| 12 | **不死鸟守护进程** | spider.py 外层持续循环 + 指数退避 (上限 5 分钟) + 分段 sleep 保证 Ctrl+C 3 秒内响应 |
| 13 | **USE_TZ = False** | Windows MySQL 无时区表，关闭时区感知避免 `pytz` 异常 |

---

## 八、技术栈

| 层级 | 组件 | 版本 | 用途 |
|---|---|---|---|
| 运行时 | Python | 3.9+ | 主语言 |
| Web 框架 | Django | 4.2.x | ORM + Admin + 模板引擎 |
| Admin 美化 | SimpleUI | 2026.x | Django Admin 现代化界面 |
| ASGI 服务器 | Daphne | 4.0+ | ASGI 入口 (预留 WebSocket) |
| 浏览器自动化 | Playwright | 1.42+ | CDP 寄生模式连接 Chrome |
| 消息队列 | Redis | 7.x / 5.0 | 三队列解耦 + 排行榜缓存 |
| 关系型数据库 | MySQL | 8.0 | 业务统计 (LiveRoom + LiveSession) |
| 文档数据库 | MongoDB | 7.x / 4.6 | 日志流水 (弹幕/礼物/热度) |
| MySQL 驱动 | mysqlclient | 2.2+ | Django MySQL 后端 |
| MongoDB 驱动 | PyMongo | 4.6+ | MongoDB Python 驱动 |
| 中文 NLP | SnowNLP | 0.12+ | 弹幕情感分析 (0~1 极性) |
| 机器学习 | Scikit-learn | 1.4+ | RandomForestRegressor |
| 数据处理 | Pandas | 2.2+ | 特征工程与数据清洗 |
| 模型持久化 | Joblib | (sklearn 内置) | pkl 序列化 |
| 前端框架 | Vue 3 | CDN | 响应式 SPA |
| 图表库 | ECharts 5.6 | CDN | 趋势/柱状/饼图 |
| CSS 框架 | TailwindCSS | CDN | 原子化 CSS |
| HTTP 客户端 | Axios 1.7 | CDN | 前端 API 调用 |
| 环境变量 | python-dotenv | 1.0+ | .env 文件加载 |

---

## 九、模块依赖关系图

```
spider.py ──RPUSH──▶ Redis (三队列) ──BLPOP──▶ consumer.py
                                                    │
                                    ┌───────────────┼───────────────┐
                                    ▼               ▼               ▼
                              mongo_client.py  data_screen/    data_screen/
                              (MongoDB 写入)   models.py      inference.py
                                               (MySQL ORM)    (RF 预测)
                                                    │               │
                                                    └───────┬───────┘
                                                            ▼
                                                    api_views.py
                                                    (5 个 REST API)
                                                            │
                                              ┌─────────────┴─────────────┐
                                              ▼                           ▼
                                      dashboard.html             predict_next_5m()
                                      (Vue3 + ECharts)           (ML 推理 + Redis 缓存)

views.py ──▶ landing.html / login.html / register.html / portal.html
                  │
                  └──▶ urls.py ──▶ 11 条路由分发

build_dataset.py ──▶ MongoDB ──▶ CSV ──▶ train_model.py ──▶ traffic_rf_model.pkl
                                                                      │
                                                                      ▼
                                                              inference.py
```

---

## 十、已知问题与维护备忘

### 10.1 需要修复的 Bug

| 优先级 | 文件 | 问题 | 修复建议 |
|---|---|---|---|
| **P0** | `consumer.py:113,118` | GIFT_MAP 字典重复键 `906a6c6371474ea`，'无尽宝藏' 被 '无尽浪漫' 静默覆盖 | 核实两个名称对应的实际哈希是否相同，若不同则修正其中一个 key；若相同则保留最新名称，删除旧条目 |
| **P1** | `auth_views.py:38-88` | `register_view` 死代码，未被任何路由引用 | 删除该函数，避免与 `views.py:user_register` 混淆 |
| **P2** | `views.py:1-18` | `django.contrib.auth` 的 import 放在 `landing_view` 定义之后 | 将所有 import 移至文件顶部 |

### 10.2 待清理

| 优先级 | 位置 | 问题 | 建议 |
|---|---|---|---|
| **P1** | `archive/unknown_gifts.json` | 陈旧副本 (22KB)，活跃文件在 `douyin_live/unknown_gifts.json` | 删除 archive 中的副本 |
| **P2** | `archive/create_test_data.py` | `DJANGO_SETTINGS_MODULE` 指向过时的 `'douyin_live.settings'` | 改为 `'config.settings'` 或在文件顶注释说明此为归档脚本 |
| **P2** | `archive/list_users.py` | 同上，过时的 settings 路径 | 同上 |
| **P3** | `LiveRoom` 模型 | `viewer_count` 和 `like_count` 字段从未被代码写入 | 评估是否需要保留，或创建迁移删除 |
| **P3** | `settings.py` | Channels/Redis 配置完整但无实际 WebSocket consumer | 如果短期内不使用 WebSocket，可移除 channels 依赖以简化部署 |

### 10.3 潜在风险

1. **GIFT_MAP 维护负担**：礼物哈希映射表硬编码在 `consumer.py` 中，抖音可能随时新增/调整礼物。当前机制会自动收录未知礼物到 `unknown_gifts.json` 并使用默认钻石数 99，运营人员需定期检查该文件并回填 GIFT_MAP。

2. **Chrome 版本兼容性**：Playwright 绑定的 Chromium 驱动需与系统 Chrome 版本匹配。当 Chrome 自动更新后，可能需要重新执行 `playwright install chromium`。

3. **Windows 服务依赖**：`check_services.py` 硬编码了 Windows 服务名 (`MySQL80`, `MongoDB`)，如果使用不同版本或手动安装路径，需要调整。

4. **单点故障**：Redis 是 spider → consumer 之间唯一的消息通道。如果 Redis 宕机，consumer 和 spider 都会降级运行 (spider 降级为终端日志，consumer 无法启动)。

---

> 📝 **维护提示**：每次修改模型 (models.py) 后请执行 `python manage.py makemigrations` + `migrate`；每次新增礼物到 GIFT_MAP 后请同步更新本文档的维护日期。
