#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
抖音直播全景监控系统 (雷达轮询 + 寄生模式) 不死鸟守护版 v6.0
"""

import re
import time
import json
import random
import signal
import sys
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Playwright
from typing import Optional, Dict, Any

# ============================================
# 基础配置
# ============================================
DEBUG_PORT = 9222
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

QUEUE_DANMU = 'douyin_danmu_queue'
QUEUE_GIFT = 'douyin_gift_queue'
QUEUE_HOT = 'douyin_hot_queue'

# Anti-AFK 防休眠心跳配置
AFK_HEARTBEAT_MIN = 60    # 心跳间隔下限（秒）：1 分钟
AFK_HEARTBEAT_MAX = 150   # 心跳间隔上限（秒）：2.5 分钟
# 安全操作区域 (x1, x2, y1, y2) — 严格避开视频中央播放器与可点击跳转链接
# 基于 1920×1080 典型页面布局：左侧弹幕面板、右上角标题栏、右侧礼物装饰区
AFK_SAFE_ZONES = [
    (10, 150, 200, 800),     # 弹幕容器左侧边缘：模拟"扫视弹幕"的余光移动
    (1000, 1800, 8, 40),     # 顶部标题栏右翼：远离房间头像与关注按钮
    (1700, 1900, 300, 700),  # 最右侧礼物动画装饰区背景
]
AFK_SCROLL_DELTA_MIN = 200   # 滚轮纵向像素下限
AFK_SCROLL_DELTA_MAX = 500   # 滚轮纵向像素上限

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    redis = None
    REDIS_AVAILABLE = False

redis_client: Optional['redis.Redis'] = None

# 不死鸟守护进程全局控制标志
watchdog_running = True

def signal_handler(signum, frame):
    global watchdog_running
    print("\n[INFO] 收到退出指令，守护进程将在当前周期结束后安全退出...")
    watchdog_running = False
    if redis_client:
        try:
            redis_client.close()
        except Exception:
            pass

# ============================================
# 核心注入引擎：雷达主动轮询 (带全面容错防御机制)
# ============================================
INJECTED_JS = r"""
window.__danmuCache = new Set();
window.__giftCache = new Set();
window.__lastHotReport = 0;

// 辅助函数：从 URL pathname 提取房间号注入数据包
function _enrich(d) {
    var parts = window.location.pathname.split('/');
    var raw = parts[parts.length - 1] || '';
    d.room_id = raw.replace(/\D/g, '');
    return d;
}

// 从 DOM 元素中提取礼物数据（图片哈希ID、用户名、连击数）
// 供聊天区轮询和特效区 MutationObserver 共用
function _extractGiftData(item) {
    // 1. 精准定位礼物 <img> — 三重回退
    var giftImg = null;
    var allSpans = item.querySelectorAll('span');
    for (var si = 0; si < allSpans.length; si++) {
        var spanText = allSpans[si].innerText || allSpans[si].textContent || '';
        if (spanText.indexOf('送出了') !== -1) {
            giftImg = allSpans[si].querySelector('img[alt=""]') ||
                      allSpans[si].querySelector('img');
            if (giftImg) break;
        }
    }
    if (!giftImg) giftImg = item.querySelector('img[alt=""]');
    if (!giftImg) {
        var imgs = item.querySelectorAll('img');
        if (imgs.length > 0) giftImg = imgs[imgs.length - 1];
    }

    // 2. 从 src URL 提取图片哈希 ID
    var giftImageId = '';
    if (giftImg) {
        var src = giftImg.src || giftImg.getAttribute('src') || '';
        var hashMatch = src.match(/\/([a-f0-9]{20,40})\.(?:png|webp|jpg|jpeg|gif)/i);
        if (hashMatch) giftImageId = hashMatch[1];
    }

    // 3. 提取连击数量
    var text = item.innerText || item.textContent || '';
    var giftCount = 1;
    var countMatch = text.match(/[×x]\s*(\d+)/);
    if (countMatch) giftCount = parseInt(countMatch[1]) || 1;

    // 4. 提取用户名
    var username = '';
    var usernameMatch = text.match(/(.+?)[\s\n]*(?:送出了|送出|赠送)/);
    if (usernameMatch) {
        username = usernameMatch[1].split('\n').pop().trim();
    }

    return { username: username, giftImageId: giftImageId, giftCount: giftCount };
}

// 从连击/特效区 DOM 中提取礼物数据（宽泛特征匹配策略）
// 连击区结构与弹幕区完全不同：无"送出了"特征词，由嵌套div+多图+零散文本组成
function _extractComboGiftData(node) {
    // 1. 遍历所有 img，嗅探 src 中的礼物图片哈希
    var giftImageId = '';
    var imgs = node.querySelectorAll('img');
    for (var i = 0; i < imgs.length; i++) {
        var src = imgs[i].src || imgs[i].getAttribute('src') || '';
        var hashMatch = src.match(/\/([a-f0-9]{20,40})\.(?:png|webp|jpg|jpeg|gif)/i);
        if (hashMatch) {
            giftImageId = hashMatch[1];
            break;
        }
    }

    // 2. 提取连击数量
    var fullText = node.innerText || node.textContent || '';
    var giftCount = 1;
    var countMatch = fullText.match(/[x×X]\s*(\d+)/);
    if (countMatch) giftCount = parseInt(countMatch[1]) || 1;

    // 3. 提取用户名：按换行分割，取首个非空行，剔除粘连的送出/xN 干扰
    var username = '';
    var lines = fullText.split('\n');
    for (var li = 0; li < lines.length; li++) {
        var line = lines[li].trim();
        if (!line) continue;
        line = line.replace(/送出.*$/, '').replace(/赠送.*$/, '').replace(/[x×X]\s*\d+/i, '').trim();
        if (line) {
            username = line;
            break;
        }
    }

    return { username: username, giftImageId: giftImageId, giftCount: giftCount };
}

// 礼物去重投递：同一用户+同一图片+同一数量在单次采集周期内去重
function _sendGift(giftData) {
    if (!giftData.username || !giftData.giftImageId) return;
    var uniqueId = giftData.username + '|' + giftData.giftImageId + '|' + giftData.giftCount;
    if (window.__giftCache.has(uniqueId)) return;
    window.__giftCache.add(uniqueId);
    if (window.reportData) window.reportData(_enrich({
        type: 'gift',
        username: giftData.username,
        gift_image_id: giftData.giftImageId,
        count: giftData.giftCount,
        timestamp: Date.now()
    }));
    if (window.__giftCache.size > 500) {
        window.__giftCache = new Set(Array.from(window.__giftCache).slice(-200));
    }
}

function pollData() {
    try {
        // 1. 扫描弹幕和礼物
        const chatItems = document.querySelectorAll(
            'div[class*="chat-item"], div[class*="webcast-chatroom___item"], div[data-e2e="chat-item"], div[class*="CommentItem"]'
        );

        chatItems.forEach(item => {
            try {
                const text = item.innerText || item.textContent || '';
                if (!text || text.length > 200 || text.includes('进入了直播间') || text.includes('分享了直播间')) return;

                // 识别礼物 — 复用 _extractGiftData / _sendGift 公共函数
                if (text.indexOf('送出了') !== -1 || text.indexOf('送出') !== -1 || text.indexOf('赠送') !== -1) {
                    var giftData = _extractGiftData(item);
                    _sendGift(giftData);
                    return;
                }

                // 识别弹幕
                let separator = text.includes('：') ? '：' : (text.includes(':') ? ':' : null);
                if (separator) {
                    let parts = text.split(separator);
                    let rawName = parts[0].trim();
                    let content = parts.slice(1).join(separator).trim();
                    
                    // 防御性提取昵称，防止因奇怪字符导致 split 报错
                    let nameArray = rawName.split('\n').pop().split(' ');
                    let nickname = nameArray[nameArray.length - 1].trim();
                    
                    if (nickname && content) {
                        let uniqueId = nickname + '|' + content; 
                        if (!window.__danmuCache.has(uniqueId)) {
                            window.__danmuCache.add(uniqueId);
                            let data = { type: 'danmu', nickname: nickname, content: content, timestamp: Date.now() };
                            
                            // 双重保险：确保通道存在再发送
                            if (window.reportData) window.reportData(_enrich(data)); 
                            
                            if (window.__danmuCache.size > 2000) {
                                window.__danmuCache = new Set(Array.from(window.__danmuCache).slice(-1000));
                            }
                        }
                    }
                }
            } catch (e) {
                // 忽略单条数据的解析错误，防止整个引擎崩溃
            }
        });

        // 2. 扫描热度数据 — [data-e2e] 精准锚定 + 小元素遍历点赞
        const now = Date.now();
        if (now - window.__lastHotReport >= 5000) {
            window.__lastHotReport = now;

            var onlineCount = null;
            var likeCount = null;

            // ---------- 在线人数：data-e2e 精准选择器 ----------
            try {
                var audienceEl = document.querySelector('[data-e2e="live-room-audience"]');
                if (audienceEl) {
                    var rawText = (audienceEl.innerText || audienceEl.textContent || '').trim();
                    if (rawText) {
                        var numVal = parseFloat(rawText.replace(/[^0-9.]/g, ''));
                        if (!isNaN(numVal) && numVal > 0) {
                            if (/[万w]/.test(rawText)) numVal = Math.floor(numVal * 10000);
                            else numVal = Math.floor(numVal);
                            onlineCount = numVal;
                        }
                    }
                }
            } catch(e) {}

            // ---------- 点赞数：小元素遍历 + 关键词正则 ----------
            var likeMax = 0;
            var likeRegex = /([\d,]+(?:\.\d+)?)\s*(?:w|万)?\s*本场点赞/i;
            var allElements = document.querySelectorAll('span, div');
            for (var i = 0; i < allElements.length; i++) {
                try {
                    var text = (allElements[i].innerText || allElements[i].textContent || '').trim();
                    if (!text || text.length >= 30) continue;
                    var m = text.match(likeRegex);
                    if (m) {
                        var val = parseFloat(m[1].replace(/,/g, ''));
                        if (!isNaN(val) && val > 0) {
                            if (/[万w]/.test(m[0])) val *= 10000;
                            else if (val < 100) val = 0;
                            if (val > likeMax) likeMax = Math.floor(val);
                        }
                    }
                } catch(e) {}
            }
            if (likeMax > 0) likeCount = likeMax;

            // 至少一个指标有效才上报，无效指标保持 null 触发后端防御
            if (onlineCount !== null || likeCount !== null) {
                var data = { type: 'hot', online_count: onlineCount, like_count: likeCount, timestamp: now };
                if (window.reportData) window.reportData(_enrich(data));
            }
        }
    } catch (globalError) {
        // 全局防御
    }
}

// 每隔 500 毫秒执行一次全局扫描
setInterval(pollData, 500);

// ── 连击区 (GiftTrayLayout) + 全屏特效 (GiftEffectLayout) 双监听 ──
(function() {
    var _comboRetries = 0;
    var _MAX_COMBO_RETRIES = 30;

    // ── 监听 1：左下角连击区 div[class*="GiftTray"] ──
    function _tryObserveCombo() {
        var target = document.querySelector('div[class*="GiftTray"]');
        if (!target) {
            _comboRetries++;
            if (_comboRetries < _MAX_COMBO_RETRIES) {
                setTimeout(_tryObserveCombo, 2000);
            } else {
                console.log('⚠️ [Spider] 连击区域选择器未匹配到元素，已放弃监听');
            }
            return;
        }
        console.log('✅ [Spider] 连击区 MutationObserver 已挂载 (GiftTray)');
        var observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                mutation.addedNodes.forEach(function(node) {
                    if (node.nodeType === 1) {
                        var giftData = _extractComboGiftData(node);
                        _sendGift(giftData);
                        var childDivs = node.querySelectorAll('div, span');
                        for (var ci = 0; ci < childDivs.length; ci++) {
                            var childData = _extractComboGiftData(childDivs[ci]);
                            _sendGift(childData);
                        }
                    }
                });
            });
        });
        observer.observe(target, { childList: true, subtree: true });
    }

    // ── 监听 2：全屏高级礼物特效 #GiftEffectLayout ──
    var _effectRetries = 0;
    var _MAX_EFFECT_RETRIES = 30;

    function _tryObserveEffect() {
        var target = document.querySelector('#GiftEffectLayout');
        if (!target) {
            _effectRetries++;
            if (_effectRetries < _MAX_EFFECT_RETRIES) {
                setTimeout(_tryObserveEffect, 2000);
            } else {
                console.log('⚠️ [Spider] 全屏特效容器 #GiftEffectLayout 未出现，已放弃监听');
            }
            return;
        }
        console.log('✅ [Spider] 全屏特效 MutationObserver 已挂载 (GiftEffectLayout)');
        var observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                mutation.addedNodes.forEach(function(node) {
                    if (node.nodeType === 1) {
                        var giftData = _extractComboGiftData(node);
                        _sendGift(giftData);
                        var childDivs = node.querySelectorAll('div, span');
                        for (var ci = 0; ci < childDivs.length; ci++) {
                            var childData = _extractComboGiftData(childDivs[ci]);
                            _sendGift(childData);
                        }
                    }
                });
            });
        });
        observer.observe(target, { childList: true, subtree: true });
    }

    // 延迟 3 秒后开始尝试挂载（等待页面 DOM 完全渲染）
    setTimeout(_tryObserveCombo, 3000);
    setTimeout(_tryObserveEffect, 3000);
})();

console.log('✅ [Spider] 高频雷达引擎已启动，防弹容错机制已开启...');
"""

# ============================================
# 数据分发模块
# ============================================
class DataDispatcher:
    def __init__(self, host='localhost', port=6379, db=0):
        self.use_redis = REDIS_AVAILABLE
        self.client = None
        self.queues = {'danmu': QUEUE_DANMU, 'gift': QUEUE_GIFT, 'hot': QUEUE_HOT}
        
        if self.use_redis:
            try:
                self.client = redis.Redis(host=host, port=port, db=db, decode_responses=False)
                self.client.ping()
                print("[INFO] Redis 就绪：实时数据流已建立")
            except Exception:
                self.use_redis = False
                print("[WARNING] Redis 离线：降级为控制台日志模式")
        else:
            print("[INFO] 依赖缺失：未检测到 Redis，开启控制台日志模式")
    
    def dispatch(self, data: Dict[str, Any]):
        try:
            data_type = data.get('type')
            if data_type not in self.queues:
                return

            # 终端回显
            if data_type == 'danmu':
                print(f"[DANMU] 👤 {data.get('nickname', '')}: 💬 {data.get('content', '')}")
            elif data_type == 'gift':
                # 礼物日志统一由后端 consumer.py 输出，前端静默抓取
                pass
            elif data_type == 'hot':
                print(f"[HOT] 🔥 在线: {data.get('online_count', '?')} | 👍 点赞: {data.get('like_count', 0)}")

            # Redis 缓冲推送
            if self.use_redis and self.client:
                message = json.dumps(data, ensure_ascii=False).encode('utf-8')
                self.client.rpush(self.queues[data_type], message)
        except Exception as e:
            # ⚠️ 不可静默吞异常，必须打印以便追踪 gift 断流
            print(f"[DISPATCH ERROR] type={data.get('type', '?')} | {type(e).__name__}: {e}")

# ============================================
# 主控引擎
# ============================================
class DouyinLiveSpider:
    def __init__(self, dispatcher: DataDispatcher):
        self.dispatcher = dispatcher
        self.playwright: Optional[Playwright] = None
        self.browser = None
        self.page = None
        self.host_name: Optional[str] = None  # 从页面标题提取的主播名
        self.target_pages: list = []          # 多页面并发：所有目标直播间页
        self.room_info: dict = {}             # room_id → host_name 映射
        # Anti-AFK 心跳计时器
        self._last_heartbeat = 0.0
        self._next_heartbeat_interval = 0

    # ============================================================
    # 防休眠心跳 (Anti-AFK Heartbeat)
    # ============================================================
    def _anti_afk_heartbeat(self, page):
        """
        [终极强化版] 在页面安全区域模拟真人鼠标动作，并自动点掉突发的"暂停播放"弹框
        """
        # 1. 随机选择安全区域与目标坐标
        zone = random.choice(AFK_SAFE_ZONES)
        target_x = random.randint(zone[0], zone[1])
        target_y = random.randint(zone[2], zone[3])

        # 2. 分段拟人移动鼠标
        steps = random.randint(2, 4)
        for i in range(steps):
            step_x = target_x + random.randint(-20, 20)
            step_y = target_y + random.randint(-20, 20)
            page.mouse.move(step_x, step_y)
            time.sleep(random.uniform(0.05, 0.15))

        # 3. 纵向滚轮：模拟翻阅弹幕
        time.sleep(random.uniform(0.2, 0.6))
        delta_y = random.randint(AFK_SCROLL_DELTA_MIN, AFK_SCROLL_DELTA_MAX)
        page.mouse.wheel(0, delta_y)

        # 4. 【核心增强】暴力除草：检测并点掉休眠弹框或暂停状态
        try:
            # 方法A：寻找屏幕上是否出现了带有"继续"字样的按钮并点击
            play_btn = page.locator("text='继续观看'").first
            if play_btn.is_visible(timeout=500):
                play_btn.click()
                print("[AFK] 捕获到休眠弹框，已自动点击【继续观看】恢复直播流！")

            # 方法B：如果画面被暗化暂停，但在视频区域盲点一下就能恢复
            # 我们在安全区域滑完之后，顺手在视频偏右侧点一下左键
            page.mouse.click(1200, 400) # 假设的视频安全点击坐标

        except Exception as e:
            pass # 没找到弹框或点击失败则静默跳过，说明状态正常

    def connect_browser(self):
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.connect_over_cdp(f'http://127.0.0.1:{DEBUG_PORT}')

            # 扫描所有目标页面：收集全部 live.douyin.com 且 URL 带数字的房间标签页
            self.target_pages = []
            self.room_info = {}

            for context in self.browser.contexts:
                for p in context.pages:
                    if "live.douyin.com" in p.url:
                        # 用 urlparse 剥离查询参数，只取 path 末段数字
                        parsed = urlparse(p.url)
                        path_segments = parsed.path.strip('/').split('/')
                        room_id_candidate = path_segments[-1] if path_segments else ''
                        # 只保留纯数字房间号
                        room_id_clean = room_id_candidate.strip()
                        if room_id_clean.isdigit():
                            self.target_pages.append(p)

                            # 提取页面标题 → 主播名映射
                            try:
                                page_title = p.title()
                                match = re.search(r'^(.+?)的抖音直播间', page_title)
                                if match:
                                    host = match.group(1).strip()
                                else:
                                    host = page_title.split(' - ')[0].strip()
                            except Exception:
                                host = None

                            self.room_info[room_id_clean] = host
                            print(f"[INFO] 已接管: room_id={room_id_clean} | 主播={host} | {page_title}")

            if not self.target_pages:
                print("[ERROR] 目标丢失：未找到活跃的直播间页面，请确认你停留在至少一个正在直播的房间里。")
                return False

            # 保持兼容性：self.page 指向第一个目标页面
            self.page = self.target_pages[0]
            self.host_name = self.room_info.get(
                urlparse(self.page.url).path.strip('/').split('/')[-1]
            ) if self.target_pages else None

            print(f"[INFO] 多路接管完成：共 {len(self.target_pages)} 个直播间")
            return True

        except Exception as e:
            print(f"[ERROR] 通信链路断开，请检查 Chrome 的 9222 端口是否开启。详细信息: {e}")
            return False
            
    def run(self) -> str:
        """
        启动采集主循环。
        返回值:
          'ok'            — 正常采集后所有页面关闭，应休眠蹲守
          'no_rooms'      — 未找到直播间，疑似主播下播
          'connection_fail' — 浏览器 9222 端口无法连接
          'interrupted'   — 用户 Ctrl+C
        """
        if not self.connect_browser():
            # 区分"无直播间"与"浏览器连不上"
            return 'connection_fail'

        exit_reason = 'ok'

        try:
            # 建立跨层数据通道：expose_function 是最稳定、直接的方法
            def handle_data(data):
                # 多房间模式：JS 已注入 room_id，据此查找对应的 host_name
                room_id = data.get('room_id', '')
                host = self.room_info.get(room_id) if room_id else None
                if host:
                    data['host_name'] = host
                elif self.host_name:
                    data['host_name'] = self.host_name
                self.dispatcher.dispatch(data)

            # 为每一个目标页面独立安装通道并下发雷达引擎
            for pg in self.target_pages:
                try:
                    pg.expose_function('reportData', handle_data)
                    pg.evaluate(INJECTED_JS)
                    print(f"[INFO] 引擎已注入: {pg.title()}")
                except Exception as e:
                    print(f"[WARN] 页面注入失败: {pg.url} — {e}")

            print(f"[INFO] 多路并发矩阵已就绪，共 {len(self.target_pages)} 路数据流")

            # Anti-AFK 心跳计时器初始化（随机首触发间隔，打散节奏）
            self._last_heartbeat = time.time()
            self._next_heartbeat_interval = random.randint(AFK_HEARTBEAT_MIN, AFK_HEARTBEAT_MAX)
            print(f"[AFK] 防休眠心跳已激活，间隔 {self._next_heartbeat_interval}s "
                  f"({self._next_heartbeat_interval // 60} 分钟)")

            # 维持生命周期，轮询检查所有页面的存活状态 + Anti-AFK 心跳
            while watchdog_running:
                try:
                    # ---- 页面存活检查 ----
                    all_dead = True
                    for pg in self.target_pages:
                        try:
                            pg.wait_for_timeout(500)
                            all_dead = False
                        except Exception:
                            pass  # 该页面已关闭

                    if all_dead:
                        print("\n[INFO] 所有直播间页面均已关闭，退出监控。")
                        break

                    # ---- Anti-AFK 心跳：非阻塞式幽灵操作 ----
                    now = time.time()
                    if now - self._last_heartbeat >= self._next_heartbeat_interval:
                        for pg in self.target_pages:
                            try:
                                self._anti_afk_heartbeat(pg)
                            except Exception:
                                pass  # 单页面心跳失败不影响其他页面
                        self._last_heartbeat = now
                        self._next_heartbeat_interval = random.randint(
                            AFK_HEARTBEAT_MIN, AFK_HEARTBEAT_MAX
                        )

                except Exception as wait_e:
                    print(f"\n[INFO] 浏览器连接已结束或断开。")
                    break

        except KeyboardInterrupt:
            exit_reason = 'interrupted'
        finally:
            # 彻底释放 Playwright 资源，防止内存泄漏
            try:
                if self.page:
                    self.page = None
            except Exception:
                pass
            try:
                if self.browser:
                    self.browser = None
            except Exception:
                pass
            if self.playwright:
                try:
                    self.playwright.stop()
                except Exception:
                    pass
                self.playwright = None

        return exit_reason

# ============================================
# 入口
# ============================================
def main():
    global redis_client, watchdog_running

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("=" * 60)
    print("       抖音直播全景监控系统 (不死鸟守护版) v6.0")
    print("=" * 60)
    print("[WATCHDOG] 守护进程已启动，采集引擎将永不崩溃...")

    # Redis 分发器全局复用，避免反复创建连接池
    dispatcher = DataDispatcher(REDIS_HOST, REDIS_PORT, REDIS_DB)
    redis_client = dispatcher.client

    consecutive_failures = 0

    while watchdog_running:
        spider = DouyinLiveSpider(dispatcher)
        try:
            exit_reason = spider.run()
        except Exception as e:
            print(f"[WATCHDOG] 蜘蛛进程抛出未捕获异常: {type(e).__name__}: {e}")
            exit_reason = 'crash'

        if not watchdog_running:
            break

        if exit_reason == 'interrupted':
            break

        consecutive_failures += 1

        # 休眠策略：
        # - 正常采集结束（页面关闭/下播）：固定 5 分钟蹲守
        # - 浏览器连接失败 / 异常崩溃：指数退避，上限 5 分钟
        if exit_reason in ('ok', 'all_closed'):
            wait_seconds = 300
            print(f"\n{'='*60}")
            print(f"[INFO] 主播疑似下播或直播间页面关闭，进入休眠蹲守模式...")
            print(f"[INFO] 将在 {wait_seconds}s（{wait_seconds//60} 分钟）后自动重连刷新。")
            print(f"{'='*60}")
        else:
            wait_seconds = min(consecutive_failures * 30, 300)
            print(f"\n[WATCHDOG] 异常退出 (原因: {exit_reason})，第 {consecutive_failures} 次重试")
            print(f"[WATCHDOG] 等待 {wait_seconds}s 后重启采集引擎...")

        # 分段 sleep，每 10 秒检查一次退出标志，保证 Ctrl+C 能及时响应
        for _ in range(wait_seconds // 10):
            if not watchdog_running:
                break
            time.sleep(10)
        # 处理不足 10s 的余数
        remainder = wait_seconds % 10
        if remainder > 0 and watchdog_running:
            time.sleep(remainder)

    print("\n[WATCHDOG] 不死鸟守护进程已安全退出。")

if __name__ == '__main__':
    main()