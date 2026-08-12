#!/usr/bin/env python3
"""
tg-to-115 — Telegram 群组视频 → 115网盘备份 + TGdown转发
============================================================
单进程 Docker 容器，稳定可靠。

功能:
  - 监控 TG 群组/频道新消息，自动下载视频
  - 上传 115 网盘 (p115client 直连，自动秒传+分块上传)
  - 可选转发到目标群组 (TGdown)
  - Telegram 消息通知 (成功/失败/每日汇总)

模式:
  forward: 批量 forward_messages (快，不下载 → 不上传115)
  upload:  逐一 download → upload 115 → send_video 到目标群

部署:
  cp .env.example .env  # 编辑填入实际值
  # 把 115-cookies.txt 放到 ./config/
  # 把 Pyrogram session 放到 ./config/ 或首次运行扫码登录
  docker compose up -d

换 NAS 迁移:
  git clone + docker compose up (config 目录带走即可)
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

from pyrogram import Client
from pyrogram.errors import FloodWait

# ============================================================
# 日志 (stdout，docker logs 可见)
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tg-to-115")

# ============================================================
# 配置 (全部从环境变量读取)
# ============================================================
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_NAME = os.environ.get("TG_SESSION_NAME", "tg_to_115")
DEST_GROUP = int(os.environ.get("TG_DEST_GROUP", "0"))
PROXY_URL = os.environ.get("TG_PROXY", "")
P115_COOKIE_FILE = os.environ.get("P115_COOKIE_FILE", "/root/115-cookies.txt")
P115_TARGET_DIR = os.environ.get("P115_TARGET_DIR", "/beifen")
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "1800"))
NOTIFY_CHAT = os.environ.get("NOTIFY_CHAT_ID", "me")
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/app/config")
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/app/downloads")
TEMP_DIR = os.environ.get("TEMP_DIR", "/app/temp")

SOURCES_FILE = os.path.join(CONFIG_DIR, "sources.json")
DEDUP_FILE = os.path.join(CONFIG_DIR, "downloaded_ids.txt")
HEARTBEAT_FILE = os.path.join(CONFIG_DIR, ".heartbeat")

# 默认垃圾过滤词
SKIP_PHRASES = [
    "一键清理", "清理僵尸粉", "僵尸粉", "清除", "清粉",
    "加群", "进群", "群号", "复制群", "打开群", "频道链接",
    "免费约", "同城约", "私聊", "一对一", "1v1", "1对1",
    "扫码", "关注公众号", "成人站",
    "看片", "免费看", "裸聊", "同城", "交友约",
    "兼职", "赚钱", "日结", "招人",
]
URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)
BARE_DOMAIN_RE = re.compile(r"[a-zA-Z0-9][-a-zA-Z0-9]*\.(?:cc|com|net|org|xyz|top|vip|link)\b", re.IGNORECASE)

# ============================================================
# 工具函数
# ============================================================

def parse_proxy(url: str) -> dict | None:
    """解析代理 URL -> Pyrogram proxy dict"""
    if not url:
        return None
    m = re.match(r"(socks5|http|socks4)://(?:(.+):(.+)@)?(.+):(\d+)", url)
    if not m:
        log.warning(f"Invalid proxy URL: {url}, expected scheme://host:port")
        return None
    return {
        "scheme": m.group(1),
        "hostname": m.group(4),
        "port": int(m.group(5)),
        "username": m.group(2),
        "password": m.group(3),
    }


def load_sources() -> list[dict]:
    """加载源群配置"""
    if not os.path.exists(SOURCES_FILE):
        log.warning(f"sources.json not found at {SOURCES_FILE}, using empty list")
        return []
    with open(SOURCES_FILE) as f:
        return json.load(f)


def save_sources(sources: list[dict]):
    with open(SOURCES_FILE, "w") as f:
        json.dump(sources, f, ensure_ascii=False, indent=2)


def progress_file(name: str) -> str:
    return os.path.join(CONFIG_DIR, f"progress_{name}.json")


def load_progress(name: str) -> dict:
    pf = progress_file(name)
    if os.path.exists(pf):
        with open(pf) as f:
            return json.load(f)
    return {"last_id": 0, "done": 0, "skip": 0, "error": 0, "last_error": ""}


def save_progress(name: str, data: dict):
    with open(progress_file(name), "w") as f:
        json.dump(data, f)


def write_heartbeat():
    """写入心跳文件，供 Docker HEALTHCHECK 使用"""
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            json.dump({"ts": time.time(), "time": str(datetime.now())}, f)
    except Exception:
        pass


def load_dedup_ids() -> set[str]:
    if os.path.exists(DEDUP_FILE):
        with open(DEDUP_FILE) as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_dedup_id(fid: str):
    with open(DEDUP_FILE, "a") as f:
        f.write(f"{fid}\n")


def safe_filename(text: str, max_len: int = 80) -> str:
    """清洗文件名，移除不安全字符"""
    if not text:
        return "untitled"
    s = text.replace("\n", " ").replace("\r", " ")
    s = s.replace("/", "_").replace("\\", "_").replace(":", "_")
    s = s.replace("*", "_").replace("?", "_").replace('"', "_")
    s = s.replace("<", "_").replace(">", "_").replace("|", "_")
    s = " ".join(s.split())[:max_len]
    return s or "untitled"


# ============================================================
# 115 上传
# ============================================================

def check_115_ready() -> bool:
    """检查 115 cookie 和 CLI 是否就绪"""
    if not os.path.exists(P115_COOKIE_FILE):
        log.warning(f"115 cookie file not found: {P115_COOKIE_FILE}")
        return False
    # 检查 115cli 是否可用
    try:
        r = subprocess.run(["115cli", "--help"], capture_output=True, timeout=5)
        return r.returncode == 0
    except FileNotFoundError:
        log.warning("115cli command not found. Install: pip install p115client")
        return False
    except Exception as e:
        log.warning(f"115cli check failed: {e}")
        return False


def upload_to_115(filepath: str, remote_dir: str, filename: str) -> bool:
    """
    上传文件到 115 网盘
    使用 p115client CLI (内置: 秒传 + 大文件分块上传)

    Returns: True if success, False if failed
    """
    remote_path = f"{remote_dir}/{filename}"
    log.info(f"  [115] Uploading: {os.path.basename(filepath)} -> {remote_path}")

    try:
        # 设置 cookie 文件环境变量 (p115client 默认读 ~/115-cookies.txt)
        env = os.environ.copy()
        env["HOME"] = "/root"

        result = subprocess.run(
            ["115cli", "upload", filepath, remote_path],
            capture_output=True,
            text=True,
            timeout=1800,  # 30 min max for large files
            env=env,
        )

        if result.returncode == 0:
            fsize_mb = os.path.getsize(filepath) / 1048576
            log.info(f"  [115] OK ({fsize_mb:.1f}MB)")
            return True
        else:
            err = result.stderr[:200] if result.stderr else "unknown"
            log.error(f"  [115] FAIL: {err}")
            return False

    except subprocess.TimeoutExpired:
        log.error(f"  [115] TIMEOUT (30min)")
        return False
    except Exception as e:
        log.error(f"  [115] ERROR: {e}")
        return False


# ============================================================
# 垃圾过滤
# ============================================================

def is_spam(msg, cfg: dict) -> tuple[bool, str]:
    """
    判断消息是否垃圾/广告
    返回 (是否垃圾, 原因)
    """
    text = (msg.text or msg.caption or "")
    tl = text.lower()

    has_video = bool(
        msg.video
        or (msg.document and "video" in (getattr(msg.document, "mime_type", "") or ""))
    )
    has_photo = bool(msg.photo)
    has_url = bool(URL_RE.search(tl))

    # 使用者配置的黑名单词
    extra_words = cfg.get("extra_skip_words", [])
    all_skip = SKIP_PHRASES + extra_words

    for phrase in all_skip:
        if phrase in text:
            return True, f"skip_word:{phrase}"

    if msg.sticker or msg.animation:
        return True, "sticker/gif"

    # 只有图 + 链接 = 引流广告
    if has_photo and not has_video and has_url:
        return True, "photo+url"

    # 纯文本短消息 + 链接 = 广告
    if not has_video and not has_photo and has_url and len(text) < 300:
        return True, "text+url"

    if msg.forward_from_chat and not has_video and not has_photo:
        return True, "forward_no_media"

    # 视频时长检查
    min_dur = cfg.get("min_duration", 10)
    min_mb = cfg.get("min_video_mb", 3)

    if msg.video:
        dur = getattr(msg.video, "duration", 0) or 0
        if dur < min_dur:
            return True, f"short_video:{dur}s"
        fsize = getattr(msg.video, "file_size", 0) or 0
        if fsize > 0 and fsize < min_mb * 1048576:
            return True, f"small_video:{fsize/1048576:.1f}MB"

    if msg.document:
        mime = getattr(msg.document, "mime_type", "") or ""
        if "video" in mime:
            dur = getattr(msg.document, "duration", 0) or 0
            if dur < min_dur:
                return True, f"short_doc:{dur}s"
            fsize = getattr(msg.document, "file_size", 0) or 0
            if fsize > 0 and fsize < min_mb * 1048576:
                return True, f"small_doc:{fsize/1048576:.1f}MB"

    return False, ""


def is_media(msg) -> bool:
    """是否含有效媒体"""
    if msg.video:
        return True
    if msg.photo:
        return True
    if msg.document:
        mime = getattr(msg.document, "mime_type", "") or ""
        if "video" in mime or "image" in mime:
            return True
    return False


def clean_caption(text: str) -> str:
    """删除 URL 链接，保留文字"""
    if not text:
        return ""
    t = URL_RE.sub("", text)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ============================================================
# 策略1: 批量转发 (forward_messages)
# ============================================================

async def process_forward(client: Client, src, dst, cfg: dict):
    """批量转发模式 (无转发限制的群)"""
    name = cfg["name"]
    prog = load_progress(name)
    last_id = prog["last_id"]

    log.info(f"[{name}] Scanning (forward mode)...")

    # 获取全部消息 (倒序)
    all_msgs = []
    async for msg in client.get_chat_history(src.id):
        all_msgs.append(msg)
    all_msgs.reverse()

    # 过滤
    valid_ids = []
    spam_count = {}
    for msg in all_msgs:
        spam, reason = is_spam(msg, cfg)
        if spam:
            spam_count[reason] = spam_count.get(reason, 0) + 1
            continue
        if is_media(msg):
            valid_ids.append(msg.id)

    pending = [mid for mid in valid_ids if mid > last_id]

    log.info(f"[{name}] Total:{len(all_msgs)} Spam:{sum(spam_count.values())} "
             f"Valid:{len(valid_ids)} Pending:{len(pending)}")

    if not pending:
        return prog

    # 批量转发
    batch_size = cfg.get("batch_size", 10)
    delay = cfg.get("batch_delay", 10)

    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        try:
            await client.forward_messages(dst.id, src.id, batch)
            prog["done"] += len(batch)
            prog["last_id"] = batch[-1]
            save_progress(name, prog)
            log.info(f"[{name}] Forwarded {i+len(batch)}/{len(pending)}")
        except FloodWait as e:
            wait = e.value + 5
            log.warning(f"[{name}] FloodWait {wait}s, sleeping...")
            save_progress(name, prog)
            await asyncio.sleep(wait)
            try:
                await client.forward_messages(dst.id, src.id, batch)
                prog["done"] += len(batch)
                prog["last_id"] = batch[-1]
                save_progress(name, prog)
            except Exception as e2:
                prog["error"] += len(batch)
                prog["last_error"] = str(e2)[:200]
                save_progress(name, prog)
        except Exception as e:
            prog["error"] += len(batch)
            prog["last_error"] = str(e)[:200]
            save_progress(name, prog)
            log.error(f"[{name}] Forward error: {e}")

        await asyncio.sleep(delay)

    save_progress(name, prog)
    return prog


# ============================================================
# 策略2: 下载→上传115→转发TGdown
# ============================================================

async def process_upload(client: Client, src, dst, cfg: dict):
    """下载上传模式 (禁止转发的群 / 需要115备份)"""
    name = cfg["name"]
    skip_photos = cfg.get("skip_photos", True)
    enable_115 = cfg.get("upload_to_115", True)

    prog = load_progress(name)
    last_id = prog["last_id"]

    log.info(f"[{name}] Scanning (upload mode)...")

    # 获取全部消息
    all_msgs = []
    async for msg in client.get_chat_history(src.id):
        all_msgs.append(msg)
    all_msgs.reverse()

    # 过滤 + 标题合并
    items = []
    spam_count = {}
    last_cap_src = 0
    cap_seq = 0
    skipped_short = 0
    skipped_photo = 0

    for i, msg in enumerate(all_msgs):
        spam, reason = is_spam(msg, cfg)
        if spam:
            spam_count[reason] = spam_count.get(reason, 0) + 1
            continue

        if not is_media(msg):
            continue

        if skip_photos and msg.photo and not msg.video:
            skipped_photo += 1
            continue

        # 标题合并: 无标题视频向前查找标题来源
        caption = msg.caption or ""
        source_id = 0

        if not caption and msg.video:
            for j in range(i - 1, max(i - 6, -1), -1):
                prev = all_msgs[j]
                prev_text = (prev.text or prev.caption or "").strip()
                if not prev_text:
                    continue
                # 检查发送者是否相同
                s1 = prev.from_user.id if prev.from_user else None
                s2 = msg.from_user.id if msg.from_user else None
                if not s1 or not s2 or s1 != s2:
                    continue
                td = abs((msg.date - prev.date).total_seconds()) if msg.date and prev.date else 999
                if td <= 60:
                    caption = prev_text
                    source_id = prev.id
                    break

        # 同一标题来源的多段视频，自动编号
        if source_id > 0 and source_id == last_cap_src:
            cap_seq += 1
        else:
            cap_seq = 0
            last_cap_src = source_id

        if cap_seq > 0:
            caption = f"{caption} {cap_seq + 1}"

        caption = clean_caption(caption)
        items.append((msg, caption))

    pending = [(m, c) for m, c in items if m.id > last_id]

    log.info(f"[{name}] Total:{len(all_msgs)} Spam:{sum(spam_count.values())} "
             f"SkipPhoto:{skipped_photo} Items:{len(items)} Pending:{len(pending)}")

    if not pending:
        return prog

    # 确保临时目录存在
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # 检查115是否就绪
    p115_ready = enable_115 and check_115_ready()
    if enable_115 and not p115_ready:
        log.warning(f"[{name}] 115 not ready, skipping 115 upload (will still forward to TG)")

    single_delay = cfg.get("single_delay", 8)

    for idx, (msg, cap) in enumerate(pending):
        display_cap = cap[:80].replace("\n", " ") if cap else "(no caption)"
        log.info(f"[{name}] [{idx+1}/{len(pending)}] msg={msg.id} | {display_cap}")

        tmp_path = os.path.join(TEMP_DIR, f"tg_{msg.id}.mp4")
        success = False

        try:
            # 重新获取消息 (刷新 file reference)
            fresh = await client.get_messages(src.id, msg.id)
            if not fresh:
                prog["skip"] += 1
                prog["last_id"] = msg.id
                save_progress(name, prog)
                continue

            # 下载
            log.info(f"  Downloading...")
            await client.download_media(fresh, file_name=tmp_path)
            fsize = os.path.getsize(tmp_path)
            log.info(f"  Downloaded: {fsize / 1048576:.1f}MB")

            # === 上传115 ===
            if p115_ready:
                src_title = safe_filename(src.title if src.title else str(src.id), max_len=30)
                now = fresh.date or datetime.now()
                remote_dir = f"{P115_TARGET_DIR}/{src_title}/{now.year}-{now.month:02d}"
                safe_name = safe_filename(cap) if cap else f"video_{msg.id}"
                filename = f"{safe_name}.mp4"
                upload_to_115(tmp_path, remote_dir, filename)

            # === 上传到目标群 (TGdown) ===
            log.info(f"  Sending to destination...")
            if fresh.video:
                v = fresh.video
                sent = await client.send_video(
                    dst.id, tmp_path, caption=cap,
                    width=getattr(v, "width", 0) or 0,
                    height=getattr(v, "height", 0) or 0,
                    duration=int(getattr(v, "duration", 0) or 0),
                )
            elif fresh.photo:
                sent = await client.send_photo(dst.id, tmp_path, caption=cap)
            else:
                sent = await client.send_document(dst.id, tmp_path, caption=cap)

            # 写入去重ID
            if sent:
                fid = None
                if sent.video:
                    fid = sent.video.file_unique_id
                elif sent.photo:
                    fid = sent.photo.file_unique_id
                elif sent.document:
                    fid = sent.document.file_unique_id
                if fid:
                    save_dedup_id(fid)

            prog["done"] += 1
            prog["last_id"] = msg.id
            success = True
            log.info(f"  OK")

        except FloodWait as e:
            wait = e.value + 5
            log.warning(f"  FloodWait {wait}s, sleeping...")
            save_progress(name, prog)
            await asyncio.sleep(wait)
            # 重试一次
            try:
                fresh = await client.get_messages(src.id, msg.id)
                if fresh:
                    await client.download_media(fresh, file_name=tmp_path)
                    if p115_ready:
                        src_title = safe_filename(src.title or str(src.id), max_len=30)
                        now = fresh.date or datetime.now()
                        remote_dir = f"{P115_TARGET_DIR}/{src_title}/{now.year}-{now.month:02d}"
                        safe_name = safe_filename(cap) if cap else f"video_{msg.id}"
                        upload_to_115(tmp_path, remote_dir, f"{safe_name}.mp4")
                    if fresh.video:
                        await client.send_video(dst.id, tmp_path, caption=cap,
                                                width=fresh.video.width,
                                                height=fresh.video.height,
                                                duration=fresh.video.duration)
                    else:
                        await client.send_document(dst.id, tmp_path, caption=cap)
                    prog["done"] += 1
                    prog["last_id"] = msg.id
                    success = True
                    log.info(f"  Retry OK")
            except Exception as e2:
                prog["error"] += 1
                prog["last_error"] = str(e2)[:200]
                log.error(f"  Retry FAIL: {e2}")

        except Exception as e:
            prog["error"] += 1
            prog["last_error"] = str(e)[:200]
            log.error(f"  FAIL: {e}")

        finally:
            # 清理临时文件
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        save_progress(name, prog)

        # 成功或失败都延迟，避免触发 Flood
        await asyncio.sleep(single_delay)

    return prog


# ============================================================
# 通知
# ============================================================

async def notify(client: Client, text: str):
    """发送通知消息到 Saved Messages (NOTIFY_CHAT_ID=me) 或指定 chat"""
    try:
        await client.send_message(NOTIFY_CHAT, text)
    except Exception as e:
        log.warning(f"Notify failed: {e}")


async def notify_daily_summary(client: Client):
    """发送每日汇总"""
    sources = load_sources()
    lines = ["<b>📊 tg-to-115 每日汇总</b>\n"]
    total_done = 0
    total_pending = 0

    for cfg in sources:
        if not cfg.get("enabled", True):
            continue
        prog = load_progress(cfg["name"])
        done = prog.get("done", 0)
        error = prog.get("error", 0)
        skip = prog.get("skip", 0)
        last_err = prog.get("last_error", "")

        total_done += done
        status = "✅" if done > 0 else "⏳"
        lines.append(f"{status} <b>{cfg['name']}</b>: done={done} err={error} skip={skip}")
        if last_err:
            lines.append(f"  ⚠️ {last_err[:80]}")

    lines.append(f"\n📤 今日处理: <b>{total_done}</b>")
    await notify(client, "\n".join(lines))


# ============================================================
# 内置命令系统 (发消息到 Saved Messages 即可管理)
# ============================================================

_CMD_LAST_ID = 0  # 上次处理的命令消息 ID
_CMD_ID_FILE = os.path.join(CONFIG_DIR, "cmd_last_id.txt")


def load_cmd_last_id() -> int:
    try:
        with open(_CMD_ID_FILE) as f:
            return int(f.read().strip())
    except Exception:
        return 0


def save_cmd_last_id(msg_id: int):
    with open(_CMD_ID_FILE, "w") as f:
        f.write(str(msg_id))


async def check_commands(client: Client):
    """检查 Saved Messages 中的命令并执行"""
    global _CMD_LAST_ID
    if _CMD_LAST_ID == 0:
        _CMD_LAST_ID = load_cmd_last_id()

    try:
        cmd_msgs = []
        async for msg in client.get_chat_history(NOTIFY_CHAT, limit=10):
            if msg.id <= _CMD_LAST_ID:
                break
            if msg.text:
                cmd_msgs.append(msg)

        if not cmd_msgs:
            return

        for msg in reversed(cmd_msgs):
            text = msg.text.strip()
            reply = await handle_command(client, text)

            if reply:
                try:
                    await client.send_message(NOTIFY_CHAT, reply)
                except Exception as e:
                    log.warning(f"Command reply failed: {e}")

            _CMD_LAST_ID = msg.id
            save_cmd_last_id(msg.id)

    except Exception as e:
        log.warning(f"Command check error: {e}")


async def handle_command(client: Client, text: str) -> str | None:
    """解析并执行命令。返回回复文本，若无匹配返回 None"""
    sources = load_sources()
    changed = False

    # --- /115 cookies 设置 ---
    if text.startswith("/115"):
        cookie_text = text[4:].strip()
        if not cookie_text:
            return (
                "📋 <b>设置115 Cookie</b>\n\n"
                "请发送: <code>/115 UID=xxx; CID=xxx; SEID=xxx; KID=xxx</code>\n\n"
                "从浏览器获取: 登录 115.com → F12 → Application → Cookies → 复制四个值"
            )
        try:
            with open(P115_COOKIE_FILE, "w") as f:
                f.write(cookie_text + "\n")
            log.info("115 cookies saved via command")
            return "✅ 115 Cookie 已保存! 下次 upload 任务将自动上传到115网盘。"
        except Exception as e:
            return f"❌ 保存失败: {e}"

    # --- /add 添加源群 ---
    if text.startswith("/add"):
        args = text[5:].strip().split()
        if not args:
            return (
                "📋 <b>添加源群</b>\n\n"
                "<code>/add @username</code> — 添加公开群 (forward模式)\n"
                "<code>/add @username upload</code> — 添加并上传115\n"
                "<code>/add -100123456 群名</code> — 添加私密群 (ID模式)\n"
                "<code>/add @username once</code> — 一次性转存"
            )
        source = args[0]
        method = "forward"
        mode = "watch"
        name = None

        for a in args[1:]:
            if a == "upload":
                method = "upload"
            elif a == "forward":
                method = "forward"
            elif a == "once":
                mode = "once"
            elif a == "watch":
                mode = "watch"
            elif not a.startswith("-"):
                name = a

        # 解析 source: @username 或 -100数字ID
        if source.startswith("@"):
            source = source[1:]  # 去掉 @
        elif source.startswith("-100"):
            pass  # 直接用作 ID
        else:
            return f"❌ 无法识别的群组标识: {source}\n请使用 @username 或 -100 开头的数字ID"

        if not name:
            name = "bot_" + source.replace("/", "_")[:30]

        # 检查重复
        for s in sources:
            if s["name"] == name:
                return f"⚠️ <b>{name}</b> 已存在"

        sources.append({
            "name": name,
            "source": source,
            "method": method,
            "enabled": True,
            "mode": mode,
            "skip_photos": method == "upload",
            "min_video_mb": 5,
            "upload_to_115": method == "upload",
            "batch_size": 10,
            "batch_delay": 10,
            "single_delay": 8,
            "extra_skip_words": [],
        })
        changed = True

        m115 = "☁️上传115" if method == "upload" else "📤仅转发"
        m_mode = "👁️持续监控" if mode == "watch" else "📦一次性"
        return f"✅ 已添加: <b>{name}</b>\n源: <code>{source}</code>\n模式: {m_mode} | {m115}"

    # --- /list 列表 ---
    if text.startswith("/list"):
        if not sources:
            return "📋 暂无源群。用 /add 添加。"
        lines = ["<b>📋 源群列表</b>\n"]
        for s in sources:
            icon = "🟢" if s.get("enabled") else "🔴"
            done = " ✅" if s.get("complete") else ""
            m = "⬆️upload" if s.get("method") == "upload" else "📤fwd"
            prog = load_progress(s["name"])
            lines.append(
                f"{icon} <b>{s['name']}</b> [{m}] done={prog.get('done',0)}{done}"
            )
        return "\n".join(lines)

    # --- /rm 删除 ---
    if text.startswith("/rm"):
        name = text[4:].strip()
        if not name:
            return "用法: <code>/rm 群组名</code>"
        new_sources = [s for s in sources if s["name"] != name]
        if len(new_sources) == len(sources):
            return f"❌ 未找到: <b>{name}</b>"
        sources[:] = new_sources
        changed = True
        # 删除进度文件
        pf = progress_file(name)
        if os.path.exists(pf):
            os.remove(pf)
        return f"🗑️ 已删除: <b>{name}</b>"

    # --- /on 启用 ---
    if text.startswith("/on"):
        name = text[4:].strip()
        for s in sources:
            if s["name"] == name:
                s["enabled"] = True
                s["complete"] = False
                changed = True
                return f"🟢 已启用: <b>{name}</b>"
        return f"❌ 未找到: <b>{name}</b>"

    # --- /off 禁用 ---
    if text.startswith("/off"):
        name = text[5:].strip()
        for s in sources:
            if s["name"] == name:
                s["enabled"] = False
                changed = True
                return f"🔴 已禁用: <b>{name}</b>"
        return f"❌ 未找到: <b>{name}</b>"

    # --- /method 改模式 ---
    if text.startswith("/method"):
        parts = text[8:].strip().split()
        if len(parts) < 2:
            return "用法: <code>/method 群组名 forward|upload</code>"
        name, method = parts[0], parts[1]
        if method not in ("forward", "upload"):
            return f"❌ 无效模式: {method} (应为 forward 或 upload)"
        for s in sources:
            if s["name"] == name:
                s["method"] = method
                s["upload_to_115"] = method == "upload"
                s["skip_photos"] = method == "upload"
                changed = True
                return f"✅ <b>{name}</b> 模式已改为: {method}"
        return f"❌ 未找到: <b>{name}</b>"

    # --- /status 状态 ---
    if text.startswith("/status"):
        enabled = [s for s in sources if s.get("enabled")]
        lines = [
            "<b>📊 系统状态</b>\n",
            f"目标群: <code>{DEST_GROUP}</code>",
            f"115 Cookie: {'✅' if os.path.exists(P115_COOKIE_FILE) else '❌ 未配置'}",
            f"源群: {len(sources)} 个 ({len(enabled)} 启用)",
            f"扫描间隔: {CHECK_INTERVAL}s",
        ]
        for s in enabled:
            prog = load_progress(s["name"])
            lines.append(
                f"  🟢 {s['name']}: {s['method']} done={prog.get('done',0)} "
                f"last_id={prog.get('last_id',0)}"
            )
        return "\n".join(lines)

    # --- /help ---
    if text.startswith("/help"):
        return (
            "<b>📡 tg-to-115 命令</b>\n\n"
            "<code>/115 UID=xxx; CID=xxx; ...</code> — 设置115 Cookie\n"
            "<code>/add @群组 [upload] [once]</code> — 添加源群\n"
            "<code>/list</code> — 查看所有源群\n"
            "<code>/rm 群组名</code> — 删除源群\n"
            "<code>/on 群组名</code> — 启用\n"
            "<code>/off 群组名</code> — 禁用\n"
            "<code>/method 群组名 forward|upload</code> — 改模式\n"
            "<code>/status</code> — 系统状态\n"
            "<code>/help</code> — 帮助"
        )

    # 保存更改
    if changed:
        save_sources(sources)

    return None  # 非命令消息，不回复


# ============================================================
# 主循环
# ============================================================

_last_summary_date = ""


async def run_once(client: Client, sources: list[dict]):
    """执行一轮: 处理所有启用的源群"""
    global _last_summary_date

    if DEST_GROUP == 0:
        log.error("TG_DEST_GROUP not set, cannot forward!")
        return

    try:
        dst = await client.get_chat(DEST_GROUP)
    except Exception as e:
        log.error(f"Cannot access destination group {DEST_GROUP}: {e}")
        return

    for cfg in sources:
        if not cfg.get("enabled", True):
            continue

        name = cfg["name"]
        method = cfg.get("method", "forward")

        log.info(f"{'='*50}")
        log.info(f"[{name}] START (method={method})")

        try:
            src = await client.get_chat(cfg["source"])
            log.info(f"[{name}] Source: {src.title} (id={src.id})")
        except Exception as e:
            log.error(f"[{name}] Cannot resolve source '{cfg['source']}': {e}")
            continue

        try:
            if method == "forward":
                prog = await process_forward(client, src, dst, cfg)
            elif method == "upload":
                prog = await process_upload(client, src, dst, cfg)
            else:
                log.warning(f"[{name}] Unknown method: {method}")
                continue
        except Exception as e:
            log.error(f"[{name}] Processing error: {e}")
            continue

        log.info(f"[{name}] DONE: done={prog['done']} skip={prog['skip']} error={prog['error']}")

        # 标记一次性任务完成
        if cfg.get("mode") == "once" and prog.get("last_id", 0) > 0:
            # 检查是否还有待处理
            all_msgs = []
            async for msg in client.get_chat_history(src.id, limit=1):
                all_msgs.append(msg)
            if all_msgs and all_msgs[0].id <= prog["last_id"]:
                cfg["complete"] = True
                save_sources(sources)
                await notify(client, f"✅ [{name}] 一次性任务完成! done={prog['done']}")

    # 每日汇总 (仅每天发一次)
    today = datetime.now().strftime("%Y-%m-%d")
    if today != _last_summary_date:
        _last_summary_date = today
        # 延迟到下次循环再发，避免频繁
        try:
            hour = datetime.now().hour
            if 8 <= hour <= 10:  # 早上 8-10 点发
                await notify_daily_summary(client)
        except Exception:
            pass


async def main():
    log.info("=" * 50)
    log.info("tg-to-115 starting...")
    log.info(f"Config dir: {CONFIG_DIR}")
    log.info(f"Temp dir: {TEMP_DIR}")
    log.info(f"Check interval: {CHECK_INTERVAL}s")
    log.info(f"Dest group: {DEST_GROUP}")

    # 确保目录存在
    os.makedirs(CONFIG_DIR, exist_ok=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    # 创建默认 sources.json (如果没有)
    if not os.path.exists(SOURCES_FILE):
        default_sources = [
            {
                "name": "zuoai_caobi",
                "source": "zuoai_caobi",
                "method": "forward",
                "enabled": True,
                "mode": "watch",
                "skip_photos": False,
                "min_video_mb": 5,
                "batch_size": 10,
                "batch_delay": 10,
                "extra_skip_words": [],
            },
            {
                "name": "old_source",
                "source": -1003945743438,
                "method": "upload",
                "enabled": False,
                "mode": "watch",
                "skip_photos": True,
                "min_video_mb": 5,
                "upload_to_115": True,
                "single_delay": 8,
                "extra_skip_words": [],
            },
        ]
        with open(SOURCES_FILE, "w") as f:
            json.dump(default_sources, f, ensure_ascii=False, indent=2)
        log.info(f"Created default sources.json")

    # 修复 session 数据库 (从旧容器复制时可能有 WAL 锁)
    _session_file = os.path.join(CONFIG_DIR, f"{SESSION_NAME}.session")
    if os.path.exists(_session_file):
        try:
            import sqlite3
            _conn = sqlite3.connect(_session_file)
            _conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            _conn.close()
            log.info("Session database checkpointed")
        except Exception as _e:
            log.warning(f"Session checkpoint failed (non-fatal): {_e}")

    # 构建 Pyrogram client
    proxy = parse_proxy(PROXY_URL)
    session_path = os.path.join(CONFIG_DIR, SESSION_NAME)

    client = Client(
        session_path,
        api_id=API_ID,
        api_hash=API_HASH,
        proxy=proxy,
        workdir=CONFIG_DIR,
    )

    await client.start()
    me = await client.get_me()
    log.info(f"Telegram login: {me.first_name} (@{me.username})")

    # 启动通知
    await notify(client, f"🟢 tg-to-115 已启动\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 主循环
    while True:
        write_heartbeat()

        try:
            # 先检查用户命令
            await check_commands(client)

            sources = load_sources()

            if not sources:
                log.warning("No sources configured. Add groups to config/sources.json")
            else:
                await run_once(client, sources)

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)
            try:
                await notify(client, f"⚠️ tg-to-115 错误: {str(e)[:200]}")
            except Exception:
                pass

        write_heartbeat()

        # 等待下一轮
        log.info(f"Sleeping {CHECK_INTERVAL}s until next check...")
        await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Received SIGINT, shutting down...")
    except Exception as e:
        log.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)
