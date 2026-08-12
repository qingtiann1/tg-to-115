# tg-to-115

> Telegram 群组视频自动下载 → 115网盘备份 → 目标群组转发

Docker 一键部署，单进程，稳定可靠。

## 做什么

```
TG 源群 (新视频)
    │
    ├─ 策略1 (forward): 批量转发到目标群 (快，不上传115)
    │
    └─ 策略2 (upload):  下载 → 上传115 → 转发到目标群
                              ↑
                         p115client 直连
                         (秒传 + 大文件分块)
```

## 快速开始

### 1. 准备文件

```bash
git clone https://github.com/qingtiann1/tg-to-115.git
cd tg-to-115
cp .env.example .env
```

### 2. 填写配置

编辑 `.env`，填入你的值（API_ID 和 API_HASH 已有默认值，其余按需修改）：

```bash
TG_API_ID=30431350
TG_API_HASH=dd31870e60686ad7b7fd01b2ac544259
TG_DEST_GROUP=-1004420616732    # 转发目标群 (TGdown)
P115_TARGET_DIR=/beifen         # 115 上传目录
CHECK_INTERVAL=1800             # 扫描间隔 (秒)
```

### 3. 准备115 Cookie

把 115 的 Cookie 保存到 `./config/115-cookies.txt`：

```
UID=...; CID=...; SEID=...; KID=...
```

> 获取方式: 浏览器登录 115.com → F12 → Application → Cookies → 复制这四个值

### 4. 配置源群

编辑 `./config/sources.json`，添加要监控的群组：

```json
[
  {
    "name": "zuoai_caobi",
    "source": "zuoai_caobi",
    "method": "forward",
    "enabled": true,
    "mode": "watch"
  },
  {
    "name": "某个禁止转发的群",
    "source": -1001234567890,
    "method": "upload",
    "enabled": true,
    "mode": "watch",
    "upload_to_115": true
  }
]
```

字段说明：

| 字段 | 说明 |
|------|------|
| `name` | 群组标识 (用于日志和进度文件) |
| `source` | 群组 ID (负数) 或公开用户名 |
| `method` | `"forward"` 批量转发 / `"upload"` 下载后上传 |
| `enabled` | 是否启用 |
| `mode` | `"watch"` 持续监控 / `"once"` 一次性 |
| `upload_to_115` | 是否上传 115 (仅 upload 模式有效) |
| `skip_photos` | 跳过图片 |
| `min_video_mb` | 最小视频大小 (MB) |
| `extra_skip_words` | 额外垃圾过滤词 |

### 5. 启动

```bash
docker compose up -d
```

### 6. 首次登录

查看日志，如果有扫码登录提示：

```bash
docker logs -f tg-to-115
```

按照提示完成 Telegram 登录（可能需要扫码或输入验证码）。Session 会保存到 `./config/` 目录，重启后自动登录。

## 换 NAS 迁移

```bash
# 新 NAS 上
git clone https://github.com/qingtiann1/tg-to-115.git
cd tg-to-115
# 把旧 NAS 的 config/ 目录拷贝过来 (session + sources + cookies + progress)
scp -r old-nas:/path/to/tg-to-115/config/ .
# 编辑 .env 如有必要
vim .env
# 启动
docker compose up -d
```

## 通知

脚本启动时和每天早上 8-10 点会发送汇总通知到你的 Telegram Saved Messages（或 `.env` 中 `NOTIFY_CHAT_ID` 指定的位置）。

## 日志

```bash
docker logs -f tg-to-115        # 实时日志
docker logs --tail 50 tg-to-115 # 最近 50 行
```

## 目录结构

```
tg-to-115/
├── tg_to_115.py          # 核心脚本
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── config/               # ← 持久化 (挂载卷)
│   ├── sources.json      #   源群配置
│   ├── tg_to_115.session #   Pyrogram session
│   ├── 115-cookies.txt   #   115 Cookie
│   ├── progress_*.json   #   各群进度
│   └── downloaded_ids.txt #  去重ID
└── downloads/            # ← 下载文件 (可选)
```

## 内置垃圾过滤

自动跳过：
- 黑名单关键词 (广告、清粉、引流等)
- 图片+链接 (引流广告)
- 纯文本短消息+链接
- Sticker/GIF
- 短视频 (< 10秒，可配置)
- 小文件 (< 3MB，可配置)

仅清洗 URL 链接，不删除正常描述文字。

## 去重

- `downloaded_ids.txt` 持久化 `file_unique_id`
- 同一文件不会重复上传

## 构建自己的镜像

```bash
docker build -t qingtiann1/tg-to-115:latest .
docker push qingtiann1/tg-to-115:latest
```

## License

MIT
