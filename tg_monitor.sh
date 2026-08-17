#!/bin/bash
# tg-to-115 走 sing-box 稳定性监控（每10分钟一次，root 运行）
LOG=/vol1/1000/docker/tg-to-115/monitor_$(date +%Y%m%d).log
TS=$(date '+%Y-%m-%d %H:%M:%S')

# 容器状态
STATUS=$(docker ps --format "{{.Names}}:{{.Status}}" 2>/dev/null | grep -E "tg-to-115|sing-box|mihomo" | tr '\n' '; ')

# 心跳落后秒数（判断是否卡死；取不到就记 NA）
HB=$(python3 -c "import json,time;d=json.load(open('/vol1/1000/docker/tg-to-115/config/.heartbeat'));print(round(time.time()-d['ts']))" 2>/dev/null || echo NA)

# 最近10分钟连接错误数
ERRS=$(docker logs --since 10m tg-to-115 2>&1 | grep -cE "ConnectionResetError|BrokenPipeError|read\(\) called")

# 上传进度（最近日志里的 已传/待传）
PROG=$(docker logs --tail 100 tg-to-115 2>&1 | grep -oE "待回传: [0-9]+ 个文件 \(已传 [0-9]+\)" | tail -1)

# sing-box 是否正常转发（最近是否有 outbound 记录）
SB=$(docker logs --since 10m sing-box 2>&1 | grep -c "outbound/vless")

echo "$TS | 容器[$STATUS] | 心跳落后:${HB}s | 10min连接错误:$ERRS | $PROG | singbox转发:$SB" >> "$LOG"
