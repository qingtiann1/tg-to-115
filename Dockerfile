FROM python:3.11-slim

WORKDIR /app

# 系统依赖 (ca-certificates for HTTPS)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 应用代码
COPY tg_to_115.py .

# 运行时目录
RUN mkdir -p /app/config /app/downloads /app/temp

VOLUME ["/app/config", "/app/downloads"]

ENV CONFIG_DIR=/app/config \
    DOWNLOAD_DIR=/app/downloads \
    TEMP_DIR=/app/temp \
    CHECK_INTERVAL=1800 \
    PYTHONUNBUFFERED=1

HEALTHCHECK --interval=300s --timeout=10s --retries=3 \
    CMD python3 -c "import json,os,time;f=open(os.environ.get('CONFIG_DIR','/app/config')+'/.heartbeat');d=json.load(f);exit(0 if time.time()-d['ts']<600 else 1)"

CMD ["python", "-u", "tg_to_115.py"]
