#!/bin/bash
# 容器启动脚本：等待管理库 -> 初始化库表 -> 后台启动监控 -> 前台启动 Web
set -e

echo "[entrypoint] 等待管理 MySQL (${MGMT_MYSQL_HOST}:${MGMT_MYSQL_PORT}) 可用..."
READY=0
for i in $(seq 1 10); do
  if python - <<'PY'
import pymysql, os
try:
    pymysql.connect(host=os.environ.get('MGMT_MYSQL_HOST','127.0.0.1'),
                    port=int(os.environ.get('MGMT_MYSQL_PORT',3306)),
                    user=os.environ.get('MGMT_MYSQL_USER','root'),
                    password=os.environ.get('MGMT_MYSQL_PASS',''),
                    connect_timeout=3)
except Exception:
    raise SystemExit(1)
PY
  then
    READY=1
    break
  else
    echo "  重试 $i/10 ..."
    sleep 2
  fi
done
if [ "$READY" -ne 1 ]; then
  echo "[entrypoint] 管理 MySQL 在 10 次重试后仍不可用，退出。请检查 MGMT_MYSQL_* 配置或管理库是否已启动。"
  exit 1
fi

echo "[entrypoint] 初始化库表与管理员账号..."
python -m app.init_db

echo "[entrypoint] 启动后台监控线程..."
python -m app.monitor &

WEB_PORT=${WEB_PORT:-28888}
echo "[entrypoint] 启动 Web 管理后台 (端口 ${WEB_PORT})..."
if [ -n "$SSL_CERT" ] && [ -n "$SSL_KEY" ]; then
  exec uvicorn app.api:app --host 0.0.0.0 --port "$WEB_PORT" --ssl-keyfile "$SSL_KEY" --ssl-certfile "$SSL_CERT"
else
  exec uvicorn app.api:app --host 0.0.0.0 --port "$WEB_PORT"
fi
