#!/bin/bash
# 容器启动脚本：等待管理库 -> 初始化库表 -> 后台启动监控 -> 前台启动 Web
set -e

echo "[entrypoint] 等待管理 MySQL (${MGMT_MYSQL_HOST}:${MGMT_MYSQL_PORT}) 可用..."
READY=0
for i in $(seq 1 10); do
  if python - <<'PY'
import pymysql, os, sys
host = os.environ.get('MGMT_MYSQL_HOST', '127.0.0.1')
port = int(os.environ.get('MGMT_MYSQL_PORT', 3306))
user = os.environ.get('MGMT_MYSQL_USER', 'root')
try:
    pymysql.connect(host=host, port=port, user=user,
                    password=os.environ.get('MGMT_MYSQL_PASS', ''),
                    connect_timeout=3)
except Exception as e:
    # 打印真实错误（2003 连不上 / 1045 鉴权失败 / 1049 库不存在…），避免静默退出无从排障
    print(f"[entrypoint] 连接管理 MySQL {host}:{port} (user={user}) 失败：{e}", file=sys.stderr)
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
  echo "[entrypoint] 管理 MySQL (${MGMT_MYSQL_HOST}:${MGMT_MYSQL_PORT}) 在 10 次重试后仍不可用，退出。"
  echo "[entrypoint] 请检查：① MGMT_MYSQL_HOST 是否为真实管理库主机（独立机，非 127.0.0.1、非集群节点）；"
  echo "[entrypoint] ② 端口/账号/口令/库名是否正确；③ docker 容器内能否路由到该地址（防火墙是否放行）。"
  exit 1
fi

echo "[entrypoint] 初始化库表与管理员账号..."
python -m app.init_db

WEB_PORT=${WEB_PORT:-28888}
echo "[entrypoint] 启动 Web 管理后台 (端口 ${WEB_PORT})..."
echo "[entrypoint] 注：后台监控线程已改为随 Web 进程内常驻（uvicorn 加载 app.api 时自动拉起），无需单独启动。"
if [ -n "$SSL_CERT" ] && [ -n "$SSL_KEY" ]; then
  exec uvicorn app.api:app --host 0.0.0.0 --port "$WEB_PORT" --ssl-keyfile "$SSL_KEY" --ssl-certfile "$SSL_CERT"
else
  exec uvicorn app.api:app --host 0.0.0.0 --port "$WEB_PORT"
fi
