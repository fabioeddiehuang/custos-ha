#!/usr/bin/env sh
# ---------------------------------------------------------------------------
# Custos —— 一键构建 / 部署
#
# 版本单一来源：.env 的 VERSION=（升版只改这一处，不要再维护 app/VERSION）。
#   docker-compose.yml 不写死版本号，构建/部署统一用 ./deploy.sh 调用
#   docker compose（它自动加载 .env 取得 VERSION）。
#
# 用法：
#   ./deploy.sh                 等价于 docker compose up -d --build
#   ./deploy.sh logs -f         参数原样透传给 docker compose
#   ./deploy.sh down            同上
#   ./deploy.sh ps              同上
#
# 说明：
#   直接执行 docker compose 也可以，但 .env 里的 VERSION 必须已设置；
#   本脚本仅做版本校验后透传参数，避免「标签旧、内容新」错配。
# ---------------------------------------------------------------------------
set -eu

cd "$(dirname "$0")"

ENV_FILE=".env"

if [ ! -f "$ENV_FILE" ]; then
  echo "错误：找不到 $ENV_FILE，请先创建并写入 VERSION=vX.Y.Z" >&2
  exit 1
fi

# 唯一版本来源：.env 的 VERSION=（去空白，含 Windows 换行符 \r）
VERSION="$(grep -E '^VERSION=' "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2 | tr -d ' \t\r\n')"
if [ -z "$VERSION" ]; then
  echo "错误：$ENV_FILE 中未找到 VERSION=，请先设置版本号（升版只改 .env 这一处）。" >&2
  exit 1
fi

export VERSION
echo "==> 版本 $VERSION（来源 $ENV_FILE）"

# 无参数时默认「构建并后台启动」
if [ "$#" -eq 0 ]; then
  set -- up -d --build
fi

exec docker compose "$@"
