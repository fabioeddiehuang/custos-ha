#!/usr/bin/env sh
# ---------------------------------------------------------------------------
# Custos —— 一键构建 / 部署
#
# 版本单一来源：app/VERSION
#   每次执行都会把 app/VERSION 同步写入 .env，再调用 docker compose，
#   因此 docker-compose.yml 里不需要（也不应该）手写版本号，
#   升版时只需改 app/VERSION，compose 文件只在改动其它内容时才动。
#
# 用法：
#   ./deploy.sh                 等价于 docker compose up -d --build
#   ./deploy.sh logs -f         参数原样透传给 docker compose
#   ./deploy.sh down            同上
#   ./deploy.sh ps              同上
#
# 说明：
#   直接执行 docker compose 也可以，但 .env 里的 VERSION 必须是最新值；
#   若 .env 与 app/VERSION 不一致，Dockerfile 会主动构建失败并提示改用本脚本。
# ---------------------------------------------------------------------------
set -eu

cd "$(dirname "$0")"

VERSION_FILE="app/VERSION"
ENV_FILE=".env"

if [ ! -f "$VERSION_FILE" ]; then
  echo "错误：找不到版本文件 $VERSION_FILE" >&2
  exit 1
fi

# 去掉所有空白字符（含 Windows 换行符 \r），避免读到 "v2.1.5\r" 这类脏值
VERSION="$(tr -d ' \t\r\n' < "$VERSION_FILE")"
if [ -z "$VERSION" ]; then
  echo "错误：$VERSION_FILE 内容为空" >&2
  exit 1
fi

# 同步到 .env：已有则替换 VERSION= 行，没有则追加，文件不存在则创建。
# 用临时文件中转，规避 sed -i 在 GNU/BSD 上的语法差异。
if [ -f "$ENV_FILE" ]; then
  grep -v '^VERSION=' "$ENV_FILE" > "$ENV_FILE.tmp" || true
  printf 'VERSION=%s\n' "$VERSION" >> "$ENV_FILE.tmp"
  mv "$ENV_FILE.tmp" "$ENV_FILE"
else
  printf 'VERSION=%s\n' "$VERSION" > "$ENV_FILE"
fi

export VERSION
echo "==> 版本 $VERSION（来源 $VERSION_FILE，已同步至 $ENV_FILE）"

# 无参数时默认「构建并后台启动」
if [ "$#" -eq 0 ]; then
  set -- up -d --build
fi

exec docker compose "$@"
