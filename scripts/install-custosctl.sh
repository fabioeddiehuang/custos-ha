#!/bin/sh
# ---------------------------------------------------------------------------
# 安装 custosctl 到 /usr/bin，并记录项目目录到 /etc/custos/custosctl.env。
# 用法：sudo ./scripts/install-custosctl.sh   （或 sudo custosctl install）
# 卸载：sudo custosctl uninstall [--purge]    （会一并删除 /usr/bin/custosctl）
# ---------------------------------------------------------------------------
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CUSTOS_HOME=${CUSTOS_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}
CONF_DIR=/etc/custos
CONF=$CONF_DIR/custosctl.env
TARGET=/usr/bin/custosctl

if [ "$(id -u)" != "0" ]; then
  echo "错误：安装需要 root，请用 sudo 执行。" >&2
  exit 1
fi

if [ ! -f "$CUSTOS_HOME/custosctl" ]; then
  echo "错误：$CUSTOS_HOME/custosctl 不存在。" >&2
  exit 1
fi
if [ ! -f "$CUSTOS_HOME/docker-compose.yml" ]; then
  echo "错误：$CUSTOS_HOME 不是 Custos 项目目录（缺少 docker-compose.yml）。" >&2
  exit 1
fi

mkdir -p "$CONF_DIR"
printf '# custosctl 配置：Custos 项目目录（docker compose 上下文）\nCUSTOS_HOME=%s\n' "$CUSTOS_HOME" > "$CONF"
chmod 644 "$CONF"

install -m 0755 "$CUSTOS_HOME/custosctl" "$TARGET"

echo "==> 已安装 $TARGET"
echo "    项目目录：$CUSTOS_HOME（记录在 $CONF）"
echo "    用法：custosctl help"
echo "    卸载：custosctl uninstall"
