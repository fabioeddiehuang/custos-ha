#!/bin/sh
# ---------------------------------------------------------------------------
# Custos 服务端卸载（仅清理【管理器】，默认绝不动被管节点与业务数据）。
#
# 用法：sudo custosctl uninstall [--purge] [--yes]
#   （默认）  停止并移除容器；保留镜像、backups/、data/、generated_nodes/；
#             并删除 /usr/bin/custosctl 与 /etc/custos
#   --purge   额外删除本项目镜像（custos-server:<VERSION>）与匿名卷
#   --yes     跳过交互确认（脚本化用）
#
# ⚠ 被管节点（MySQL/MariaDB + keepalived + 业务数据）不在本脚本范围内：
#    那上面的数据库是业务库，删除不可逆。需要清理节点请到各节点执行
#    generated_nodes/<ip>/Uninstall-*.sh（或节点目录下的卸载脚本）。
# ---------------------------------------------------------------------------
set -eu

CONF=${CUSTOS_CONF:-/etc/custos/custosctl.env}
[ -r "$CONF" ] && . "$CONF" || true
if [ -z "${CUSTOS_HOME:-}" ]; then
  SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
  CUSTOS_HOME=$(cd "$SCRIPT_DIR/.." && pwd)
fi
[ -f "$CUSTOS_HOME/docker-compose.yml" ] || {
  echo "错误：$CUSTOS_HOME 不是 Custos 项目目录。" >&2; exit 1; }

PURGE=0
ASSUME_YES=0
for a in "$@"; do
  case "$a" in
    --purge) PURGE=1 ;;
    --yes|-y) ASSUME_YES=1 ;;
    *) echo "未知参数：$a（支持 --purge / --yes）" >&2; exit 1 ;;
  esac
done

if [ "$(id -u)" != "0" ]; then
  echo "错误：卸载需要 root，请用 sudo 执行。" >&2
  exit 1
fi

cd "$CUSTOS_HOME"
echo "即将卸载 Custos 管理器"
echo "  项目目录：$CUSTOS_HOME"
echo "  范围     ：停止并移除容器${PURGE:+ + 删除本项目镜像与匿名卷}"
echo "  保留     ：backups/ data/ generated_nodes/（业务备份与节点配置不会被删）"
echo "  不动     ：被管节点上的数据库与 keepalived"
if [ "$ASSUME_YES" != "1" ]; then
  printf '确认继续？请输入 yes： '
  read -r ans
  [ "$ans" = "yes" ] || { echo "已取消。"; exit 0; }
fi

# 1) 停并移除容器（--purge 时连镜像与匿名卷一起清）
if [ "$PURGE" = "1" ]; then
  docker compose down --rmi local --volumes || true
else
  docker compose down || true
fi

# 2) 删除命令行工具与配置
rm -f /usr/bin/custosctl
rm -rf /etc/custos

echo "==> 卸载完成：custos-server 容器已移除，custosctl 已删除。"
echo "    数据仍在：$CUSTOS_HOME（backups/ data/ generated_nodes/ 未动）"
echo "    若需清理被管节点，请到各节点执行对应的 Uninstall 脚本（谨慎：会删业务库）。"
