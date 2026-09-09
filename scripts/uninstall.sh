#!/bin/sh
# ---------------------------------------------------------------------------
# Custos 服务端卸载（仅清理【管理器】；默认绝不动被管节点与业务数据）。
#
# 用法：sudo custosctl uninstall [-f|--purge] [--yes]
#
#   默认       停止并移除容器 + 删除本项目镜像（custos-server:<VERSION>）与匿名卷
#              + 删除 /usr/bin/custosctl 与 /etc/custos
#              （保留 backups/ data/ generated_nodes/）
#   -f|--purge 额外【永久删除】backups/ data/（含 data/custos-ha/）generated_nodes/
#              需输入 DELETE 二次确认（--yes 可跳过确认，供脚本化使用）
#   --yes      跳过交互确认
#
# ⚠ 被管节点（MySQL/MariaDB + keepalived + 业务数据）不在本脚本范围内：
#    节点上的库是业务库，删除不可逆。需要清理节点请到各节点执行
#    generated_nodes/<ip>/Uninstall-*.sh（或节点部署目录下的卸载脚本）。
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
    -f|--purge) PURGE=1 ;;
    --yes|-y)   ASSUME_YES=1 ;;
    *) echo "未知参数：$a（支持 -f/--purge、--yes）" >&2; exit 1 ;;
  esac
done

if [ "$(id -u)" != "0" ]; then
  echo "错误：卸载需要 root，请用 sudo 执行。" >&2
  exit 1
fi

cd "$CUSTOS_HOME"

echo "即将卸载 Custos 管理器"
echo "  项目目录：$CUSTOS_HOME"
echo "  动作     ：停止并移除容器 + 删除本项目镜像与匿名卷 + 删除 custosctl"
if [ "$PURGE" = "1" ]; then
  echo "  -f       ：额外永久删除以下数据目录："
  for d in backups data generated_nodes; do
    if [ -e "$CUSTOS_HOME/$d" ]; then
      echo "              - $CUSTOS_HOME/$d  ($(du -sh "$CUSTOS_HOME/$d" 2>/dev/null | cut -f1))"
    fi
  done
fi
echo "  不动     ：被管节点上的数据库与 keepalived（请到各节点单独处理）"

if [ "$ASSUME_YES" != "1" ]; then
  printf '确认继续？请输入 yes： '
  read -r ans
  [ "$ans" = "yes" ] || { echo "已取消。"; exit 0; }
fi

if [ "$PURGE" = "1" ] && [ "$ASSUME_YES" != "1" ]; then
  printf '即将永久删除数据目录，此操作不可逆。请输入 DELETE： '
  read -r ans2
  if [ "$ans2" != "DELETE" ]; then
    echo "未确认：已保留 backups/ data/ generated_nodes/。"
    PURGE=0
  fi
fi

# 1) 停止并移除容器（同时删除本项目镜像与匿名卷）
docker compose down --rmi local --volumes || true

# 2) -f：删除数据目录（备份、管理器数据含 data/custos-ha/、节点生成目录）
if [ "$PURGE" = "1" ]; then
  for d in backups data generated_nodes; do
    if [ -e "$CUSTOS_HOME/$d" ]; then
      rm -rf "$CUSTOS_HOME/$d"
      echo "    已删除：$CUSTOS_HOME/$d"
    fi
  done
fi

# 3) 删除命令行工具与配置（实现「随卸载脚本一同卸载」）
rm -f /usr/bin/custosctl
rm -rf /etc/custos

echo "==> 卸载完成：容器与本项目镜像已移除，custosctl 已删除。"
if [ "$PURGE" = "1" ]; then
  echo "    数据目录已删除：backups/ data/ generated_nodes/"
else
  echo "    数据仍在：$CUSTOS_HOME（backups/ data/ generated_nodes/ 未动）"
fi
echo "    如需清理被管节点，请到各节点执行对应的 Uninstall 脚本（谨慎：会删业务库）。"
