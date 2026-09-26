#!/bin/sh
# ---------------------------------------------------------------------------
# Custos 服务端卸载（仅清理【管理器】；默认绝不动被管节点与业务数据）。
#
# 用法：sudo custosctl uninstall [-f|--purge] [--yes]
#
#   默认       停止并移除容器 + 删除本项目镜像（custos-server:<VERSION>）与匿名卷
#              + 删除 /usr/bin/custosctl 与 /etc/custos
#              （例外：本机同时是节点时，仅删管理器配置 custosctl.env，保留 node.env 与命令，custosctl node 仍可用）
#              （保留 backups/ data/ generated_nodes/）
#   -f|--purge 额外【永久删除】backups/ data/（含 data/custos-ha/）generated_nodes/
#              需输入 DELETE 二次确认（--yes 可跳过确认，供脚本化使用）
#   --yes      跳过交互确认
#
#   密钥保底  无论是否 -f，凭证密钥（data/cred.key、data/secret_key）都会先镜像到
#             /var/lib/custos/（项目目录之外，本脚本不删除）；重装时 install.sh 自动取回，
#             故「删掉整个项目目录后重装」仍能解开存量凭证。需彻底作废请手动删该目录。
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

# 0) 凭证密钥保底（v2.3.10）：先把密钥镜像到【项目目录之外】的保险库。
#    否则 -f 删除 data/、或用户直接删掉整个项目目录后，管理库里所有 ENC1 凭证将
#    永久解不开 —— 表现为复制口令变成密文、CHANGE MASTER 报 1470、复制集体瘫痪，
#    且没有任何显式告警，极难定位。
#    保险库位于 /var/lib/custos（不属项目目录），默认卸载与 -f 都**不会**删除它；
#    重装时 install.sh 会自动取回，从而实现「删掉项目目录重装也能恢复凭证」。
KEY_VAULT=/var/lib/custos
PRESERVED_KEYS=""
mkdir -p "$KEY_VAULT" 2>/dev/null || true
for k in cred.key secret_key; do
  if [ -f "$CUSTOS_HOME/data/$k" ]; then
    if cp -f "$CUSTOS_HOME/data/$k" "$KEY_VAULT/$k" 2>/dev/null; then
      chmod 600 "$KEY_VAULT/$k" 2>/dev/null || true
      PRESERVED_KEYS="$PRESERVED_KEYS $k"
    fi
  fi
done

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
if [ -n "$PRESERVED_KEYS" ]; then
  echo "  保留     ：凭证密钥$PRESERVED_KEYS 已镜像到 $KEY_VAULT（重装 install.sh 自动取回）"
fi

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
#    本机若同时为节点（存在有效 node.env → 指向真实节点包），则仅移除【管理器】配置，
#    保留 node.env 与 /usr/bin/custosctl，使 custosctl node 仍可正常使用（极边缘场景保护）。
_KEEP_NODE=0
if [ -f /etc/custos/node.env ]; then
  _nd=$(. /etc/custos/node.env 2>/dev/null; echo "${NODE_DIR:-}")
  if [ -n "$_nd" ] && { [ -f "$_nd/docker-compose.yaml" ] || [ -f "$_nd/docker-compose.yml" ]; }; then
    _KEEP_NODE=1
  fi
fi
# 兜底：老版本引导未登记 node.env（或登记失效）时，按常见基目录探测本机真实节点包，
# 避免把「本机同时是节点」漏判成纯管理器 —— 那样会真删掉节点还在用的 CLI。
# ⚠ 必须排除本管理器的 generated_nodes/<ip>：那里放的是【待下发到远端节点】的部署单元，
#   并非本机节点。若不排除，纯管理器会被误判为兼节点，从而错误保留 CLI。
if [ "$_KEEP_NODE" != "1" ]; then
  _gen="$CUSTOS_HOME/generated_nodes"
  for _base in /home/docker /opt /data /srv /home; do
    for _d in "$_base"/*; do
      [ -d "$_d" ] || continue
      case "$_d" in "$_gen"/*) continue ;; esac
      if { [ -f "$_d/docker-compose.yaml" ] || [ -f "$_d/docker-compose.yml" ]; } \
         && { [ -d "$_d/mariadb12.3.3/conf" ] || [ -d "$_d/mysql8.0/conf" ]; }; then
        _KEEP_NODE=1
        break 2
      fi
    done
  done
fi
rm -f /usr/share/bash-completion/completions/custosctl 2>/dev/null || true
rm -f /etc/bash_completion.d/custosctl 2>/dev/null || true
rm -f /usr/local/share/zsh/site-functions/_custosctl 2>/dev/null || true
rm -f /usr/share/zsh/site-functions/_custosctl 2>/dev/null || true
if [ "$_KEEP_NODE" = "1" ]; then
  # 仅删管理器配置，保留节点配置与命令
  rm -f /etc/custos/custosctl.env
  echo "    本机同时为节点：已保留 /etc/custos/node.env 与 /usr/bin/custosctl（custosctl node 仍可用）。" >&2
  echo "          如需彻底移除节点，请到节点包目录执行卸载脚本，或手动 rm -f /etc/custos/node.env。" >&2
else
  rm -f /usr/bin/custosctl
  rm -rf /etc/custos
fi

echo "==> 卸载完成：容器与本项目镜像已移除${_KEEP_NODE:+；本机兼为节点，custosctl 与节点配置已保留}。"
if [ "$PURGE" = "1" ]; then
  echo "    数据目录已删除：backups/ data/ generated_nodes/"
else
  echo "    数据仍在：$CUSTOS_HOME（backups/ data/ generated_nodes/ 未动）"
fi
if [ -n "$PRESERVED_KEYS" ]; then
  echo "    凭证密钥已保留在 $KEY_VAULT（重装时 install.sh 自动恢复；"
  echo "    如需连同管理库凭证一起彻底作废，请手动删除该目录）"
fi
echo "    如需清理被管节点，请到各节点执行对应的 Uninstall 脚本（谨慎：会删业务库）。"
