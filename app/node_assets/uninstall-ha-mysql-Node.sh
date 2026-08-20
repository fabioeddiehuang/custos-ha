#!/usr/bin/env bash
#
# uninstall-ha-mysql-Node.sh — 还原由 Install-ha-mysql-Node.sh 在本节点所做的全部修改
#
# 一键还原：
#   1) 恢复原始 my.cnf（优先用 Install 创建的 pristine 备份 *.before-ha-node；无备份则移除本脚本写入的标记片段）
#   2) 删除远程 root 账号（root@'%' 及 Install 时指定的 manager-ip 来源）
#   3) 若曾执行 init_baremetal.sql（INIT_DONE=1），删除 ha_monitor / replication / app 账号与 ha_local 库
#   4) 回退防火墙（MySQL 端口 + VRRP 协议 112）
#   5) 停止并移除 keepalived 客户端容器（若 Install 时用 -c 拉起）
#   6) 重启 MySQL 使配置生效，并清理状态文件
#
# 说明：本脚本与 Install 脚本同目录发布，在本节点以 root 执行一次即可。
#      状态文件默认 /etc/ha-mysql-node/install-state（由 Install 写入，含 root 密码，权限 600）。
#      若状态文件丢失，可用 -p 指定 root 密码，并自动在常见 my.cnf 路径中查找标记片段还原。
#
# 用法：
#   sudo bash uninstall-ha-mysql-Node.sh
#   sudo bash uninstall-ha-mysql-Node.sh -p 'MySQL的root密码'   # 状态文件丢失时手动提供
#
set -euo pipefail

ROOT_PASS_OVERRIDE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--root-pass) ROOT_PASS_OVERRIDE="$2"; shift 2;;
    -h|--help) sed -n '2,18p' "$0"; exit 0;;
    *) echo "未知参数: $1"; exit 1;;
  esac
done

log(){ echo -e "\033[1;32m[uninstall]\033[0m $*"; }
warn(){ echo -e "\033[1;33m[warn]\033[0m $*"; }
err(){ echo -e "\033[1;31m[error]\033[0m $*"; }

if [[ $EUID -ne 0 ]]; then
  err "请使用 root 执行：sudo bash $0 ..."; exit 1
fi

# ---------- 读取安装状态（若存在） ----------
STATE=/etc/ha-mysql-node/install-state
CNF=""; CNF_BAK=""; HOST_SPEC=""; PORT=""; SERVER_ID=""; COMPOSE_DIR=""; INIT_DONE=""; ROOT_PASS=""
if [[ -f "$STATE" ]]; then
  # shellcheck disable=SC1090
  source "$STATE"
  log "已载入安装状态: $STATE"
else
  warn "未找到安装状态文件 $STATE；将尝试在常见 my.cnf 中查找标记片段还原，并用 -p 指定密码。"
fi

if [[ -n "$ROOT_PASS_OVERRIDE" ]]; then
  ROOT_PASS="$ROOT_PASS_OVERRIDE"
fi

# ---------- 定位 MySQL 客户端 ----------
MYSQL_BIN="$(command -v mysql || true)"
if [[ -z "$MYSQL_BIN" && -x /www/server/mysql/bin/mysql ]]; then
  MYSQL_BIN=/www/server/mysql/bin/mysql
fi
if [[ -z "$MYSQL_BIN" ]]; then
  err "未找到 mysql 客户端，无法连接 MySQL 删除账号；将仅还原 my.cnf / 防火墙 / keepalived。"; MYSQL_BIN=""
fi

# ---------- 连接 MySQL（优先用状态密码，失败回退 socket） ----------
HAVE_SQL=0
CONN=()
if [[ -n "$MYSQL_BIN" ]]; then
  if [[ -n "$ROOT_PASS" ]] && "$MYSQL_BIN" -uroot -p"$ROOT_PASS" -e "SELECT 1" >/dev/null 2>&1; then
    CONN=( "$MYSQL_BIN" -uroot -p"$ROOT_PASS" ); HAVE_SQL=1
  elif "$MYSQL_BIN" -uroot -e "SELECT 1" >/dev/null 2>&1; then
    CONN=( "$MYSQL_BIN" -uroot ); HAVE_SQL=1
    warn "本地 root 为 auth_socket 免密登录；将用 socket 连接删除账号。"
  else
    warn "无法连接本地 MySQL（未提供密码或密码错误）。跳过账号删除步骤，仅还原 my.cnf / 防火墙 / keepalived。"
  fi
fi
SQL(){ "${CONN[@]}" "$@"; }

# ---------- 1) 删除远程账号 + HA 账号 ----------
if [[ $HAVE_SQL -eq 1 ]]; then
  log "删除远程 root 账号..."
  SQL -e "DROP USER IF EXISTS 'root'@'${HOST_SPEC:-%}';" 2>/dev/null || true
  SQL -e "DROP USER IF EXISTS 'root'@'%';" 2>/dev/null || true
  if [[ "${INIT_DONE:-0}" == "1" ]]; then
    log "删除 HA 账号与 ha_local 库（init_baremetal.sql 曾执行）..."
    for u in ha_monitor replication app; do
      SQL -e "DROP USER IF EXISTS '${u}'@'%';" 2>/dev/null || true
    done
    SQL -e "DROP DATABASE IF EXISTS ha_local;" 2>/dev/null || true
  fi
  SQL -e "FLUSH PRIVILEGES;" 2>/dev/null || true
fi

# ---------- 2) 还原 my.cnf ----------
if [[ -z "$CNF" ]]; then
  for f in /www/server/mysql/my.cnf /etc/my.cnf /etc/mysql/my.cnf; do
    if [[ -f "$f" ]]; then CNF="$f"; break; fi
  done
fi
if [[ -n "$CNF" && -f "$CNF" ]]; then
  if [[ -n "$CNF_BAK" && -f "$CNF_BAK" ]]; then
    mv "$CNF_BAK" "$CNF" && log "✓ 已用 pristine 备份还原 $CNF（并移除备份）"
  elif grep -q "HA-MYSQL-NODE-SETUP-BEGIN" "$CNF"; then
    sed -i '/HA-MYSQL-NODE-SETUP-BEGIN/,/HA-MYSQL-NODE-SETUP-END/d' "$CNF" \
      && log "✓ 已移除本脚本写入的标记片段，还原 $CNF"
  else
    warn "未找到 pristine 备份，也未发现标记片段；my.cnf 可能未被本工具修改，跳过。"
  fi
else
  warn "未找到 my.cnf，跳过配置还原。"
fi

# ---------- 3) 回退防火墙 ----------
P="${PORT:-3306}"
log "回退防火墙（MySQL 端口 $P + VRRP 112）..."
if command -v ufw >/dev/null 2>&1 && ufw status >/dev/null 2>&1; then
  ufw delete allow "${P}/tcp" >/dev/null 2>&1 && log "ufw: 移除 ${P}/tcp" || true
fi
if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active firewalld >/dev/null 2>&1; then
  firewall-cmd --permanent --remove-port="${P}/tcp" >/dev/null 2>&1 && firewall-cmd --reload >/dev/null 2>&1 && log "firewalld: 移除 ${P}/tcp" || true
  firewall-cmd --permanent --remove-protocol=vrrp >/dev/null 2>&1 && firewall-cmd --reload >/dev/null 2>&1 && log "firewalld: 移除 VRRP(112)" || true
fi
if command -v iptables >/dev/null 2>&1; then
  iptables -D INPUT -p tcp --dport "${P}" -j ACCEPT 2>/dev/null || true
  iptables -D INPUT -p vrrp -j ACCEPT 2>/dev/null || true
  log "iptables: 已尝试移除 ${P}/tcp 与 VRRP(112) 规则"
fi

# ---------- 4) 停止 keepalived 客户端 ----------
if [[ -n "$COMPOSE_DIR" && -d "$COMPOSE_DIR" ]] \
   && [[ -f "$COMPOSE_DIR/docker-compose.yaml" || -f "$COMPOSE_DIR/docker-compose.yml" ]]; then
  if command -v docker >/dev/null 2>&1; then
    log "停止并移除 keepalived 客户端容器..."
    ( cd "$COMPOSE_DIR" && docker compose down ) || true
  fi
fi

# ---------- 5) 重启 MySQL ----------
log "重启 MySQL 使还原后的配置生效 ..."
if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files mysqld.service >/dev/null 2>&1; then
  systemctl restart mysqld 2>/dev/null || systemctl restart mysql 2>/dev/null || true
elif [[ -x /etc/init.d/mysqld ]]; then
  /etc/init.d/mysqld restart
elif [[ -x /www/server/mysql/support-files/mysql.server ]]; then
  /www/server/mysql/support-files/mysql.server restart
else
  warn "未识别 MySQL 重启方式，请手动重启（service mysql restart / 宝塔面板）。"
fi
sleep 3

# ---------- 6) 清理状态文件 ----------
if [[ -f "$STATE" ]]; then rm -f "$STATE" && log "已清理状态文件 $STATE"; fi

echo
log "==== 本节点已还原 ===="
echo "  • my.cnf 已恢复到安装前状态"
echo "  • 远程 root 账号与 HA 账号（如有）已删除"
echo "  • 防火墙规则与 keepalived 客户端已回退"
echo "  • 若需重新接入管理器，再次执行 Install-ha-mysql-Node.sh 即可。"
