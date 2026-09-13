#!/usr/bin/env bash
# =============================================================
# Uninstall-mariadb-Node.sh  —  裸机 MariaDB 节点卸载（交互式）
# -------------------------------------------------------------
# 用途：卸载由 Install-ha-mariadb-Node.sh 安装的裸机 MariaDB 节点：
#   • 停止并禁用 systemd 单元（mariadb-ha.service）
#   • 清理复制通道、删除 HA 账号/远程 root、删除 ha_local、复位参数
#   • 停止 keepalived 客户端容器、回退防火墙
#   • 恢复 /etc/my.cnf（安装前备份）、删除新增的软链/状态文件/数据目录
#   • 【默认保留】MariaDB 程序目录（/opt/mariadb），便于快速重装/回滚
#
# 用法（在节点目录内，以 root 执行）：
#       sudo bash Uninstall-mariadb-Node.sh            # 交互式（默认保留安装包）
#       sudo bash Uninstall-mariadb-Node.sh -f         # 同时删除程序目录 /opt/mariadb
#       sudo bash Uninstall-mariadb-Node.sh -y         # 跳过确认（配合 -f 可无人值守）
#
# 注意：本脚本只处理「裸机 MariaDB」节点；Docker MySQL 8.0 节点无需本脚本
#       （直接 cd 节点目录 && docker compose down 即可）。
# =============================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"   # 节点包根（docker-compose.yaml 所在）

FORCE_DEL_DIR=0
ASSUME_YES=0
for a in "$@"; do
  case "$a" in
    -f) FORCE_DEL_DIR=1 ;;
    -y) ASSUME_YES=1 ;;
    -h|--help) sed -n '3,18p' "$0"; exit 0 ;;
  esac
done

log(){ echo -e "\033[1;32m[mariadb-uninstall]\033[0m $*"; }
warn(){ echo -e "\033[1;33m[warn]\033[0m $*"; }
err(){ echo -e "\033[1;31m[error]\033[0m $*"; }

if [[ $EUID -ne 0 ]]; then
  err "请使用 root 执行：sudo bash Uninstall-mariadb-Node.sh"; exit 1
fi

# ---------- 读取状态文件（缺省值兜底） ----------
STATE_DIR=/etc/ha-mysql-node
STATE="$STATE_DIR/install-state-mariadb"
NODE_TYPE="mariadb"
INSTALL_DIR=/opt/mariadb
DATA_DIR=/data/mariadb
SYSTEMD_UNIT=mariadb-ha.service
PORT=3306
LOCAL_ROOT_PASS=""
if [[ -f "$STATE" ]]; then
  while IFS='=' read -r k v; do
    case "$k" in
      NODE_TYPE) NODE_TYPE="$v" ;;
      INSTALL_DIR) INSTALL_DIR="$v" ;;
      DATA_DIR) DATA_DIR="$v" ;;
      SYSTEMD_UNIT) SYSTEMD_UNIT="$v" ;;
      PORT) PORT="$v" ;;
      LOCAL_ROOT_PASS) LOCAL_ROOT_PASS="$v" ;;
    esac
  done < "$STATE"
  log "已读取安装状态：$STATE"
else
  warn "未找到安装状态文件 $STATE，将按缺省值（INSTALL_DIR=$INSTALL_DIR DATA_DIR=$DATA_DIR）尝试卸载"
fi

MYSQL_BIN=""
[[ -x "$INSTALL_DIR/bin/mysql" ]] && MYSQL_BIN="$INSTALL_DIR/bin/mysql"
[[ -z "$MYSQL_BIN" && -x /usr/local/bin/mysql ]] && MYSQL_BIN=/usr/local/bin/mysql
SOCK="$DATA_DIR/mysql.sock"

# ---------- 交互确认 ----------
cat <<EOF

即将执行以下卸载操作（裸机 MariaDB 节点）：
  1. 停止/禁用 systemd 单元：$SYSTEMD_UNIT
  2. 清理复制通道、删除 HA 账号/远程 root、删除 ha_local、复位参数
  3. 停止 keepalived 客户端容器、回退防火墙（端口 $PORT/tcp + VRRP）
  4. 恢复 /etc/my.cnf（安装前备份）、删除新增软链/状态文件/数据目录 $DATA_DIR
  5. 程序目录 $INSTALL_DIR：$( [[ $FORCE_DEL_DIR -eq 1 ]] && echo "【将删除】" || echo "【默认保留，便于快速重装；加 -f 可删除】" )

EOF
if [[ $ASSUME_YES -ne 1 ]]; then
  read -r -p "确认卸载以上内容？[Y/n] " ans
  case "$ans" in
    n|N|no|NO) log "已取消卸载"; exit 0 ;;
  esac
fi

# ---------- 0) 尝试连接做 SQL 清理（连不上则跳过，仅做文件/服务清理） ----------
SQL_PW="__UNSET__"
connect_sql() {
  for pw in "$LOCAL_ROOT_PASS" ""; do
    [[ -n "$pw" ]] || continue
    if "$MYSQL_BIN" -uroot -p"$pw" --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; then SQL_PW="$pw"; return 0; fi
  done
  if "$MYSQL_BIN" -uroot --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; then SQL_PW=""; return 0; fi
  # 状态文件密码失效（如曾手工改密），交互询问
  read -r -s -p "无法用已知密码连接 MariaDB，请输入当前 root 密码（用于清理账号/复制；留空尝试免密）：" P; echo
  if "$MYSQL_BIN" -uroot ${P:+-p"$P"} --socket="$SOCK" -e "SELECT 1" >/dev/null 2>&1; then SQL_PW="$P"; return 0; fi
  return 1
}

if [[ -n "$MYSQL_BIN" && -S "$SOCK" ]]; then
  if connect_sql; then
    PW_ARG=""; [[ -n "$SQL_PW" ]] && PW_ARG="-p$SQL_PW"
    log "已连接 MariaDB，执行 SQL 清理..."
    "$MYSQL_BIN" -uroot $PW_ARG --socket="$SOCK" <<'SQL' || warn "部分 SQL 清理跳过（请手动检查）"
-- 停复制并清除复制通道（避免残留中继日志/通道配置）
STOP REPLICA IO_THREAD, SQL_THREAD;
RESET REPLICA ALL;
-- 删除远程 root（仅保留本地登录，清除内网 '%' 暴露）
DROP USER IF EXISTS 'root'@'%';
DROP USER IF EXISTS 'root'@'::1';
-- 删除 HA 专用账号
DROP USER IF EXISTS 'ha_monitor'@'%';
DROP USER IF EXISTS 'ha_monitor'@'127.0.0.1';
DROP USER IF EXISTS 'replication'@'%';
DROP USER IF EXISTS 'replication'@'127.0.0.1';
DROP USER IF EXISTS 'app'@'%';
DROP USER IF EXISTS 'app'@'127.0.0.1';
-- 删除本节点本地状态库
DROP DATABASE IF EXISTS ha_local;
-- 复位持久化参数 + 关闭只读，恢复为独立实例
SET GLOBAL read_only = OFF;
RESET PERSIST server_id;
RESET PERSIST gtid_domain_id;
RESET PERSIST gtid_strict_mode;
RESET PERSIST enforce_gtid_consistency;
RESET PERSIST log_bin;
RESET PERSIST log_slave_updates;
FLUSH PRIVILEGES;
SQL
    log "SQL 清理完成（复制/HA 账号/ha_local 已移除）"
  else
    warn "无法连接 MariaDB（实例可能已停止），跳过 SQL 清理，仅做文件/服务级卸载"
  fi
else
  warn "未找到 mysql 客户端或 socket（$SOCK），跳过 SQL 清理"
fi

# ---------- 1) 停止并禁用 systemd 单元 ----------
if systemctl list-unit-files "$SYSTEMD_UNIT" >/dev/null 2>&1; then
  systemctl stop "$SYSTEMD_UNIT" >/dev/null 2>&1 || true
  systemctl disable "$SYSTEMD_UNIT" >/dev/null 2>&1 || true
  rm -f /etc/systemd/system/"$SYSTEMD_UNIT"
  systemctl daemon-reload >/dev/null 2>&1 || true
  log "已停止/禁用并删除 systemd 单元 $SYSTEMD_UNIT"
else
  warn "未找到 systemd 单元 $SYSTEMD_UNIT（可能未注册）"
fi

# ---------- 2) 停止 keepalived 客户端容器 ----------
# 注意：仅 docker compose down 停止容器，【不删除】本目录的安装脚本/docker-compose.yaml，
# 否则后续在同目录重装会因脚本/compose 已被删而 keepalived 起不来。
if command -v docker >/dev/null 2>&1 && [[ -f "$NODE_ROOT/docker-compose.yaml" || -f "$NODE_ROOT/docker-compose.yml" ]]; then
  ( cd "$NODE_ROOT" && docker compose down ) >/dev/null 2>&1 || warn "keepalived 容器停止失败，请手动 docker compose down"
  log "已停止 keepalived 客户端容器（保留本目录安装脚本/compose，便于重装）"
fi

# ---------- 3) 回退防火墙（MySQL 端口 + VRRP） ----------
revert_port_vrrp() {
  if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active firewalld >/dev/null 2>&1; then
    firewall-cmd --permanent --remove-port="${PORT}/tcp" >/dev/null 2>&1 || true
    firewall-cmd --permanent --remove-protocol=vrrp >/dev/null 2>&1 || true
    firewall-cmd --reload >/dev/null 2>&1 || true
    log "firewalld：已移除 ${PORT}/tcp + VRRP(112)"
  elif command -v ufw >/dev/null 2>&1 && ufw status >/dev/null 2>&1; then
    ufw delete allow "${PORT}/tcp" >/dev/null 2>&1 || true
    iptables -D INPUT -p vrrp -j ACCEPT 2>/dev/null || true
    log "ufw：已移除 ${PORT}/tcp + VRRP(112)"
  else
    iptables -D INPUT -p tcp --dport "$PORT" -j ACCEPT 2>/dev/null || true
    iptables -D INPUT -p vrrp -j ACCEPT 2>/dev/null || true
    log "iptables：已移除 ${PORT}/tcp + VRRP(112)"
  fi
}
revert_port_vrrp

# ---------- 4) 恢复 /etc/my.cnf（安装前备份） ----------
if [[ -f /etc/my.cnf.before-mariadb-ha ]]; then
  mv -f /etc/my.cnf.before-mariadb-ha /etc/my.cnf
  log "已恢复安装前的 /etc/my.cnf"
elif [[ -f /etc/my.cnf ]]; then
  # 没有备份说明此前无宿主配置被覆盖，直接移除我们写入的 my.cnf
  rm -f /etc/my.cnf
  warn "无安装前备份，已删除 /etc/my.cnf（若该机原本有 MySQL，请确认配置）"
fi

# ---------- 5) 删除新增软链 ----------
for bin in mysql mariadb mysqldump mysqladmin mysqlshow myisamchk; do
  rm -f /usr/local/bin/$bin /usr/bin/$bin
done
rm -f /tmp/mysql.sock /var/run/mysqld/mysqld.sock
log "已清理客户端/socket 软链"

# ---------- 6) 删除数据目录（所有新增数据） ----------
if [[ -d "$DATA_DIR" ]]; then
  rm -rf "$DATA_DIR"
  log "已删除数据目录 $DATA_DIR"
fi

# ---------- 7) 删除状态文件 ----------
rm -f "$STATE"
rmdir "$STATE_DIR" >/dev/null 2>&1 || true
log "已删除安装状态文件"

# ---------- 8) 程序目录（默认保留） ----------
if [[ $FORCE_DEL_DIR -eq 1 ]]; then
  rm -rf "$INSTALL_DIR"
  log "已删除程序目录 $INSTALL_DIR（-f）"
else
  log "已保留程序目录 $INSTALL_DIR（如需彻底删除，重新执行：sudo bash Uninstall-mariadb-Node.sh -f）"
fi

echo
log "==== MariaDB 节点卸载完成 ===="
echo "提醒：请到管理器 Web 界面删除该节点（避免后台继续探测/选主）。"
echo "若仅需更新密码或配置，无需卸载——重新生成节点目录后重跑 Install-ha-mariadb-Node.sh 即可（幂等更新）。"
