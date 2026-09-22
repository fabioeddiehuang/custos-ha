#!/bin/bash
# Custos 节点卸载脚本：裸机 MySQL 8.0（node_type=mysql8）
# 职责：停止并禁用 Custos 的 MySQL 服务单元 → 卸载程序（安装包形态删除 /opt/mysql）
#       → 清理配置、日志与安装状态。
# ⚠ 默认【不删除数据目录 /data/custos-ha/mysql8】，以免误删业务数据；
#   如需彻底清理请加 --purge-data。
set -eu
PURGE=0
[ "${1:-}" = "--purge-data" ] && PURGE=1

DATA_DIR="/data/custos-ha/mysql8"
LOG_DIR="/data/custos-ha/logs/$(basename "$DATA_DIR")"
# 状态文件记录了上次安装的真实路径与单元名（若被环境变量覆盖过），优先采用
STATE=/etc/ha-mysql-node/install-state-mysql8
if [ -f "$STATE" ]; then
  # shellcheck disable=SC1090
  . "$STATE" || true
  DATA_DIR="${DATA_DIR:-/data/custos-ha/mysql8}"
  LOG_DIR="${LOG_DIR:-/data/custos-ha/logs/$(basename "$DATA_DIR")}"
fi
UNIT="${SYSTEMD_UNIT:-mysql8-ha.service}"

echo "[mysql8-uninstall] 卸载裸机 MySQL 8.0 ..."
if command -v systemctl >/dev/null 2>&1; then
  systemctl stop "$UNIT" 2>/dev/null || systemctl stop mysqld 2>/dev/null || true
  systemctl disable "$UNIT" 2>/dev/null || true
  rm -f "/etc/systemd/system/$UNIT"
  systemctl daemon-reload 2>/dev/null || true
else
  service mysql stop 2>/dev/null || service mysqld stop 2>/dev/null || true
fi

# 系统包安装的 MySQL（程序目录不在 /opt/mysql 时）才走包管理卸载
if [ ! -d /opt/mysql ] && command -v apt-get >/dev/null 2>&1; then
  DEBIAN_FRONTEND=noninteractive apt-get purge -y mysql-server-8.0 mysql-server 2>/dev/null || true
elif [ ! -d /opt/mysql ] && command -v dnf >/dev/null 2>&1; then
  dnf remove -y mysql-community-server 2>/dev/null || true
elif [ ! -d /opt/mysql ] && command -v yum >/dev/null 2>&1; then
  yum remove -y mysql-community-server 2>/dev/null || true
fi
rm -rf /opt/mysql /opt/mysql-8.0.*-linux-glibc2.28-x86_64

rm -f /etc/mysql/my.cnf /etc/mysql/my.cnf.before-custos
rm -rf "$LOG_DIR"
rm -f /etc/logrotate.d/custos-ha
rm -f /var/run/mysqld/mysqld.sock /tmp/mysql.sock
rm -rf /etc/ha-mysql-node
[ "$PURGE" = "1" ] && rm -rf "$DATA_DIR"

echo "[mysql8-uninstall] 完成（数据目录 $DATA_DIR 保留，除非指定 --purge-data）。"
