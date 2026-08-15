#!/bin/bash
# keepalived 状态切换为 MASTER 时触发：本节点持有 VIP，置为可写主库（关闭 read_only）。
# 与 check_mysql_master.sh 共用 /etc/keepalived/ha_check.conf 里的本机探活账号。
set -u
CONF=/etc/keepalived/ha_check.conf
if [ -f "$CONF" ]; then
  . "$CONF"
fi
mysql -h127.0.0.1 -P"${LOCAL_PORT:-3306}" \
      -u"${LOCAL_USER:-ha_monitor}" -p"${LOCAL_PASS}" \
      -e "SET GLOBAL read_only=0;" >/dev/null 2>&1 || true
exit 0
