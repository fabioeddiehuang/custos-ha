#!/bin/bash
# keepalived track_script（宿主机直装模式）：本机是否应持有 VIP（ha_mysql）
# 返回 0 = 允许持有 VIP；非 0 = 放弃，VIP 漂走到其它节点
#
# current_master 判定【完全本地】：读取本机 MySQL 的 ha_local.state.current_master
# （由管理端在每次切换后写入），不依赖节点回连管理库，规避管理库不可达导致 VIP 卡死。
set -u

CONF=/etc/keepalived/ha_check.conf
[ -f "$CONF" ] && source "$CONF"

MYSQL_LOCAL="mysql -h127.0.0.1 -P${LOCAL_PORT} -u${LOCAL_ROOT_USER:-root} -N --connect-timeout=2"

TO=""
command -v timeout >/dev/null 2>&1 && TO="timeout 2"

# 1) 本机 MySQL 必须存活
if ! MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e "SELECT 1" >/dev/null 2>&1; then
    exit 1
fi

# 2) 本机必须是可写主库（read_only = OFF）
RO=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e "SELECT @@read_only" 2>/dev/null)
if [ "$RO" != "0" ]; then
    exit 1
fi

# 3) 本机必须被管理端标记为当前主库（防脑裂）。读本机本地 ha_local.state。
CM=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e \
     "SELECT v FROM ha_local.state WHERE k='current_master'" 2>/dev/null)
if [ -n "$CM" ] && [ "$CM" != "${LOCAL_HOST}:${LOCAL_PORT}" ]; then
    exit 1
fi

exit 0
