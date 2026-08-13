#!/bin/bash
# keepalived 健康检查：本节点是否应持有 VIP（成为可写主库）。
# 判定完全基于【本机 MySQL】，不依赖节点回连管理库，避免管理库不可达导致 VIP 不漂移：
#   1) 本机 MySQL 必须存活
#   2) 本机必须是可写主（read_only = 0）
#   3) 本机是否「应为主」读本机 ha_local.state.current_master（管理端切换时写入）；
#      若已写入且 != 本机 -> 释放 VIP（即使 read_only 异常也为安全兜底）；
#      若尚未写入（初始化阶段）-> 仅靠 read_only 判定。
set -u
CONF=/etc/keepalived/ha_check.conf
[ -f "$CONF" ] && source "$CONF"

MYSQL_LOCAL="mysql -h127.0.0.1 -P${LOCAL_PORT} -u${LOCAL_ROOT_USER:-root} -p${LOCAL_ROOT_PASS} -N --connect-timeout=3"

# 1) 本机 MySQL 必须存活
if ! $MYSQL_LOCAL -e "SELECT 1" >/dev/null 2>&1; then
    exit 1
fi

# 2) 必须是可写主
RO=$($MYSQL_LOCAL -e "SELECT @@read_only" 2>/dev/null)
if [ "$RO" != "0" ]; then
    exit 1
fi

# 3) 本机是否应为主：优先读本机 ha_local.state（管理端在切换时写入）
LM=$($MYSQL_LOCAL -e "SELECT v FROM ha_local.state WHERE k='current_master'" 2>/dev/null)
if [ -n "$LM" ]; then
    if [ "$LM" != "${LOCAL_HOST}:${LOCAL_PORT}" ]; then
        exit 1
    fi
fi

exit 0
