#!/bin/bash
# keepalived vrrp_script：本机是否应持有 VIP
# 条件全部满足才返回 0（持有 VIP），否则返回 1（释放 VIP）
# 读取 /etc/keepalived/ha_check.conf
set -u
CONF=/etc/keepalived/ha_check.conf
[ -f "$CONF" ] && source "$CONF"

MYSQL="mysql -h127.0.0.1 -P${LOCAL_PORT} -u${LOCAL_USER} -p${LOCAL_PASS} -N --connect-timeout=3"

# 1) 本机 MySQL 必须存活
if ! $MYSQL -e "SELECT 1" >/dev/null 2>&1; then
    exit 1
fi

# 2) 本机必须是可写主（read_only = OFF）
RO=$($MYSQL -e "SELECT @@read_only" 2>/dev/null)
if [ "$RO" != "0" ]; then
    exit 1
fi

# 3) 管理库 current_master 必须指向本机（防脑裂）
#    若管理库不可达，安全跳过该检查（不影响业务连续性，对应"管理库单点仅影响管理能力"）
CM=$($MYSQL -h${MGMT_HOST} -P${MGMT_PORT} -u${MGMT_USER} -p${MGMT_PASS} \
     -N -e "SELECT CONCAT(host,':',port) FROM ${MGMT_DB:-ha}.current_master WHERE id=1" 2>/dev/null)
if [ -n "$CM" ]; then
    if [ "$CM" != "${LOCAL_HOST}:${LOCAL_PORT}" ]; then
        exit 1
    fi
fi

exit 0
