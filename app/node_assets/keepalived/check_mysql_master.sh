#!/bin/bash
# keepalived track_script：本机是否应持有 VIP（ha_mysql）
# 返回 0 = 允许持有 VIP；非 0 = 放弃，VIP 漂走到其它节点
#
# 设计要点：current_master 判定【完全本地】，读取本机 MySQL 的
# ha_local.state.current_master（由管理端在每次切换后写入），
# 不依赖节点回连管理库——彻底规避「管理库不可达（如防火墙 DROP）时
# 查询挂死，拖累整个检查脚本，导致合法主库永远拿不到 VIP」的故障
# （如 Anolis 7.9 节点 33 无法连通管理端时 VIP 卡死）。
set -u

CONF=/etc/keepalived/ha_check.conf
[ -f "$CONF" ] && source "$CONF"

# 本机 MySQL 探活连接（127.0.0.1 走宿主机回环，由 keepalived 容器 host 网络映射）
MYSQL_LOCAL="mysql -h127.0.0.1 -P${LOCAL_PORT} -u${LOCAL_ROOT_USER:-root} -N --connect-timeout=2"

# 用 timeout 硬封装每个 mysql 调用，防止任何连接异常挂死、拖累 keepalived（避免 already running）
TO=""
command -v timeout >/dev/null 2>&1 && TO="timeout 2"

# 1) 本机 MySQL 必须存活（root 探活）
if ! MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e "SELECT 1" >/dev/null 2>&1; then
    exit 1
fi

# 2) 本机必须是可写主库（read_only = OFF）
RO=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e "SELECT @@read_only" 2>/dev/null)
if [ "$RO" != "0" ]; then
    exit 1
fi

# 3) 本机必须被管理端标记为当前主库（防脑裂）。
#    读取【本机本地】MySQL 的 ha_local.state.current_master，无需回连管理库。
#    - 为空（尚未划分主库 / 本地状态缺失）：安全放行，VIP 由 read_only 这把锁兜底，
#      避免管理端抖动误杀 VIP。
#    - 等于本机（LOCAL_HOST:LOCAL_PORT）：放行。
#    - 等于其它节点：放弃 VIP（本机非主，避免双主脑裂）。
CM=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e \
     "SELECT v FROM ha_local.state WHERE k='current_master'" 2>/dev/null)
if [ -n "$CM" ] && [ "$CM" != "${LOCAL_HOST}:${LOCAL_PORT}" ]; then
    exit 1
fi

exit 0
