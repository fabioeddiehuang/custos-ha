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

# 诊断日志：把每次判定结果/失败原因落盘到宿主机 /var/log（keepalived 容器为 host 网络，
# /var/log 即宿主机目录，便于排障时 `cat /var/log/keepalived_chk.log` 直接查看，
# 无需依赖 docker logs 被 keepalived 自身日志淹没）。最近 200 行循环覆盖。
# 日志落盘：优先 /var/log（生产 keepalived 容器以 root 运行可写）；
# 若该目录不可写（如调试时非 root），自动回退到 /tmp，保证诊断日志始终可用。
LOG=/var/log/keepalived_chk.log
if ! : >>"$LOG" 2>/dev/null; then
    LOG=/tmp/keepalived_chk.log
fi
log() {
    echo "$(date '+%F %T') [$MY_NODE] $*" >>"$LOG" 2>/dev/null
    echo "$(date '+%F %T') [$MY_NODE] $*" >&2
}

CONF="${HA_CHECK_CONF:-/etc/keepalived/ha_check.conf}"
[ -f "$CONF" ] && source "$CONF"
MY_NODE="${NODE_IP:-${LOCAL_HOST:-127.0.0.1}}"

# ---- 客户端命令探测（MariaDB 12.x 关键适配）----
# MariaDB 11+ 官方把 `mysql` 标记为废弃、12.x 只提供 `mariadb`；而 Debian 的
# default-mysql-client 又可能只提供 `mysql`。硬编码任一命令，在另一种环境下就是
# `command not found` —— 旧版脚本把这种失败也一并静默吞掉，表现为「探活永远失败、
# 节点永远放弃 VIP」，或 notify 置可写失败、业务写入报 1290。
# 故统一探测：优先 mariadb，回退 mysql；都找不到则明确报错而不是假装失败。
MYSQL_BIN=""
for _b in mariadb mysql; do
  if command -v "$_b" >/dev/null 2>&1; then MYSQL_BIN="$_b"; break; fi
done
if [ -z "$MYSQL_BIN" ]; then
  log "FAIL@0 容器内未找到 mariadb/mysql 客户端，无法探活：放弃VIP。"
  log "       请在 keepalived 镜像中安装 mariadb-client（或 default-mysql-client）后重建。"
  exit 1
fi

# 本机 MySQL 探活连接（127.0.0.1 走宿主机回环，由 keepalived 容器 host 网络映射）
MYSQL_LOCAL="${MYSQL_BIN} -h127.0.0.1 -P${LOCAL_PORT} -u${LOCAL_ROOT_USER:-root} -N --connect-timeout=2"

# 用 timeout 硬封装每个 mysql 调用，防止任何连接异常挂死、拖累 keepalived（避免 already running）
TO=""
command -v timeout >/dev/null 2>&1 && TO="timeout 2"

# 1) 本机 MySQL 必须存活（root 探活）
#    捕获 mysql 真实错误（如 Access denied / Can't connect）到诊断日志，
# 避免 2>/dev/null 吞掉根因导致无从排障。
PROBE_ERR=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e "SELECT 1" 2>&1)
if [ $? -ne 0 ]; then
    log "FAIL@1 本机MySQL探活失败 (host=127.0.0.1 port=$LOCAL_PORT user=${LOCAL_ROOT_USER:-root})：$PROBE_ERR"
    exit 1
fi
log "OK@1 本机MySQL探活通过"

# 2) 本机必须是可写主库（read_only = OFF）
#    注意：MySQL 的 @@read_only 返回 0/1，而 MariaDB 返回 OFF/ON（字符串）。
#    两种都要识别——否则 MariaDB 节点永远被判为「只读」→ 所有节点都放弃 VIP → VIP 离线。
RO=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e "SELECT @@read_only" 2>&1 | tr '[:lower:]' '[:upper:]')
# 读不到 read_only（连接/权限异常，输出里含 ERROR/Access denied/Can't connect）时，
# 不应误判为「可写」放行，须明确失败并暴露真实错误，否则主库拿不到 VIP 且无从排障。
if echo "$RO" | grep -qiE "ERROR|ACCESS DENIED|CAN'T CONNECT|UNKNOWN VARIABLE|TIMED OUT"; then
    log "FAIL@2 读取 read_only 异常 (host=127.0.0.1 port=$LOCAL_PORT)：$RO"
    exit 1
fi
if [ "$RO" = "1" ] || [ "$RO" = "ON" ]; then
    log "FAIL@2 本机read_only=$RO（只读，非主库）：放弃VIP。若本机应为主库，请检查管理端是否已将其提升为可写(SET GLOBAL read_only=OFF)"
    exit 1
fi
log "OK@2 本机read_only=$RO（可写主库）"

# 2b) super_read_only 必须关闭（MySQL 8.0 专有）。
#     陷阱：super_read_only=ON 会强制 read_only=ON 并拒绝普通业务写入（同样报 1290）。
#     若只查 read_only，会出现「read_only=OFF 但实际不可写」的假主节点持有 VIP，
#     业务经 VIP 写入报 1290 而 VIP 却不会漂移。
#     MariaDB 无该变量（返回 Unknown system variable），视为不支持并跳过，不影响判定。
SRO=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e "SELECT @@super_read_only" 2>&1 | tr '[:lower:]' '[:upper:]')
if echo "$SRO" | grep -qiE "UNKNOWN SYSTEM VARIABLE|UNKNOWN VARIABLE"; then
    log "OK@2b 本机不支持 super_read_only（MariaDB）：跳过该项检查"
elif echo "$SRO" | grep -qiE "ERROR|ACCESS DENIED|CAN'T CONNECT|TIMED OUT"; then
    log "FAIL@2b 读取 super_read_only 异常 (host=127.0.0.1 port=$LOCAL_PORT)：$SRO"
    exit 1
elif [ "$SRO" = "1" ] || [ "$SRO" = "ON" ]; then
    log "FAIL@2b 本机super_read_only=$SRO（超级只读，业务写入会报1290）：放弃VIP"
    exit 1
else
    log "OK@2b 本机super_read_only=$SRO"
fi

# 3) 本机必须被管理端标记为当前主库（防脑裂）。
#    读取【本机本地】MySQL 的 ha_local.state.current_master，无需回连管理库。
#    - 为空（尚未划分主库 / 本地状态缺失）：安全放行，VIP 由 read_only 这把锁兜底，
#      避免管理端抖动误杀 VIP。
#    - 等于本机（LOCAL_HOST:LOCAL_PORT）：放行。
#    - 等于其它节点：放弃 VIP（本机非主，避免双主脑裂）。
CM=$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO $MYSQL_LOCAL -e \
     "SELECT v FROM ha_local.state WHERE k='current_master'" 2>&1)
# 读取失败（连接/权限/表不存在）时 CM 会含 ERROR，视为「本地状态缺失」安全放行，
# 避免误判为「本机非主」而放弃 VIP；但同时打日志暴露异常便于排障。
if echo "$CM" | grep -qiE "ERROR|ACCESS DENIED|CAN'T CONNECT|UNKNOWN TABLE|TIMED OUT"; then
    log "WARN@3 读取 current_master 异常（视为状态缺失，安全放行）：$CM"
    CM=""
fi
# 本节点的「对外标识」：裸机场景本地探活走 127.0.0.1，但管理器写入的 current_master
# 是节点注册用的真实 IP（如 192.0.2.203:33065），故以 NODE_IP 为主、LOCAL_HOST 兜底，
# 二者任一命中即视为「本机就是当前主库」，避免裸机节点永远拿不到 VIP。
# 为兼容 current_master 可能不带端口（早期写入格式），同时按「IP 段」比较。
MY_NODE="${NODE_IP:-$LOCAL_HOST}"
cm_ip="${CM%%:*}"   # current_master 的 IP 段
if [ -z "$CM" ]; then
    # 为空：安全放行（尚未划分主库 / 本地状态缺失），由 read_only 兜底，避免管理端抖动误杀 VIP
    log "OK@3 current_master 为空：安全放行（read_only 已把关，VIP 由可写状态决定）"
elif [ "$CM" = "${MY_NODE}:${LOCAL_PORT}" ] || [ "$CM" = "${LOCAL_HOST}:${LOCAL_PORT}" ] || [ "$cm_ip" = "$MY_NODE" ] || [ "$cm_ip" = "$LOCAL_HOST" ]; then
    log "OK@3 current_master=$CM 命中本机($MY_NODE/$LOCAL_HOST)：放行持 VIP"
else
    log "FAIL@3 current_master=$CM 不等于本机($MY_NODE:$LOCAL_PORT 或 $LOCAL_HOST:$LOCAL_PORT)：本机非主库，放弃VIP（防脑裂）"
    exit 1
fi

exit 0
