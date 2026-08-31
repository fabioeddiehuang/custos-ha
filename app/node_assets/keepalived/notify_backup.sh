#!/bin/bash
# keepalived 状态切换为 BACKUP 时触发：本节点释放 VIP，置为只读从库（避免双写脑裂）。
#
# ============ v2.2.12 修复 ============
# 旧实现 `-e "SET GLOBAL read_only=1;" >/dev/null 2>&1 || true` 同样静默吞错：
# 置只读失败时日志一片空白，节点会保持可写 —— 一旦它曾是主库就会形成双写。
# 现改为与 notify_master.sh 对称的多重回退（ha_monitor -> root）+ 重试 + 错误落盘。
# 注意：置只读失败【不退出非 0】—— 状态切换已进入 BACKUP，脚本退出码不会影响
# keepalived 状态机；失败仅记录告警，交由管理端下个周期自愈（make_slave_of 会再压一次）。
# ======================================
set -u

CONF=/etc/keepalived/ha_check.conf
if [ -f "$CONF" ]; then
  . "$CONF"
fi

LOG=/var/log/keepalived_notify.log
if ! : >>"$LOG" 2>/dev/null; then
  LOG=/tmp/keepalived_notify.log
fi
log() {
  echo "$(date '+%F %T') [notify_backup] $*" >>"$LOG" 2>/dev/null
  echo "$(date '+%F %T') [notify_backup] $*" >&2
}

# ---- 客户端命令探测（MariaDB 12.x 关键适配）----
# MariaDB 12.x 只提供 `mariadb`（`mysql` 别名已移除），硬编码 `mysql` 会 command not found。
# 旧版把它静默吞掉，节点置不了只读，若它曾是主库则形成双写。故统一探测。
MYSQL_BIN=""
for _b in mariadb mysql; do
  if command -v "$_b" >/dev/null 2>&1; then MYSQL_BIN="$_b"; break; fi
done
if [ -z "$MYSQL_BIN" ]; then
  log "WARN 容器内未找到 mariadb/mysql 客户端，无法置只读（存在双写风险）。"
  exit 0
fi

PORT="${LOCAL_PORT:-3306}"
H="127.0.0.1"
RETRY="${NOTIFY_RETRY:-3}"

SQL_SET_RO="SET GLOBAL read_only=1;"

TO=""
command -v timeout >/dev/null 2>&1 && TO="timeout 5"

attempt() {
  # $1=标签  $2=账号  $3=密码
  local tag="$1" u="$2" pw="$3" i out rc
  for i in $(seq 1 "$RETRY"); do
    out="$(MYSQL_PWD="$pw" $TO ${MYSQL_BIN} -h"$H" -P"$PORT" -u"$u" -N \
           --connect-timeout=3 -e "$SQL_SET_RO" 2>&1)"
    rc=$?
    if [ $rc -eq 0 ]; then
      log "OK [$tag] 已置为只读 (host=$H port=$PORT user=$u, 第${i}次尝试)"
      return 0
    fi
    log "FAIL [$tag] 第${i}/${RETRY}次 user=$u rc=$rc 错误：$out"
    [ "$i" -lt "$RETRY" ] && sleep 1
  done
  return 1
}

attempt "ha_monitor" "${LOCAL_USER:-ha_monitor}" "${LOCAL_PASS:-}" && exit 0
attempt "root" "${LOCAL_ROOT_USER:-root}" "${LOCAL_ROOT_PASS:-}" && exit 0

log "WARN 所有账号均无法置只读。本节点若仍可写且持有 VIP，存在双写风险；"
log "      管理端下个监控周期会通过 make_slave_of 再次压只读，持续失败请查 ha_check.conf 口令。"
exit 0
