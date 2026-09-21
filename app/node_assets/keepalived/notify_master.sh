#!/bin/bash
# keepalived 状态切换为 MASTER 时触发：本节点持有 VIP，置为可写主库（关闭 read_only）。
#
# ============ v2.2.12 彻底修复（必读）============
# 旧实现只有一行：
#     mysql ... -e "SET GLOBAL read_only=0;" >/dev/null 2>&1 || true
# 把 1045(密码错) / 1227(权限不足) / 2003(连不上) 等【全部】静默吞掉并 exit 0。
# 后果：keepalived 以为「置可写成功」、VIP 留在本机，而本机其实仍是只读，
#       业务经 VIP 写入必报：
#         1290 - The MariaDB server is running with the --read-only=ON option
#       且排障时日志里一片空白（错误全被 2>/dev/null 吞掉），极难定位。
# 这正是「切到某节点为主后，客户端建库报 1290」的真凶：该节点的
# ha_check.conf 里 LOCAL_PASS 与实际账号脱节（密码漂移），置可写静默失败。
#
# 现在的策略：
#   1) 多重回退账号：ha_monitor(LOCAL_PASS，可能漂移) -> root@127.0.0.1
#      (LOCAL_ROOT_PASS，随节点固定、不漂移)，任一成功即可；
#   2) 每个账号重试 RETRY 次，规避瞬时的连接/锁等待；
#   3) 真实错误【完整落盘】到 /var/log/keepalived_notify.log，不再吞掉；
#   4) 先关 super_read_only 再关 read_only —— MySQL 规定 super_read_only=ON 时
#      不允许直接关 read_only（关了也会被立刻拉回），必须先关前者；
#      MariaDB 无该变量，语句报错属正常，忽略即可；
#   5) 全部失败 -> exit 1。keepalived 的 check_mysql_master.sh 有 read_only 判定，
#      下一轮（约 6s）会判定本机只读而放弃 VIP，让 VIP 漂到真正可写的节点，
#      而不是把业务一直钉死在一个只读的「假主」上。
#   6) 防脑裂：若本机 ha_local.state 明确指向【别的节点】，说明本次升主与
#      管理端记录冲突，拒绝置可写并退出，避免双写。
# ==================================================
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
  echo "$(date '+%F %T') [notify_master] $*" >>"$LOG" 2>/dev/null
  echo "$(date '+%F %T') [notify_master] $*" >&2
}

# ---- 客户端命令探测（MariaDB 12.x 关键适配）----
# MariaDB 12.x 官方镜像只提供 `mariadb`（`mysql` 兼容别名已移除），而 Debian 的
# default-mysql-client 可能只提供 `mysql`。硬编码 `mysql` 在纯 MariaDB 环境下就是
# `command not found` —— 旧版写法 `... || true` 会把这种失败静默吞掉，
# 节点永远置不了可写，业务经 VIP 写入必报 1290。故统一探测并明确报错。
MYSQL_BIN=""
for _b in mariadb mysql; do
  if command -v "$_b" >/dev/null 2>&1; then MYSQL_BIN="$_b"; break; fi
done
if [ -z "$MYSQL_BIN" ]; then
  log "FATAL 容器内未找到 mariadb/mysql 客户端，无法置可写。"
  log "       请在 keepalived 镜像中安装 mariadb-client（或 default-mysql-client）后重建。"
  exit 1
fi

PORT="${LOCAL_PORT:-3306}"
H="127.0.0.1"
RETRY="${NOTIFY_RETRY:-3}"

TO=""
command -v timeout >/dev/null 2>&1 && TO="timeout 5"

# ---- 防脑裂：确认本机确实是管理端记录的当前主 ----
# 读失败 / 为空 -> 放行（由 read_only 兜底，避免管理端抖动误杀 VIP）；
# 明确指向别的节点 -> 拒绝置可写。
CM=""
if [ -n "${LOCAL_ROOT_PASS:-}" ]; then
  CM="$(MYSQL_PWD="${LOCAL_ROOT_PASS}" $TO ${MYSQL_BIN} -h"$H" -P"$PORT" \
        -u"${LOCAL_ROOT_USER:-root}" -N --connect-timeout=3 \
        -e "SELECT v FROM ha_local.state WHERE k='current_master'" 2>&1)"
fi
case "$CM" in
  *ERROR*|*"Access denied"*|*"Can't connect"*|*"Unknown table"*)
    log "WARN 读取 ha_local.state 异常（放行，由 read_only 兜底）：$CM"
    CM=""
    ;;
esac
if [ -n "$CM" ]; then
  cm_ip="${CM%%:*}"
  MY_IP="${NODE_IP:-${LOCAL_HOST:-127.0.0.1}}"
  if [ "$cm_ip" != "$MY_IP" ] && [ "$cm_ip" != "${LOCAL_HOST:-127.0.0.1}" ] \
     && [ "$CM" != "${MY_IP}:${PORT}" ] && [ "$CM" != "${LOCAL_HOST:-127.0.0.1}:${PORT}" ]; then
    log "FATAL 拒绝置可写：本机 ha_local.state 记录 current_master=$CM，"
    log "      与本机(${MY_IP}:${PORT})不符，存在脑裂风险。请检查管理端主库记录。"
    exit 1
  fi
  log "OK 校验通过：current_master=$CM 命中本机，允许置可写"
fi

# ---- 尝试置可写：多重回退 + 重试 ----
attempt() {
  # $1=标签  $2=账号  $3=密码
  local tag="$1" u="$2" pw="$3" i out rc
  if [ -z "$pw" ]; then
    # 典型诱因：ha_check.conf 中 *_PASS 未做 shell 转义，口令含 空格/#/$/引号 时被
    # 截断或整行变注释，source 后变量为空 -> 以空口令连接，报
    #   ERROR 1045: Access denied for user 'root'@'localhost' (using password: NO)
    log "FAIL [$tag] 口令为空！ha_check.conf 中该账号的口令变量为空或已被 shell 解析破坏。"
    log "       请检查 /etc/keepalived/ha_check.conf 的 LOCAL_PASS / LOCAL_ROOT_PASS；"
    log "       若口令含 空格 # \$ ' 等特殊字符，须在管理端【重新生成节点目录】并重新下发 keepalived/ 目录。"
    return 1
  fi
  for i in $(seq 1 "$RETRY"); do
    # 必须拆成两次独立调用，绝不能写成 `-e "SET ...; SET ...;"`！
    # 客户端执行多语句时，第一条报错就会中止、后续语句【根本不会执行】。
    # MariaDB 没有 super_read_only，第一句必然报 Unknown system variable —— 若合并执行，
    # 真正的 `SET GLOBAL read_only=0` 将永远执行不到，节点依旧只读 → 业务报 1290。
    # 故：super_read_only 只在 MySQL 8.0 上有意义，失败一律忽略；
    #     以 `SET GLOBAL read_only=0` 的退出码作为成败依据。
    MYSQL_PWD="$pw" $TO ${MYSQL_BIN} -h"$H" -P"$PORT" -u"$u" -N \
      --connect-timeout=3 -e "SET GLOBAL super_read_only=0" >/dev/null 2>&1
    out="$(MYSQL_PWD="$pw" $TO ${MYSQL_BIN} -h"$H" -P"$PORT" -u"$u" -N \
           --connect-timeout=3 -e "SET GLOBAL read_only=0" 2>&1)"
    rc=$?
    if [ $rc -eq 0 ]; then
      log "OK [$tag] 已置为可写 (host=$H port=$PORT user=$u, 第${i}次尝试)"
      return 0
    fi
    log "FAIL [$tag] 第${i}/${RETRY}次 user=$u rc=$rc 错误：$out"
    [ "$i" -lt "$RETRY" ] && sleep 1
  done
  return 1
}

# 1) ha_monitor：正统账号，但密码可能与管理端脱节（漂移）
attempt "ha_monitor" "${LOCAL_USER:-ha_monitor}" "${LOCAL_PASS:-}" && exit 0

# 2) root@127.0.0.1：ha_check.conf 中的节点 root 口令，不随 HA_PASS 漂移，兜底用
attempt "root" "${LOCAL_ROOT_USER:-root}" "${LOCAL_ROOT_PASS:-}" && exit 0

log "FATAL 所有账号均无法置可写（VIP 将停留在本只读节点上）。"
log "      业务经 VIP 写入会报 1290 (--read-only=ON)。"
log "      请检查：① ha_check.conf 的 LOCAL_PASS/LOCAL_ROOT_PASS 是否与实际口令一致；"
log "              ② 账号是否具备 SUPER / READ_ONLY ADMIN；③ 本机 MySQL 是否存活。"
log "      提示：在管理端重新生成该节点目录并重新下发 keepalived 目录即可刷新口令。"
exit 1
