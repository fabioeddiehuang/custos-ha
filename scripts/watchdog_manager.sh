#!/usr/bin/env bash
# Custos 管理器看门狗
#
# 作用：管理器挂掉时它自己不会报告自己挂了 —— 本脚本从外部探测，
#       连续失败达到阈值后自动重启容器，消除「无人知晓的不可用」。
#
# 为什么够用：VIP / keepalived 跑在**节点**上，不依赖管理器。
#       管理器挂 ≠ 业务挂，仅失去自动故障切换能力；故只需保证「快速拉起」，
#       不必上分布式选主（Raft/etcd 的运维成本远高于收益）。
#
# 用法：
#   ./scripts/watchdog_manager.sh              # dry-run：只报告，不动作（默认）
#   ./scripts/watchdog_manager.sh --apply      # 真正执行重启
#
# 建议部署（每分钟）：
#   * * * * * cd /path/to/custos-ha && ./scripts/watchdog_manager.sh --apply >> logs/watchdog.log 2>&1
#
# 更可靠：放在**另一台机器**上跑并探测管理器的真实内网 IP，
#         否则管理器所在宿主机宕机时本脚本一起消失（那时需人工介入）。
#
# 可调（环境变量）：
#   CUSTOS_URL        探测地址，默认 http://127.0.0.1:28888
#   CUSTOS_CONTAINER  容器名，默认 custos-server
#   CUSTOS_COMPOSE_DIR  compose 目录（设置后优先用 `docker compose up -d` 拉起）
#   CUSTOS_FAIL_N     连续失败次数阈值，默认 3
#   CUSTOS_TIMEOUT    单次探测超时秒，默认 5
#   CUSTOS_ALERT_MAILTO      默认告警通道：邮件。设置后用系统 mail 命令发送（需系统有 mail）。
#                            推荐优先配置它作为默认通道，最稳、不依赖外网 IM 服务。
#   CUSTOS_ALERT_WEBHOOK     可选叠加通道：告警 webhook URL（钉钉/企微/飞书/通用）。为空则不发。
#   CUSTOS_ALERT_WEBHOOK_TYPE  generic(默认) | dingtalk | wecom | feishu
#   CUSTOS_ALERT_EVENTS      触发告警的事件：action,recover,all（逗号分隔，默认 action,recover）

set -uo pipefail

APPLY=0
for a in "$@"; do [ "$a" = "--apply" ] && APPLY=1; done

URL="${CUSTOS_URL:-http://127.0.0.1:28888}"
CONTAINER="${CUSTOS_CONTAINER:-custos-server}"
COMPOSE_DIR="${CUSTOS_COMPOSE_DIR:-}"
FAIL_N="${CUSTOS_FAIL_N:-3}"
TIMEOUT="${CUSTOS_TIMEOUT:-5}"
STATE_FILE="${CUSTOS_STATE_FILE:-/tmp/custos_watchdog.state}"
LOG_FILE="${CUSTOS_LOG_FILE:-logs/watchdog.log}"
WEBHOOK="${CUSTOS_ALERT_WEBHOOK:-}"
WEBHOOK_TYPE="${CUSTOS_ALERT_WEBHOOK_TYPE:-generic}"
MAILTO="${CUSTOS_ALERT_MAILTO:-}"
ALERT_EVENTS="${CUSTOS_ALERT_EVENTS:-action,recover}"

mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*" | tee -a "$LOG_FILE" 2>/dev/null; }

# --- 告警投递 ---
json_esc() {
  local s="$1"
  s="${s//\\/\\\\}"; s="${s//\"/\\\"}"; s="${s//$'\n'/\\n}"; s="${s//$'\t'/\\t}"
  printf '"%s"' "$s"
}
alert() {
  local msg="$1"; local event="$2"
  # 事件过滤：仅在 ALERT_EVENTS 含该事件或 all 时发送
  case ",${ALERT_EVENTS}," in
    *",${event},"*|*",all,"*) ;;
    *) return 0 ;;
  esac
  # 默认通道：邮件优先。邮件是默认推荐的告警方式（最稳、不依赖外网 IM 服务）。
  if [ -n "$MAILTO" ] && command -v mail >/dev/null 2>&1; then
    echo "$msg" | mail -s "Custos 管理器告警" "$MAILTO" >/dev/null 2>&1 || true
  fi
  # 可选叠加通道：webhook（钉钉/企微/飞书/通用）。与邮件互补，二者可同时触发。
  if [ -n "$WEBHOOK" ]; then
    local body
    case "$WEBHOOK_TYPE" in
      dingtalk) body="{\"msgtype\":\"text\",\"text\":{\"content\":$(json_esc "$msg")}}" ;;
      wecom)    body="{\"msgtype\":\"text\",\"content\":{\"content\":$(json_esc "$msg")}}" ;;
      feishu)   body="{\"msg_type\":\"text\",\"content\":{\"text\":$(json_esc "$msg")}}" ;;
      *)        body="{\"text\":$(json_esc "$msg"),\"title\":\"Custos 管理器看门狗\"}" ;;
    esac
    # 发送失败不影响看门狗主逻辑（不能因告警通道故障而崩溃）
    curl -s --max-time 8 -H 'Content-Type: application/json' -X POST \
         -d "$body" "$WEBHOOK" >/dev/null 2>&1 || true
  fi
}

# --- 探测 ---
# /login 无需登录即可 200；未登录访问其它路径会 307 到 /login —— 307 同样说明进程活着。
# 注意：连接失败时 curl 本身就会输出 000 且退出码非 0；
# 若再 `|| echo 000` 会拼成 "000000"，故只在输出为空时兜底。
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time "$TIMEOUT" "$URL/login" 2>/dev/null)
[ -z "$code" ] && code=000

case "$code" in
  200|301|302|307|308) healthy=1 ;;
  *) healthy=0 ;;
esac

fail=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
case "$fail" in ''|*[!0-9]*) fail=0 ;; esac

if [ "$healthy" = "1" ]; then
  if [ "$fail" != "0" ]; then
    log "恢复：HTTP $code（此前连续失败 ${fail} 次），计数清零"
    alert "【Custos 管理器看门狗】管理器已恢复（此前连续失败 ${fail} 次）。$(ts)" recover
  fi
  echo 0 > "$STATE_FILE" 2>/dev/null || true
  exit 0
fi

fail=$((fail + 1))
echo "$fail" > "$STATE_FILE" 2>/dev/null || true
log "探测失败 ${fail}/${FAIL_N}：HTTP ${code}（$URL）"

if [ "$fail" -lt "$FAIL_N" ]; then
  exit 0
fi

# --- 达到阈值：拉起 ---
if [ "$APPLY" != "1" ]; then
  log "DRY-RUN：已达到阈值，若加 --apply 将拉起 ${CONTAINER}（未实际执行）"
  exit 0
fi

log "达到阈值，尝试拉起 ${CONTAINER}"
if [ -n "$COMPOSE_DIR" ] && [ -f "$COMPOSE_DIR/docker-compose.yml" ]; then
  out=$(cd "$COMPOSE_DIR" && docker compose up -d 2>&1)
else
  out=$(docker restart "$CONTAINER" 2>&1)
fi
rc=$?
log "拉起结果(rc=$rc)：$(echo "$out" | tail -3 | tr '\n' ' ')"
alert "【Custos 管理器看门狗】管理器无响应（连续失败 ${FAIL_N} 次，HTTP ${code}），已自动重启容器 ${CONTAINER}。$(ts)" action

echo 0 > "$STATE_FILE" 2>/dev/null || true
