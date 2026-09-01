#!/usr/bin/env bash
# ============================================================================
# Uninstall-mariadb-docker-Node.sh —— 容器化 MariaDB 节点卸载（交互式）
# ----------------------------------------------------------------------------
# 用途：卸载容器化 MariaDB 节点，自动识别两种部署来源：
#   A) 纯 compose 部署【当前默认】：compose 内含 mariadb + keepalived 两个服务，
#      部署方式就是 `docker compose up -d`。
#   B) Install-mariadb-docker-Node.sh 脚本部署【仅「裸机 + 容器混部」场景】：
#      MariaDB 容器由脚本以 host 网络单独拉起，compose 只跑 keepalived。
#
# 卸载内容：
#   • 连接容器内 MariaDB 清理复制通道 / HA 账号 / 远程 root / ha_local
#   • 停止并删除 MariaDB 容器 + keepalived 容器
#   • 回退防火墙（数据库端口 + VRRP），兼容 firewalld(Anolis/RHEL) 与 ufw/iptables(Ubuntu)
#   • 【默认保留】数据目录（便于重装后直接复用数据），加 -f 一并删除
#
# 用法（在节点目录内，以 root 执行）：
#       sudo bash Uninstall-mariadb-docker-Node.sh        # 交互式（默认保留数据目录）
#       sudo bash Uninstall-mariadb-docker-Node.sh -f     # 同时删除数据目录
#       sudo bash Uninstall-mariadb-docker-Node.sh -y     # 跳过确认（配合 -f 可无人值守）
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FORCE_DEL_DATA=0
ASSUME_YES=0
for a in "$@"; do
  case "$a" in
    -f) FORCE_DEL_DATA=1 ;;
    -y) ASSUME_YES=1 ;;
    -h|--help) sed -n '3,24p' "$0"; exit 0 ;;
  esac
done

log(){ echo -e "\033[1;32m[mariadb-docker-uninstall]\033[0m $*"; }
warn(){ echo -e "\033[1;33m[warn]\033[0m $*"; }
err(){ echo -e "\033[1;31m[error]\033[0m $*"; }

if [[ $EUID -ne 0 ]]; then
  err "请使用 root 执行：sudo bash Uninstall-mariadb-docker-Node.sh"; exit 1
fi

# ---------- 默认常量（与 node_gen.py 保持一致）----------
DEFAULT_DATA_DIR="/data/custos-ha/mariadb12"
DEFAULT_IMAGE="mariadb:12.3.2"
CT_NAME="mariadb-ha-node"
COMPOSE_SVC="mariadb"
STATE_DIR=/etc/ha-mysql-node
STATE="$STATE_DIR/install-state-mariadb-docker"

# ---------- 探测 compose ----------
COMPOSE_BIN=""
if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_BIN="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_BIN="docker-compose"
  fi
fi

COMPOSE_FILE=""
for f in docker-compose.yaml docker-compose.yml; do
  if [[ -f "$f" ]]; then COMPOSE_FILE="$f"; break; fi
done

# ---------- 判定部署方式 ----------
# compose 里存在独立的 mariadb 服务 => 纯 compose 部署；否则视为脚本部署（只跑 keepalived）。
DEPLOY_MODE="script"
if [[ -n "$COMPOSE_FILE" ]] && grep -qE '^[[:space:]]+mariadb:[[:space:]]*$' "$COMPOSE_FILE"; then
  DEPLOY_MODE="compose"
fi

# ---------- 端口/口令：优先取服务端下发的 ha_check.conf（权威值）----------
PORT=3306
LOCAL_ROOT_PASS=""
if [[ -f keepalived/ha_check.conf ]]; then
  # 该文件由服务端生成且已做 shell 转义，可安全 source
  # shellcheck disable=SC1091
  source keepalived/ha_check.conf 2>/dev/null || warn "读取 keepalived/ha_check.conf 失败，回退缺省值"
  PORT="${LOCAL_PORT:-3306}"
  LOCAL_ROOT_PASS="${LOCAL_ROOT_PASS:-}"
fi

DATA_DIR="$DEFAULT_DATA_DIR"
MARIA_IMAGE="$DEFAULT_IMAGE"
# 脚本部署的状态文件优先（其中记录的是当时真实的容器名/镜像/数据目录）
if [[ -f "$STATE" ]]; then
  while IFS='=' read -r k v; do
    case "$k" in
      CT_NAME) CT_NAME="$v" ;;
      MARIA_IMAGE) MARIA_IMAGE="$v" ;;
      DATA_DIR) DATA_DIR="$v" ;;
      PORT) PORT="$v" ;;
      LOCAL_ROOT_PASS) LOCAL_ROOT_PASS="$v" ;;
    esac
  done < "$STATE"
  log "已读取脚本部署状态：$STATE"
fi

# ---------- 交互确认 ----------
cat <<EOF

即将执行以下卸载操作（容器化 MariaDB 节点）：
  部署方式：$( [[ "$DEPLOY_MODE" == "compose" ]] && echo "纯 compose（含 mariadb + keepalived 服务）" || echo "安装脚本部署（MariaDB 由脚本单独拉起）" )
  1. 清理复制通道、删除 HA 账号/远程 root、删除 ha_local（需容器仍在运行）
  2. 停止并删除 MariaDB 容器（$MARIA_IMAGE）
  3. 停止 keepalived 客户端容器、回退防火墙（端口 $PORT/tcp + VRRP）
  4. 数据目录 $DATA_DIR：$( [[ $FORCE_DEL_DATA -eq 1 ]] && echo "【将删除】" || echo "【默认保留，便于重装复用；加 -f 可删除】" )

EOF
if [[ $ASSUME_YES -ne 1 ]]; then
  read -r -p "确认卸载以上内容？[Y/n] " ans
  case "$ans" in
    n|N|no|NO) log "已取消卸载"; exit 0 ;;
  esac
fi

# ---------- 1) SQL 清理（容器仍在运行才可能连上）----------
# 统一入口：把「在容器内执行」抽象成 run_in_db()，屏蔽 compose/脚本两种部署差异。
DB_READY=0
run_in_db() {
  if [[ $DB_READY -ne 1 ]]; then return 0; fi
  if [[ "$DEPLOY_MODE" == "compose" ]]; then
    $COMPOSE_BIN exec -T "$COMPOSE_SVC" "$CLIENT_BIN" -uroot -p"$LOCAL_ROOT_PASS" \
      -h 127.0.0.1 -P "$PORT" --protocol=tcp "$@"
  else
    docker exec "$CT_NAME" "$CLIENT_BIN" -uroot -p"$LOCAL_ROOT_PASS" \
      -h 127.0.0.1 -P "$PORT" --protocol=tcp "$@"
  fi
}

CLIENT_BIN=""
if command -v docker >/dev/null 2>&1; then
  if [[ "$DEPLOY_MODE" == "compose" ]]; then
    if [[ -n "$COMPOSE_BIN" ]] && $COMPOSE_BIN ps --services 2>/dev/null | grep -qx "$COMPOSE_SVC"; then
      for b in mariadb mysql; do
        if $COMPOSE_BIN exec -T "$COMPOSE_SVC" sh -c "command -v $b" >/dev/null 2>&1; then
          CLIENT_BIN="$b"; break
        fi
      done
    fi
  else
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CT_NAME"; then
      for b in mariadb mysql; do
        if docker exec "$CT_NAME" sh -c "command -v $b" >/dev/null 2>&1; then
          CLIENT_BIN="$b"; break
        fi
      done
    fi
  fi
fi

if [[ -n "$CLIENT_BIN" ]]; then
  if run_in_db -e "SELECT 1" >/dev/null 2>&1; then
    DB_READY=1
    log "已连接 MariaDB，执行 SQL 清理..."
    if [[ "$DEPLOY_MODE" == "compose" ]]; then
      $COMPOSE_BIN exec -T "$COMPOSE_SVC" "$CLIENT_BIN" -uroot -p"$LOCAL_ROOT_PASS" \
        -h 127.0.0.1 -P "$PORT" --protocol=tcp <<'SQL' || warn "部分 SQL 清理跳过（请手动检查）"
STOP SLAVE IO_THREAD, SQL_THREAD;
RESET SLAVE ALL;
DROP USER IF EXISTS 'root'@'%';
DROP USER IF EXISTS 'ha_monitor'@'%';
DROP USER IF EXISTS 'replication'@'%';
DROP USER IF EXISTS 'app'@'%';
DROP DATABASE IF EXISTS ha_local;
SET GLOBAL read_only = OFF;
FLUSH PRIVILEGES;
SQL
    else
      docker exec -i "$CT_NAME" "$CLIENT_BIN" -uroot -p"$LOCAL_ROOT_PASS" \
        -h 127.0.0.1 -P "$PORT" --protocol=tcp <<'SQL' || warn "部分 SQL 清理跳过（请手动检查）"
STOP SLAVE IO_THREAD, SQL_THREAD;
RESET SLAVE ALL;
DROP USER IF EXISTS 'root'@'%';
DROP USER IF EXISTS 'ha_monitor'@'%';
DROP USER IF EXISTS 'replication'@'%';
DROP USER IF EXISTS 'app'@'%';
DROP DATABASE IF EXISTS ha_local;
SET GLOBAL read_only = OFF;
FLUSH PRIVILEGES;
SQL
    fi
    log "SQL 清理完成（复制/HA 账号/ha_local 已移除）"
  else
    warn "无法用已知密码连接容器内 MariaDB（可能已改密或容器未就绪），跳过 SQL 清理，仅做容器/文件级卸载"
  fi
else
  warn "未找到运行中的 MariaDB 容器，跳过 SQL 清理"
fi

# ---------- 2) 停止并删除容器 ----------
if command -v docker >/dev/null 2>&1; then
  if [[ "$DEPLOY_MODE" == "compose" ]]; then
    # 纯 compose：一条 down 同时停掉 mariadb + keepalived（并移除默认网络）
    if [[ -n "$COMPOSE_BIN" && -n "$COMPOSE_FILE" ]]; then
      $COMPOSE_BIN down --remove-orphans >/dev/null 2>&1 \
        && log "已停止并移除 compose 服务（mariadb + keepalived）" \
        || warn "compose down 失败，请手动：$COMPOSE_BIN down"
    fi
  else
    # 脚本部署：先删 MariaDB 容器，再停 keepalived（compose 里只有 keepalived）
    if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$CT_NAME"; then
      docker rm -f "$CT_NAME" >/dev/null 2>&1 \
        && log "已停止并删除容器 $CT_NAME" \
        || warn "删除容器 $CT_NAME 失败，请手动：docker rm -f $CT_NAME"
    else
      warn "未找到容器 $CT_NAME（可能已删除）"
    fi
    # 注意：仅停止容器，【不删除】本目录的脚本/compose，否则同目录重装会起不来。
    if [[ -n "$COMPOSE_BIN" && -n "$COMPOSE_FILE" ]]; then
      $COMPOSE_BIN down >/dev/null 2>&1 \
        && log "已停止 keepalived 客户端容器（保留本目录脚本/compose，便于重装）" \
        || warn "keepalived 容器停止失败，请手动：$COMPOSE_BIN down"
    fi
  fi
fi

# ---------- 3) 回退防火墙 ----------
# 兼容三类：firewalld(Anolis/RHEL) / ufw(Ubuntu) / 裸 iptables
revert_port_vrrp() {
  if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active firewalld >/dev/null 2>&1; then
    firewall-cmd --permanent --remove-port="${PORT}/tcp" >/dev/null 2>&1 || true
    firewall-cmd --permanent --remove-protocol=vrrp >/dev/null 2>&1 || true
    firewall-cmd --reload >/dev/null 2>&1 || true
    log "firewalld：已移除 ${PORT}/tcp + VRRP(112)"
  elif command -v ufw >/dev/null 2>&1 && ufw status >/dev/null 2>&1; then
    ufw delete allow "${PORT}/tcp" >/dev/null 2>&1 || true
    iptables -D INPUT -p vrrp -j ACCEPT 2>/dev/null || true
    log "ufw：已移除 ${PORT}/tcp + VRRP(112)"
  else
    iptables -D INPUT -p tcp --dport "$PORT" -j ACCEPT 2>/dev/null || true
    iptables -D INPUT -p vrrp -j ACCEPT 2>/dev/null || true
    log "iptables：已移除 ${PORT}/tcp + VRRP(112)"
  fi
  # 纯 compose 部署不依赖端口转发；脚本部署可能建过 VIP 端口转发，一并清理
  if [[ -f keepalived/ha_check.conf ]]; then
    local vip_port
    vip_port="$(grep -E '^VIP_PORT=' keepalived/ha_check.conf 2>/dev/null | head -1 | cut -d= -f2 | tr -d '"' | tr -d ' ')"
    if [[ -n "${vip_port:-}" && "$vip_port" != "$PORT" ]]; then
      if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active firewalld >/dev/null 2>&1; then
        firewall-cmd --permanent --remove-forward-port="port=${vip_port}:proto=tcp:toport=${PORT}" >/dev/null 2>&1 || true
        firewall-cmd --reload >/dev/null 2>&1 || true
        log "firewalld：已移除端口转发 ${vip_port} -> ${PORT}"
      else
        iptables -t nat -D PREROUTING -p tcp --dport "$vip_port" -j REDIRECT --to-port "$PORT" 2>/dev/null || true
        log "iptables：已移除端口转发 ${vip_port} -> ${PORT}"
      fi
    fi
  fi
}
revert_port_vrrp

# ---------- 4) 数据目录 ----------
if [[ $FORCE_DEL_DATA -eq 1 ]]; then
  rm -rf "$DATA_DIR" && log "已删除数据目录 $DATA_DIR（-f）"
else
  log "已保留数据目录 $DATA_DIR（如需彻底删除：sudo bash Uninstall-mariadb-docker-Node.sh -f）"
fi

# ---------- 5) 删除状态文件 ----------
rm -f "$STATE"
rmdir "$STATE_DIR" >/dev/null 2>&1 || true

echo
log "==== 容器化 MariaDB 节点卸载完成 ===="
echo "提醒：请到管理器 Web 界面删除该节点（避免后台继续探测/选主）。"
echo "重装：纯 compose 部署直接在本目录执行 docker compose up -d 即可（数据目录保留则数据复用）。"
