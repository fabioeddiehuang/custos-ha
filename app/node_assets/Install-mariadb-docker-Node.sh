#!/usr/bin/env bash
# ============================================================================
# Install-mariadb-docker-Node.sh —— 容器化 MariaDB 节点部署（node_type=mariadb-docker）
# ----------------------------------------------------------------------------
# 适用：
#   1) 目标机操作系统过老（如 CentOS 7 / glibc 2.17、Ubuntu 18.04 / glibc 2.27），
#      MariaDB 12.3.3 的 linux-systemd 裸机二进制要求 glibc≥2.28，直接报
#      "... version `GLIBC_2.28' not found" 且无法启动；
#   2) 目标机不想/不能装 MariaDB（要脱离面板、保持环境干净），直接用官方镜像跑。
#
# 原理：用官方 MariaDB 容器（自带运行时，不受宿主 glibc 限制）跑 MariaDB，
#       复用本节点目录已有的 docker-my.cnf / init_mariadb.sql / fix-node-creds.sql /
#       keepalived 客户端。对管理器而言与「裸机 MariaDB」节点行为完全一致：
#       同一个 MariaDB GTID 复制路径（gtid_domain_id + MASTER_USE_GTID=slave_pos）、
#       同样的 VIP 探活（127.0.0.1:port）、同样的切换逻辑。
#       ⇒ 因此容器化 MariaDB 与裸机 MariaDB **可以同集群混部**（见 mysql_compat.node_family）。
#
# 关键点：
#   - --network host：容器直接占用宿主端口，与裸机 MariaDB 行为一致，
#     keepalived 用 127.0.0.1:<port> 探活即可（无需端口映射）。
#   - --user root：mariadb:*-ubi 镜像默认 USER=mysql(uid 999)，无法 chown 宿主数据目录，
#     导致 "Can't create/write to file ... (Errcode: 13 Permission denied)" 起不来；
#     以 root 启动后入口脚本会自动 chown 数据目录再降权到 mysql。
#   - 配置双挂：UBI 构建读 /etc/my.cnf.d/*.cnf，Debian 构建读 /etc/mysql/conf.d/*.cnf。
#     同一个文件挂到两个目录，使 12.3.3-ubi 与 12.3.2 任一镜像都生效（99- 前缀最后加载）。
#
# 前置：docker 已安装（compose 可选，仅用于拉起 keepalived）。
# 用法： sudo bash Install-mariadb-docker-Node.sh
# 可选环境变量：
#   MARIA_IMAGE=mariadb:12.3.3-ubi         # 主镜像（拉取失败自动退 12.3.2）
#   MARIA_IMAGE_FALLBACK=mariadb:12.3.2    # 备用镜像
#   MARIA_DATA_DIR=/data/mariadb           # 宿主数据目录（与裸机 MariaDB 保持一致）
#   CT_NAME=mariadb-ha-node                # 容器名
#   MARIA_BUFFER_POOL=1G                   # 覆盖 innodb_buffer_pool_size（小内存机务必调小）
#                                          # 建议 ≈ 物理内存的 40%~50%，如 2G 内存用 512M
# ============================================================================
set -euo pipefail
set -E

# ---------- 日志 / 错误处理 ----------
log(){ echo -e "\033[1;32m[info]\033[0m $*"; }
warn(){ echo -e "\033[1;33m[warn]\033[0m $*"; }
err(){ echo -e "\033[1;31m[error]\033[0m $*"; }

_REPORTED=0
die(){ _REPORTED=1; err "$*"; exit 1; }
# ERR trap 精确定位失败行与失败命令（EXIT trap 的 BASH_LINENO 恒 0、DEBUG trap 的
# $LINENO 恒 1，均不可用；ERR trap 的 $LINENO/$BASH_COMMAND 才是真正的失败点）。
trap 'rc=$?; if [[ $_REPORTED -eq 0 ]]; then _REPORTED=1; err "脚本在第 $LINENO 行执行失败（退出码 $rc）：${BASH_COMMAND:0:200}"; fi' ERR
trap 'rc=$?; if [[ $rc -ne 0 && $_REPORTED -eq 0 ]]; then err "脚本意外退出：退出码 $rc（无更多上下文）"; fi' EXIT

# 安全读取 KEY=VALUE：文件不存在 / 键缺失 / 无匹配时返回空串，且绝不触发 set -e 退出
conf_get() {
  local f="$1" k="$2"
  [[ -f "$f" ]] || return 0
  grep -E "^${k}=" "$f" 2>/dev/null | tail -1 | cut -d= -f2- \
    | sed -E "s/^'//; s/'$//" || true
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || die "无法进入脚本所在目录：$SCRIPT_DIR"

[[ $EUID -eq 0 ]] || die "请使用 root 执行：sudo bash Install-mariadb-docker-Node.sh"

command -v docker >/dev/null 2>&1 \
  || die "未找到 docker。请先安装 Docker（容器自带运行时，与宿主 glibc 版本无关）。"

# ---------- 可变参数（可用环境变量覆盖） ----------
MARIA_IMAGE="${MARIA_IMAGE:-mariadb:12.3.3-ubi}"
MARIA_IMAGE_FALLBACK="${MARIA_IMAGE_FALLBACK:-mariadb:12.3.2}"
MARIA_DATA_DIR="${MARIA_DATA_DIR:-/data/mariadb}"
CT_NAME="${CT_NAME:-mariadb-ha-node}"
STATE_DIR=/etc/ha-mysql-node
STATE="$STATE_DIR/install-state-mariadb-docker"

# ---------- root 密码 / 端口（来自服务端生成的节点目录） ----------
ROOT_PASS="$(conf_get "$SCRIPT_DIR/keepalived/ha_check.conf" LOCAL_ROOT_PASS || true)"
[[ -n "$ROOT_PASS" ]] \
  || die "未在 keepalived/ha_check.conf 找到 LOCAL_ROOT_PASS，请在服务端生成的节点目录内执行本脚本。"

CONF_SRC=""
[[ -f "$SCRIPT_DIR/docker-my.cnf" ]] && CONF_SRC="$SCRIPT_DIR/docker-my.cnf"
[[ -z "$CONF_SRC" && -f "$SCRIPT_DIR/my.cnf" ]] && CONF_SRC="$SCRIPT_DIR/my.cnf"
[[ -n "$CONF_SRC" ]] \
  || die "当前目录未找到 docker-my.cnf（或 my.cnf），请在服务端生成的节点目录内执行本脚本。"

# 端口优先取配置文件，其次 ha_check.conf 的 LOCAL_PORT，最后兜底 3306
PORT="$(grep -E '^[[:space:]]*port[[:space:]]*=' "$CONF_SRC" 2>/dev/null | tail -1 | awk -F= '{print $2}' | tr -d ' ' || true)"
[[ -z "$PORT" ]] && PORT="$(conf_get "$SCRIPT_DIR/keepalived/ha_check.conf" LOCAL_PORT || true)"
PORT="${PORT:-3306}"

SERVER_ID="$(grep -E '^[[:space:]]*(server-id|server_id)[[:space:]]*=' "$CONF_SRC" 2>/dev/null | tail -1 | awk -F= '{print $2}' | tr -d ' ' || true)"
GTID_DOMAIN="$(grep -E '^[[:space:]]*gtid_domain_id[[:space:]]*=' "$CONF_SRC" 2>/dev/null | tail -1 | awk -F= '{print $2}' | tr -d ' ' || true)"

log "节点目录：$SCRIPT_DIR"
log "镜像：$MARIA_IMAGE（备用：$MARIA_IMAGE_FALLBACK）"
log "容器名：$CT_NAME   端口：$PORT   server-id：${SERVER_ID:-?}   gtid_domain_id：${GTID_DOMAIN:-?}"
log "数据目录(宿主)：$MARIA_DATA_DIR"

# ---------- 1) 生成容器用配置 ----------
# 剔除 datadir / socket / pid_file：容器内固定为 /var/lib/mysql 与 /run/mariadb/mariadb.sock，
# 沿用宿主 /data/mariadb 会导致容器起不来。
CNF_TMP="/tmp/mariadb-docker-99-ha.cnf"
grep -viE '^[[:space:]]*(datadir|socket|pid[_-]?file)[[:space:]]*=' "$CONF_SRC" > "$CNF_TMP"
grep -qiE '^[[:space:]]*port[[:space:]]*=' "$CNF_TMP" || printf '\n[mysqld]\nport = %s\n' "$PORT" >> "$CNF_TMP"
# 小内存机保护：服务端默认给 1G buffer pool，2G 内存的机器容易 OOM。
# 用 MARIA_BUFFER_POOL=512M 覆盖即可（后写的值覆盖前面的同名项）。
if [[ -n "${MARIA_BUFFER_POOL:-}" ]]; then
  printf '\n[mysqld]\ninnodb_buffer_pool_size = %s\n' "$MARIA_BUFFER_POOL" >> "$CNF_TMP"
  log "已覆盖 innodb_buffer_pool_size = $MARIA_BUFFER_POOL（由 MARIA_BUFFER_POOL 指定）"
else
  TOTAL_MEM_MB="$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo 2>/dev/null || echo 0)"
  if [[ "$TOTAL_MEM_MB" -gt 0 && "$TOTAL_MEM_MB" -lt 3000 ]]; then
    warn "本机物理内存仅约 ${TOTAL_MEM_MB}MB，而服务端生成的 buffer pool 为 1G，有 OOM 风险。"
    warn "建议改小后重跑，例如：MARIA_BUFFER_POOL=512M sudo bash $0"
  fi
fi
# 必须以 0644 落盘：容器内 mariadbd 最终以 mysql(uid 999) 运行，需要能读该挂载文件。
chmod 644 "$CNF_TMP"
log "已生成容器配置：$CNF_TMP（移除 datadir/socket/pid_file，端口 $PORT）"

# SELinux  enforcing 的发行版（CentOS/RHEL/Anolis）需给绑定挂载加 :z 重打标签，
# 否则容器内访问宿主文件会被拒（表现为配置文件不生效 / 数据目录 Permission denied）。
SELINUX_FLAG=""
if command -v getenforce >/dev/null 2>&1 && [[ "$(getenforce 2>/dev/null || true)" == "Enforcing" ]]; then
  SELINUX_FLAG=":z"
  log "检测到 SELinux=Enforcing，绑定挂载附加 :z 标签"
fi

# ---------- 2) 准备数据目录 ----------
# 宿主数据目录若有「上次裸机初始化失败」的残留（非空但没有 mysql 系统库），清理后交给容器重新初始化。
# 注意：只清「无 mysql 系统库」的目录，已有数据的目录一律保留（重装/改密是幂等操作）。
if [[ -d "$MARIA_DATA_DIR" && -n "$(ls -A "$MARIA_DATA_DIR" 2>/dev/null || true)" \
      && ! -d "$MARIA_DATA_DIR/mysql" ]]; then
  warn "$MARIA_DATA_DIR 非空但无 mysql 系统库，判定为上次初始化失败的残留，清理后由容器重新初始化："
  ls -A "$MARIA_DATA_DIR" 2>/dev/null | head -20 | sed 's/^/      /' || true
  rm -rf "$MARIA_DATA_DIR"/* "$MARIA_DATA_DIR"/.[!.]* 2>/dev/null || true
fi
mkdir -p "$MARIA_DATA_DIR"

# ---------- 3) 拉取镜像（失败自动退备用镜像） ----------
log "拉取镜像 $MARIA_IMAGE ..."
if ! docker pull "$MARIA_IMAGE" >/dev/null 2>&1; then
  warn "拉取 $MARIA_IMAGE 失败（离线或无法访问镜像仓库），尝试备用镜像 $MARIA_IMAGE_FALLBACK ..."
  if docker pull "$MARIA_IMAGE_FALLBACK" >/dev/null 2>&1; then
    MARIA_IMAGE="$MARIA_IMAGE_FALLBACK"
    log "已改用备用镜像：$MARIA_IMAGE"
  else
    die "主/备镜像均拉取失败。请确认可访问镜像仓库，或在可联网机器 docker save 后 docker load 导入本节点。"
  fi
fi

# ---------- 4) 启动容器 ----------
# host 网络下容器直接占用宿主端口：若端口已被占用（残留 mysqld / 面板 MySQL / 上次失败实例），
# 容器会直接 "Address already in use" 退出，且报错不明显，故先显式预检。
port_busy() {
  command -v ss >/dev/null 2>&1 && ss -lnt 2>/dev/null | awk '{print $4}' | grep -qE "(:${PORT})$" && return 0
  command -v netstat >/dev/null 2>&1 && netstat -lnt 2>/dev/null | awk '{print $4}' | grep -qE "(:${PORT})$" && return 0
  return 1
}
if port_busy; then
  warn "宿主端口 $PORT 已被占用（host 网络下容器无法绑定）："
  (command -v ss >/dev/null 2>&1 && ss -lntp 2>/dev/null | grep -E ":${PORT}\b") || true
  (command -v netstat >/dev/null 2>&1 && netstat -lntp 2>/dev/null | grep -E ":${PORT}\b") || true
  die "请先释放端口 $PORT（停掉占用进程），或修改 docker-my.cnf 的 port 后在管理器重新生成节点。"
fi

if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$CT_NAME"; then
  log "检测到已存在的容器 $CT_NAME，先停止并删除（数据目录 $MARIA_DATA_DIR 保留）..."
  docker rm -f "$CT_NAME" >/dev/null 2>&1 || true
fi

log "启动 MariaDB 容器（host 网络，数据挂载 $MARIA_DATA_DIR:/var/lib/mysql）..."
docker run -d --name "$CT_NAME" \
  --user root \
  --network host \
  --restart always \
  -e MARIADB_ROOT_PASSWORD="$ROOT_PASS" \
  -e TZ=Asia/Shanghai \
  -v "$CNF_TMP:/etc/my.cnf.d/99-ha.cnf:ro${SELINUX_FLAG}" \
  -v "$CNF_TMP:/etc/mysql/conf.d/99-ha.cnf:ro${SELINUX_FLAG}" \
  -v "$MARIA_DATA_DIR:/var/lib/mysql${SELINUX_FLAG}" \
  "$MARIA_IMAGE" >/dev/null

# ---------- 5) 等待就绪 ----------
# 容器内客户端：MariaDB 10.4.6+ 提供 mariadb，旧包只有 mysql，两者都试。
CLIENT_BIN=""
for b in mariadb mysql; do
  if docker exec "$CT_NAME" command -v "$b" >/dev/null 2>&1; then CLIENT_BIN="$b"; break; fi
done
[[ -n "$CLIENT_BIN" ]] || die "容器内未找到 mariadb/mysql 客户端，请检查镜像 $MARIA_IMAGE"

log "等待 MariaDB 就绪（最多 180s）..."
ready=0
for i in $(seq 1 180); do
  if docker exec "$CT_NAME" "$CLIENT_BIN" -uroot -p"$ROOT_PASS" -h 127.0.0.1 -P "$PORT" \
       --protocol=tcp -e "SELECT 1" >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 1
done
if [[ $ready -ne 1 ]]; then
  err "MariaDB 容器未能在 180s 内就绪，最近日志如下："
  docker logs --tail 40 "$CT_NAME" 2>&1 | sed 's/^/      /' || true
  die "请检查：docker logs $CT_NAME（常见原因：宿主 $PORT 端口被占用 / 数据目录权限 / SELinux）"
fi
log "MariaDB 已就绪（$CLIENT_BIN 客户端可用）。"

# ---------- 6) 执行初始化 / 账号修复 SQL ----------
run_sql() {
  docker exec -i "$CT_NAME" "$CLIENT_BIN" -uroot -p"$ROOT_PASS" -h 127.0.0.1 -P "$PORT" --protocol=tcp
}
if [[ -f init_mariadb.sql ]]; then
  log "执行 init_mariadb.sql ..."
  run_sql < init_mariadb.sql || warn "init_mariadb.sql 执行有告警，请检查"
fi
if [[ -f fix-node-creds.sql ]]; then
  log "执行 fix-node-creds.sql ..."
  run_sql < fix-node-creds.sql || warn "fix-node-creds.sql 执行有告警，请检查"
fi

# ---------- 7) 防火墙（MySQL 端口 + VRRP 112） ----------
allow_port_vrrp() {
  if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active firewalld >/dev/null 2>&1; then
    firewall-cmd --permanent --add-port="${PORT}/tcp" >/dev/null 2>&1 || true
    firewall-cmd --permanent --add-protocol=vrrp >/dev/null 2>&1 || true
    firewall-cmd --reload >/dev/null 2>&1 || true
    log "firewalld: 已放行 ${PORT}/tcp + VRRP(112)"
  elif command -v ufw >/dev/null 2>&1 && ufw status >/dev/null 2>&1; then
    ufw allow "${PORT}/tcp" >/dev/null 2>&1 || true
    iptables -I INPUT -p vrrp -j ACCEPT 2>/dev/null || true
    log "ufw: 已放行 ${PORT}/tcp + VRRP(112)"
  else
    iptables -I INPUT -p tcp --dport "$PORT" -j ACCEPT 2>/dev/null || true
    iptables -I INPUT -p vrrp -j ACCEPT 2>/dev/null || true
    log "iptables: 已放行 ${PORT}/tcp + VRRP(112)"
  fi
}
allow_port_vrrp

# ---------- 8) 拉起 keepalived 客户端 ----------
# 本目录的 docker-compose.yaml 仅含 keepalived（host 网络探活 127.0.0.1:$PORT）。
COMPOSE_BIN=""
if docker compose version >/dev/null 2>&1; then
  COMPOSE_BIN="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_BIN="docker-compose"
fi
if [[ -f docker-compose.yaml || -f docker-compose.yml ]]; then
  if [[ -n "$COMPOSE_BIN" ]]; then
    log "拉起 keepalived 客户端（$COMPOSE_BIN up -d）..."
    $COMPOSE_BIN up -d || warn "keepalived 启动失败，请手动：cd $SCRIPT_DIR && $COMPOSE_BIN up -d"
  else
    warn "未找到 docker compose / docker-compose，keepalived 未启动；请安装 Compose V2 后手动拉起。"
  fi
else
  warn "未找到 docker-compose.yaml（keepalived 客户端）；如该节点不需要 VIP 可忽略。"
fi

# ---------- 9) 状态文件（供卸载脚本还原） ----------
mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
{
  printf 'NODE_TYPE=mariadb-docker\n'
  printf 'CT_NAME=%q\n' "$CT_NAME"
  printf 'MARIA_IMAGE=%q\n' "$MARIA_IMAGE"
  printf 'DATA_DIR=%q\n' "$MARIA_DATA_DIR"
  printf 'PORT=%q\n' "$PORT"
  printf 'LOCAL_ROOT_PASS=%q\n' "$ROOT_PASS"
} > "$STATE"
chmod 600 "$STATE"

echo
log "==== 容器化 MariaDB 节点部署完成 ===="
log "  容器：$CT_NAME（$MARIA_IMAGE）  数据：$MARIA_DATA_DIR  端口：$PORT"
log "  VIP 探活：127.0.0.1:$PORT"
echo "下一步：到管理器刷新该节点状态；若长时间 offline，请检查："
echo "  docker logs $CT_NAME   与   $COMPOSE_BIN logs（keepalived）"
echo "如需卸载：sudo bash Uninstall-mariadb-docker-Node.sh"
