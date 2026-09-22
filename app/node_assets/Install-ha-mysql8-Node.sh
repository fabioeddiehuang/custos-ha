#!/usr/bin/env bash
# =============================================================
# Install-ha-mysql8-Node.sh  —  裸机 MySQL 8.0 节点【唯一】全量安装脚本
# -------------------------------------------------------------
# 用途：在「裸机 MySQL 8.0」类型节点上，安装/确认 MySQL 8.0 → 落盘服务端生成的
#       my.cnf → 初始化数据目录 → systemd 托管拉起 → 按【创建节点时填写的 root 密码】
#       设置本机 root 口令 → 执行 init.sql（建 ha_monitor/replication/app 账号）
#       → 放行防火墙 → 拉起 keepalived 客户端与节点 agent。
#
# 用法（在节点目录内，以 root 执行）：
#       sudo bash Install-ha-mysql8-Node.sh
#
# 幂等说明：再次执行即为「更新」——不重新初始化数据目录，只覆盖配置、更新 root
#       密码、重放账号脚本并重启服务（旧密码从安装状态文件读取，故支持改密后重装）。
#
# 注意：本脚本只适用于「裸机 MySQL 8.0」（node_type=mysql8）；容器形态
#       （docker / mysql8-docker）不需要本脚本，直接 docker compose up -d 即可。
# =============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 节点包目录归整：本脚本位于 <节点>/mysql8.0/scripts/，故：
#   NODE_ROOT = <节点>/                （keepalived/ 与 docker-compose.yaml 所在）
#   CONF_DIR  = <节点>/mysql8.0/conf/  （服务端生成的 my.cnf / init.sql 等）
NODE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONF_DIR="$SCRIPT_DIR/../conf"
cd "$NODE_ROOT"

# ---------- 可手动替换的下载链接（系统仓库无 MySQL 8.0 包时使用） ----------
MYSQL_DOWNLOAD_URL="${MYSQL_DOWNLOAD_URL:-https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.46-linux-glibc2.28-x86_64.tar.xz}"

# ---------- 安装路径 / 数据目录 ----------
# ⚠ DATA_DIR 必须与服务端生成的 my.cnf 里的 datadir【完全一致】
#   （render_mysql_my_cnf 默认 /data/custos-ha/mysql8）；日志目录由它的基名推导，
#   两者不一致会导致 mysqld 写不了 error.log 而启动失败。
INSTALL_DIR="${MYSQL_INSTALL_DIR:-/opt/mysql}"
DATA_DIR="${MYSQL_DATA_DIR:-/data/custos-ha/mysql8}"
# 统一日志根目录：/data/custos-ha/logs/<数据目录基名>/ —— 三种形态（MySQL 容器 /
# MariaDB 裸机 / MariaDB 容器）排错时都按这一路径找日志。
LOG_ROOT="/data/custos-ha/logs"
LOG_DIR="$LOG_ROOT/$(basename "$DATA_DIR")"
SYSTEMD_UNIT="mysql8-ha.service"
CONF_TARGET="/etc/mysql/my.cnf"
STATE_DIR=/etc/ha-mysql-node
STATE="$STATE_DIR/install-state-mysql8"

log(){ echo -e "\033[1;32m[mysql8-install]\033[0m $*"; }
warn(){ echo -e "\033[1;33m[warn]\033[0m $*"; }
err(){ echo -e "\033[1;31m[error]\033[0m $*"; }

# 静默退出兜底诊断（set -euo pipefail 下 grep 无匹配会终止脚本且无输出，极难定位）
_REPORTED=0
die(){ _REPORTED=1; err "$*"; exit 1; }
set -E
trap 'rc=$?; if [[ $_REPORTED -eq 0 ]]; then _REPORTED=1; err "脚本在第 $LINENO 行执行失败（退出码 $rc）：${BASH_COMMAND:0:200}"; fi' ERR
trap 'rc=$?; if [[ $rc -ne 0 && $_REPORTED -eq 0 ]]; then err "脚本意外退出：退出码 $rc（无更多上下文）"; fi' EXIT

# 安全读取 KEY=VALUE：文件缺失 / 键缺失时返回空串，绝不触发 set -e 退出
conf_get() {
  local f="$1" k="$2"
  [[ -f "$f" ]] || return 0
  grep -E "^${k}=" "$f" 2>/dev/null | tail -1 | cut -d= -f2- \
    | sed -E "s/^'//; s/'$//" || true
}

if [[ $EUID -ne 0 ]]; then
  die "请使用 root 执行：sudo bash Install-ha-mysql8-Node.sh"
fi

# ---------- 0) 环境依赖 ----------
for bin in curl tar; do
  command -v "$bin" >/dev/null 2>&1 || { die "缺少必要命令：$bin，请先安装（apt/yum install $bin）"; }
done
# MySQL 8.0 需要 libaio（缺失会启动即报错）
if ! ldconfig -p 2>/dev/null | grep -q "libaio.so"; then
  warn "未检测到 libaio（MySQL 8.0 运行必需）。尝试自动安装…"
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update >/dev/null 2>&1 && apt-get install -y libaio1 2>/dev/null || apt-get install -y libaio1 || true
  elif command -v yum >/dev/null 2>&1; then
    yum install -y libaio 2>/dev/null || true
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y libaio 2>/dev/null || true
  fi
  ldconfig -p 2>/dev/null | grep -q "libaio.so" \
    || die "libaio 安装失败，请手动安装（apt install libaio1 / yum install libaio）后再运行。"
fi

# ---------- 1) 安装 MySQL 8.0（已装则跳过）----------
# 函数必须先定义后调用：bash 是顺序执行，定义在调用之后会 command not found。
fetch_tarball() {
  local url="$MYSQL_DOWNLOAD_URL"
  local tar="/tmp/$(basename "$url")"
  log "获取 MySQL 8.0 安装包（glibc2.28+）：$url"
  ( command -v curl >/dev/null 2>&1 && curl -fSL "$url" -o "$tar" ) || \
  ( command -v wget >/dev/null 2>&1 && wget -q "$url" -O "$tar" ) || \
    die "下载失败，请手动在目标机安装 MySQL 8.0 后重试（或设置 MYSQL_DOWNLOAD_URL）。"
  mkdir -p /opt
  tar -xf "$tar" -C /opt
  local dir
  dir="$(find /opt -maxdepth 1 -type d -name 'mysql-8.0*' | head -1 || true)"
  [[ -n "$dir" ]] || die "解压后未找到 MySQL 8.0 目录。"
  rm -rf "$INSTALL_DIR"
  ln -sfn "$dir" "$INSTALL_DIR"
  "$INSTALL_DIR/bin/mysqld" --version >/dev/null 2>&1 || die "解压后 mysqld 不可用：$INSTALL_DIR/bin/mysqld"
  log "已从安装包部署到 $INSTALL_DIR（$dir）"
}

MYSQLD=""
MYSQL_CLI=""
locate_mysqld() {
  if [[ -x "$INSTALL_DIR/bin/mysqld" ]]; then
    MYSQLD="$INSTALL_DIR/bin/mysqld"; MYSQL_CLI="$INSTALL_DIR/bin/mysql"; return 0
  fi
  if [[ -x /usr/sbin/mysqld ]]; then
    MYSQLD=/usr/sbin/mysqld
    MYSQL_CLI="$(command -v mysql || true)"
    [[ -n "$MYSQL_CLI" ]] || MYSQL_CLI=/usr/bin/mysql
    return 0
  fi
  if command -v mysqld >/dev/null 2>&1; then
    MYSQLD="$(command -v mysqld)"
    MYSQL_CLI="$(command -v mysql || true)"
    # 客户端与服务端同目录（如 /usr/bin/mysqld + /usr/bin/mysql）
    [[ -n "$MYSQL_CLI" ]] || MYSQL_CLI="$(dirname "$MYSQLD")/mysql"
    [[ -x "$MYSQL_CLI" ]] || return 1
    return 0
  fi
  return 1
}

if locate_mysqld; then
  log "检测到已安装 mysqld（$MYSQLD），跳过安装步骤（仅重放配置/账号）。"
else
  if command -v apt-get >/dev/null 2>&1; then
    log "使用 apt 安装 mysql-server-8.0 ..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y mysql-server-8.0 >/dev/null 2>&1 || {
      warn "apt 安装 mysql-server-8.0 失败（发行版仓库可能无此包），回退安装包方式。"
      fetch_tarball
    }
  elif command -v dnf >/dev/null 2>&1; then
    log "使用 dnf 安装 mysql-community-server ..."
    dnf install -y mysql-community-server >/dev/null 2>&1 || fetch_tarball
  elif command -v yum >/dev/null 2>&1; then
    log "使用 yum 安装 mysql-community-server ..."
    yum install -y mysql-community-server >/dev/null 2>&1 || fetch_tarball
  else
    warn "未识别包管理器，直接使用安装包方式。"
    fetch_tarball
  fi
  locate_mysqld || die "MySQL 8.0 安装后仍找不到 mysqld，请手动安装后重试。"
fi
log "MySQL 程序就位：mysqld=$MYSQLD  mysql=$MYSQL_CLI"

# ---------- 2) 统一日志目录 + logrotate ----------
# my.cnf 的 log_error 指向此处：必须先建目录并给 mysql 属主，否则 mysqld 起不来。
log "初始化统一日志目录：$LOG_DIR"
mkdir -p "$LOG_DIR"
id mysql >/dev/null 2>&1 || useradd -r -s /sbin/nologin mysql 2>/dev/null || true
chown -R mysql:mysql "$LOG_DIR"
chmod 0755 "$LOG_DIR"
# 节点包内 logs 软链（便于运维直接在包目录查看 error/keepalived/agent 日志）
ln -sfn "$LOG_DIR" logs
if [[ -f "$CONF_DIR/custos-ha-logrotate.conf" ]]; then
  if command -v logrotate >/dev/null 2>&1; then
    install -m 0644 "$CONF_DIR/custos-ha-logrotate.conf" /etc/logrotate.d/custos-ha
    log "已安装 logrotate：/etc/logrotate.d/custos-ha"
  else
    warn "未检测到 logrotate，跳过安装；可手动：cp $CONF_DIR/custos-ha-logrotate.conf /etc/logrotate.d/custos-ha"
  fi
fi

# ---------- 3) 落盘 my.cnf（服务端生成，保证配置一致性）----------
CNF_SRC="$CONF_DIR/my.cnf"
[[ -f "$CNF_SRC" ]] || die "未找到服务端生成的 my.cnf（应在：$CNF_SRC）。请重新在管理器生成节点目录。"
# 发行版自带的 MySQL 服务若仍在跑，会与本节点抢端口/数据目录：先停掉并禁用。
for u in mysql mysqld; do
  systemctl stop "$u" >/dev/null 2>&1 || true
  systemctl disable "$u" >/dev/null 2>&1 || true
done
mkdir -p "$(dirname "$CONF_TARGET")"
[[ -f "$CONF_TARGET" && ! -f "${CONF_TARGET}.before-custos" ]] && cp -a "$CONF_TARGET" "${CONF_TARGET}.before-custos"
cp -a "$CNF_SRC" "$CONF_TARGET"
log "已落盘 $CONF_TARGET（含 server_id / gtid_mode / port / datadir / log_error）"

# ---------- 4) 初始化数据目录 ----------
SOCKET="$DATA_DIR/mysql.sock"
mkdir -p "$DATA_DIR"
chown -R mysql:mysql "$DATA_DIR"
if [[ -d "$DATA_DIR/mysql" ]]; then
  log "检测到 $DATA_DIR/mysql 已存在，跳过初始化（保留既有数据）"
else
  if [[ -n "$(ls -A "$DATA_DIR" 2>/dev/null || true)" ]]; then
    warn "数据目录非空但无 mysql 系统库，判定为上次初始化失败的残留，清理后重新初始化。"
    rm -rf "$DATA_DIR"/* "$DATA_DIR"/.[!.]* 2>/dev/null || true
  fi
  log "初始化数据目录（mysqld --initialize-insecure）..."
  # ⚠ 必须用 --initialize-insecure：--initialize 会给 root@localhost 生成【随机临时密码】
  # 并写进错误日志，随后 `mysql -uroot` 免密登录失败、init.sql 全部不执行 →
  # 节点无 ha_monitor/replication 账号、管理器永远连不上。insecure 让 root 初始为空
  # 密码，再由本脚本第 6 步设为「创建节点时填写的密码」。
  # 仅安装包形态需要显式 --basedir（系统包自带 share/errmsg.sys，无需指定）
  BASEDIR_ARG=()
  [[ "$MYSQLD" == "$INSTALL_DIR/bin/mysqld" ]] && BASEDIR_ARG=(--basedir="$INSTALL_DIR")
  # 展开写法兼容 bash 4.3- 的 set -u（空数组直接 "${arr[@]}" 会报 unbound variable）
  "$MYSQLD" --initialize-insecure --user=mysql --datadir="$DATA_DIR" ${BASEDIR_ARG[@]+"${BASEDIR_ARG[@]}"} \
    >/tmp/mysql8-initialize.log 2>&1 || {
      err "mysqld --initialize-insecure 失败，日志：/tmp/mysql8-initialize.log"
      tail -30 /tmp/mysql8-initialize.log 2>/dev/null || true
      die "请按上述错误修复后重跑本脚本（残留数据目录会在下次自动清理）。"
    }
  log "数据目录初始化完成"
fi

# ---------- 5) systemd 单元（独立单元，不干扰发行版自带 MySQL）----------
# 用 --defaults-file 显式指向服务端生成的 my.cnf：避免发行版 /etc/mysql/conf.d/*.cnf
# 里的 datadir / socket 覆盖我们的配置（常见于 apt 安装后残留默认配置）。
# 安装包形态（/opt/mysql）必须显式 --basedir：否则 mysqld 按编译期默认 basedir 去找
# share/errmsg.sys 等文件，报 "Can't find error-message file" 而启动失败。
EXTRA_START=""
[[ "$MYSQLD" == "$INSTALL_DIR/bin/mysqld" ]] && EXTRA_START="--basedir=$INSTALL_DIR"
log "注册 systemd 单元 $SYSTEMD_UNIT"
cat > /etc/systemd/system/$SYSTEMD_UNIT <<EOF
[Unit]
Description=MySQL 8.0 HA Node (standalone, managed by Custos)
After=network.target

[Service]
Type=simple
User=mysql
Group=mysql
ExecStart=$MYSQLD --defaults-file=$CONF_TARGET --datadir=$DATA_DIR $EXTRA_START
Restart=on-failure
RestartSec=5
LimitNOFILE=65535
Environment=MALLOC_ARENA_MAX=2
PrivateTmp=false

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable "$SYSTEMD_UNIT" >/dev/null 2>&1 || true
# 不硬退出：启动失败时后面仍要拉起 keepalived 并给出诊断，避免「两个服务都没起来」
systemctl restart "$SYSTEMD_UNIT" || warn "systemctl restart $SYSTEMD_UNIT 失败，请查看 journalctl -u $SYSTEMD_UNIT"

# ---------- 6) 解析 root 密码 ----------
# 新密码：节点创建时填写、由服务端下发到 keepalived/ha_check.conf（LOCAL_ROOT_PASS）。
# 旧密码：上次安装写入的状态文件（重装/改密后先用旧密码连上再改密）。
ROOT_PASS="$(conf_get "$NODE_ROOT/keepalived/ha_check.conf" LOCAL_ROOT_PASS || true)"
if [[ -z "$ROOT_PASS" ]]; then
  ROOT_PASS="$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 16)"
  warn "未从 ha_check.conf 读到 root 密码，已生成随机密码（请在管理器同步）：$ROOT_PASS"
fi
OLD_ROOT_PASS="$(conf_get "$STATE" LOCAL_ROOT_PASS || true)"
MYSQL_PW=""
probe_root_pw() {
  MYSQL_PW=""
  for pw in "$OLD_ROOT_PASS" "$ROOT_PASS"; do
    [[ -n "$pw" ]] || continue
    if "$MYSQL_CLI" -uroot -p"$pw" --socket="$SOCKET" -e "SELECT 1" >/dev/null 2>&1; then
      MYSQL_PW="-p$pw"; return 0
    fi
  done
  if "$MYSQL_CLI" -uroot --socket="$SOCKET" -e "SELECT 1" >/dev/null 2>&1; then
    MYSQL_PW=""; return 0
  fi
  return 1
}
# 等待实例起来（首次安装为免密，重装可能已是旧密码）
for i in $(seq 1 60); do
  probe_root_pw && break
  sleep 2
done
MYSQL_OK=1
if ! probe_root_pw; then
  err "MySQL 启动后仍无法连接，请查看 journalctl -u $SYSTEMD_UNIT 与 $LOG_DIR/error.log"
  warn "将继续尝试拉起 keepalived，但 MySQL 未就绪时 VIP 不会上浮。"
  MYSQL_OK=0
fi
[[ "$MYSQL_OK" -eq 1 ]] && log "MySQL 实例已启动"

# ---------- 7) 设置 root 密码（创建节点时填写的密码，绝不留空密码）----------
if [[ "$MYSQL_OK" -eq 1 ]]; then
  if ! probe_root_pw; then
    read -r -s -p "无法用已知密码连接 MySQL，请输入当前 root 密码：" OLD_ROOT_PASS
    echo
    MYSQL_PW="-p$OLD_ROOT_PASS"
  fi
  "$MYSQL_CLI" -uroot $MYSQL_PW --socket="$SOCKET" <<SQL
ALTER USER 'root'@'localhost' IDENTIFIED BY '$ROOT_PASS';
CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED BY '$ROOT_PASS';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '$ROOT_PASS';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SQL
  log "root 密码已设置（localhost + 127.0.0.1 + 远程，与创建节点时填写的一致）"
fi

# ---------- 7.5) 客户端 / socket 软链（使 mysql -uroot -p 直接可用）----------
for b in mysql mysqldump mysqladmin mysqlshow; do
  [[ -x "$INSTALL_DIR/bin/$b" ]] || continue
  ln -sf "$INSTALL_DIR/bin/$b" "/usr/local/bin/$b"
  ln -sf "$INSTALL_DIR/bin/$b" "/usr/bin/$b" 2>/dev/null || true
done
ln -sfn "$SOCKET" /tmp/mysql.sock 2>/dev/null || true
mkdir -p /var/run/mysqld 2>/dev/null || true
[[ -d /var/run/mysqld ]] && ln -sfn "$SOCKET" /var/run/mysqld/mysqld.sock 2>/dev/null || true
hash -r 2>/dev/null || true

# ---------- 8) 执行 init.sql（建 ha_monitor / replication / app 账号）----------
MYSQL_ARR=("$MYSQL_CLI" -uroot -p"$ROOT_PASS" --socket="$SOCKET")
if [[ "$MYSQL_OK" -eq 1 && -f "$CONF_DIR/init.sql" ]]; then
  log "执行 init.sql（创建 ha_monitor/replication/app + ha_local）..."
  "${MYSQL_ARR[@]}" < "$CONF_DIR/init.sql" \
    || warn "init.sql 执行有告警，请检查（以随后账号校验结果为准）"
fi

# ---------- 8.5) 初始化结果校验（账号缺失必须判失败）----------
# 历史教训：init SQL 失败只 warn 继续、最后仍打印「完成」，节点实则缺 ha_monitor /
# replication —— 表现为管理端连不上(1045)、本节点当主库时其余节点全部复制失败。
verify_init_accounts() {
  [[ "$MYSQL_OK" -eq 1 ]] || return 0
  [[ -f "$CONF_DIR/init.sql" ]] || return 0
  local users missing=() u cnt
  users="$(grep -hoE "CREATE USER( IF NOT EXISTS)? '[^']+'" "$CONF_DIR/init.sql" 2>/dev/null \
           | sed -E "s/.*'([^']+)'/\1/" | sort -u || true)"
  [[ -z "$users" ]] && return 0
  local CRIT=" ha_monitor replication root "
  while IFS= read -r u; do
    [[ -z "$u" ]] && continue
    cnt="$("${MYSQL_ARR[@]}" -N -e "SELECT COUNT(*) FROM mysql.user WHERE user='${u}'" 2>/dev/null \
           | tail -1 | tr -d '[:space:]')"
    if ! [[ "$cnt" =~ ^[0-9]+$ ]] || (( cnt < 1 )); then
      [[ "$CRIT" == *" $u "* ]] && missing+=("$u") || warn "业务账号 $u 未创建（不影响 HA，管理器自愈会补建）"
    fi
  done <<< "$users"
  if ((${#missing[@]})); then
    err "初始化校验失败：HA 关键账号未创建成功 -> ${missing[*]}"
    err "后果：管理端连不上本节点(1045)；本节点作为主库时其余节点复制全部失败。"
    return 1
  fi
  log "初始化校验通过：HA 关键账号已就位（$(printf '%s' "$users" | tr '\n' ' ')）。"
  return 0
}
verify_init_accounts || die "节点初始化未完成，已中止部署（避免产出看似成功实则不可用的节点）。"

# ---------- 9) 防火墙（MySQL 端口 + VRRP 112）----------
PORT="$(grep -E '^[[:space:]]*port[[:space:]]*=' "$CONF_TARGET" 2>/dev/null | tail -1 | awk -F= '{print $2}' | tr -d ' ' || true)"
PORT="${PORT:-3306}"
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

# ---------- 9.5) 业务端口转发（VIP 端口 -> 本机 MySQL 端口）----------
# keepalived 只做三层 IP 漂移、不做端口转换；VIP 端口与本机端口不一致时，
# 业务连「VIP:VIP_PORT」会无人监听（表现为 VIP 能 ping 通但连不上库）。
setup_vip_port_forward() {
  local vip_port
  vip_port="$(conf_get "$NODE_ROOT/keepalived/ha_check.conf" VIP_PORT || true)"
  [[ -z "$vip_port" || "$vip_port" == "$PORT" ]] && return 0
  log "配置业务端口转发：VIP 端口 $vip_port -> 本机 $PORT ..."
  if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active firewalld >/dev/null 2>&1; then
    firewall-cmd --permanent --add-forward-port="port=${vip_port}:proto=tcp:toport=${PORT}" >/dev/null 2>&1 \
      && firewall-cmd --reload >/dev/null 2>&1 \
      && log "firewalld: 已配置端口转发 ${vip_port} -> ${PORT}" \
      || warn "firewalld 端口转发失败，请手动添加"
  else
    if ! iptables -t nat -C PREROUTING -p tcp --dport "$vip_port" -j REDIRECT --to-port "$PORT" >/dev/null 2>&1; then
      iptables -t nat -A PREROUTING -p tcp --dport "$vip_port" -j REDIRECT --to-port "$PORT" 2>/dev/null \
        && log "iptables: 已配置端口转发 ${vip_port} -> ${PORT}" \
        || warn "iptables 端口转发失败，请手动添加 NAT 规则"
    fi
  fi
}
setup_vip_port_forward

# ---------- 10) 启动 keepalived 客户端 + 节点 agent（compose）----------
if [[ -f "$NODE_ROOT/docker-compose.yaml" || -f "$NODE_ROOT/docker-compose.yml" ]]; then
  if command -v docker >/dev/null 2>&1; then
    log "启动 keepalived 客户端与节点 agent 容器..."
    ( cd "$NODE_ROOT" && docker compose up -d keepalived custos-node-agent ) \
      || warn "keepalived/agent 启动失败，请手动：cd $NODE_ROOT && docker compose up -d keepalived custos-node-agent"
  else
    warn "未安装 docker，跳过 keepalived/agent（安装后手动执行上述 compose 命令）"
  fi
fi

# ---------- 11) 状态文件（供卸载 / 重装取旧密码）----------
mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
: > "$STATE"
printf 'NODE_TYPE=mysql8\n'                 >> "$STATE"
printf 'LOCAL_ROOT_PASS=%q\n'  "$ROOT_PASS" >> "$STATE"
printf 'INSTALL_DIR=%q\n'      "$INSTALL_DIR" >> "$STATE"
printf 'DATA_DIR=%q\n'         "$DATA_DIR"   >> "$STATE"
printf 'LOG_DIR=%q\n'          "$LOG_DIR"    >> "$STATE"
printf 'SYSTEMD_UNIT=%q\n'     "$SYSTEMD_UNIT" >> "$STATE"
printf 'PORT=%q\n'             "$PORT"       >> "$STATE"
chmod 600 "$STATE"
log "已记录 MySQL 安装状态到 $STATE"

echo
log "==== MySQL 8.0 节点安装完成 ===="
echo "  • 程序目录     : $INSTALL_DIR（或系统包安装路径）"
echo "  • 数据目录     : $DATA_DIR"
echo "  • 日志目录     : $LOG_DIR/error.log"
echo "  • 监听端口     : $PORT"
echo "  • systemd 单元 : $SYSTEMD_UNIT（systemctl start/stop/status $SYSTEMD_UNIT）"
echo "  • 卸载方式     : sudo bash mysql8.0/scripts/Uninstall-mysql8-Node.sh"
echo
if [[ "$MYSQL_OK" -eq 1 ]]; then
  log "✅ MySQL 已启动（root 密码与创建节点时填写的一致）"
else
  warn "⚠️  MySQL 未启动成功（见上方错误）：journalctl -u $SYSTEMD_UNIT"
fi
echo "后续：各节点安装完成后，管理器会自动建复制、选主、漂移 VIP。"
echo "如需指定路径/端口，设置环境变量 MYSQL_INSTALL_DIR / MYSQL_DATA_DIR / MYSQL_DOWNLOAD_URL 后重跑本脚本。"
