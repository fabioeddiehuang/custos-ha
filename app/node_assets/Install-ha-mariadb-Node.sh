#!/usr/bin/env bash
# =============================================================
# Install-ha-mariadb-Node.sh  —  裸机 MariaDB 节点【唯一】全量安装脚本
# -------------------------------------------------------------
# 用途：在「裸机 MariaDB」类型节点上，一键下载/解压/初始化/拉起
#       MariaDB 12.x（systemd 托管），并完成 HA 所需账号、复制、
#       keepalived 客户端等初始化与启动（本文件自包含，无需其他安装脚本）。
#
# 用法（在节点目录内，以 root 执行）：
#       sudo bash Install-ha-mariadb-Node.sh
#
# 幂等说明：再次执行（例如重新生成配置、覆盖密码/my.cnf 后）即为「更新」：
#       不会重新初始化数据目录，只会覆盖配置、更新 root 密码、重建
#       账号与复制，并重启服务。
#
# 注意：本脚本只适用于「裸机 MariaDB」部署；「Docker MySQL 8.0」节点
#       不需要本脚本，直接用目录内的 docker-compose.yaml 启动即可。
# =============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ---------- 可手动替换的下载链接（留空则只尝试用本地已存在的安装包） ----------
MARIADB_DOWNLOAD_URL="${MARIADB_DOWNLOAD_URL:-https://mirrors.tuna.tsinghua.edu.cn/mariadb//mariadb-12.3.3/bintar-linux-systemd-x86_64/mariadb-12.3.3-linux-systemd-x86_64.tar.gz}"

# ---------- 安装路径（如需指定，改这里；或加环境变量 MARIADB_INSTALL_DIR 覆盖） ----------
INSTALL_DIR="${MARIADB_INSTALL_DIR:-/opt/mariadb}"
DATA_DIR="${MARIADB_DATA_DIR:-/data/mariadb}"
SYSTEMD_UNIT="mariadb-ha.service"

log(){ echo -e "\033[1;32m[mariadb-install]\033[0m $*"; }
warn(){ echo -e "\033[1;33m[warn]\033[0m $*"; }
err(){ echo -e "\033[1;31m[error]\033[0m $*"; }

# ---- 静默退出兜底诊断 ----
# 本脚本开启 set -euo pipefail，而 `VAR=$(grep ... | ...)` 这类赋值在 grep 无匹配时
# （pipefail 会把失败状态传出来）会**一声不响地终止整个脚本**——在远程节点上表现为
# 「打印到某一行后就没输出了、也没有任何报错」，极难定位。故：
#   1) 用 die() 统一「已明确诊断」的失败（不触发下面的兜底提示，避免噪音）；
#   2) 其余非预期失败由 ERR trap 打印行号与失败命令。
_REPORTED=0
die(){ _REPORTED=1; err "$*"; exit 1; }
# 精确定位失败点（实测结论）：
#   - EXIT trap 里的 ${BASH_LINENO[0]} 在顶层脚本恒为 0，不可用；
#   - DEBUG trap 里的 $LINENO 恒为 trap 自身行号（1），同样不可用；
#   - ERR trap 的 $LINENO / $BASH_COMMAND 才是真正的失败行与失败命令。
# set -E（errtrace）让函数体内的失败也能触发 ERR；
# _REPORTED 保证 die() 已给出明确诊断时不再追加兜底提示，避免重复噪音。
set -E
trap 'rc=$?; if [[ $_REPORTED -eq 0 ]]; then _REPORTED=1; err "脚本在第 $LINENO 行执行失败（退出码 $rc）：${BASH_COMMAND:0:200}"; fi' ERR
trap 'rc=$?; if [[ $rc -ne 0 && $_REPORTED -eq 0 ]]; then err "脚本意外退出：退出码 $rc（无更多上下文）"; fi' EXIT

# 安全读取 KEY=VALUE：文件不存在 / 键缺失 / 无匹配时返回空串，且**绝不**触发 set -e 退出
conf_get() {
  local f="$1" k="$2"
  [[ -f "$f" ]] || return 0
  grep -E "^${k}=" "$f" 2>/dev/null | tail -1 | cut -d= -f2- \
    | sed -E "s/^'//; s/'$//" || true
}

if [[ $EUID -ne 0 ]]; then
  die "请使用 root 执行：sudo bash Install-ha-mariadb-Node.sh"
fi

# ---------- 0) 环境依赖检测 ----------
for bin in curl tar gzip; do
  command -v "$bin" >/dev/null 2>&1 || { die "缺少必要命令：$bin，请先安装（apt/yum install $bin）"; }
done
# MariaDB 二进制需要 libaio；缺失会导致启动即段错误
if ! ldconfig -p 2>/dev/null | grep -q "libaio.so"; then
  warn "未检测到 libaio（MariaDB 运行必需）。尝试自动安装…"
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update >/dev/null 2>&1 && apt-get install -y libaio1 libaio1 2>/dev/null || apt-get install -y libaio1
  elif command -v yum >/dev/null 2>&1; then
    yum install -y libaio 2>/dev/null || true
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y libaio 2>/dev/null || true
  fi
  if ! ldconfig -p 2>/dev/null | grep -q "libaio.so"; then
    die "libaio 安装失败，请手动安装（apt install libaio1 / yum install libaio）后再运行。"
  fi
fi

# ---------- 1) 定位 / 下载 MariaDB 安装包 ----------
PKG=""
# 优先用当前目录已存在的 tarball（用户要求：已有则跳过下载）
shopt -s nullglob
for f in mariadb-*.tar.gz mariadb-*.tar.xz MariaDB-*.tar.gz; do
  PKG="$f"; break
done
shopt -u nullglob

if [[ -n "$PKG" ]]; then
  log "检测到本地安装包：$PKG（跳过下载）"
else
  if [[ -z "$MARIADB_DOWNLOAD_URL" ]]; then
    die "当前目录未找到 MariaDB 安装包，且未设置 MARIADB_DOWNLOAD_URL。请手动上传安装包到本目录后重试。"
  fi
  PKG="mariadb-install.tar.gz"
  log "未找到本地安装包，开始下载：$MARIADB_DOWNLOAD_URL"
  if command -v curl >/dev/null 2>&1; then
    curl -fSL "$MARIADB_DOWNLOAD_URL" -o "$PKG" || die "下载失败：$MARIADB_DOWNLOAD_URL"
  else
    wget -O "$PKG" "$MARIADB_DOWNLOAD_URL" || die "下载失败：$MARIADB_DOWNLOAD_URL"
  fi
  log "下载完成：$PKG"
fi

# ---------- 2) 解压到 INSTALL_DIR ----------
# 若 $INSTALL_DIR 已存在可用的 mariadbd（曾经解压/安装过），跳过解压直接进入后续步骤。
if [[ -x "$INSTALL_DIR/bin/mariadbd" ]]; then
  log "检测到 $INSTALL_DIR/bin/mariadbd 已存在且可执行，跳过解压（沿用既有安装）"
else
  log "解压 $PKG → $INSTALL_DIR"
  mkdir -p "$INSTALL_DIR"
  TMP_EXTRACT="$(mktemp -d)"
  tar -xzf "$PKG" -C "$TMP_EXTRACT" --strip-components=1 || tar -xf "$PKG" -C "$TMP_EXTRACT" --strip-components=1
  # 若 tarball 含顶层目录（多数情况），用其解出的真实目录
SRC_DIR="$TMP_EXTRACT"
if [[ -d "$TMP_EXTRACT"/* && $(ls -1 "$TMP_EXTRACT" | wc -l) -eq 1 ]]; then
  SRC_DIR="$(ls -d "$TMP_EXTRACT"/*)"
fi
  # 清掉旧安装目录内容后覆盖（保留 INSTALL_DIR 路径本身，便于 systemd 指向固定路径）
  rm -rf "$INSTALL_DIR"/*
  cp -a "$SRC_DIR"/. "$INSTALL_DIR"/
  rm -rf "$TMP_EXTRACT"
  chown -R root:root "$INSTALL_DIR"
fi
# tarball 内二进制可能不带执行位（如某些镜像为 -rw-r--r--），systemd 启动会 Permission denied。
# 显式给 bin/ 下所有文件加执行权限（MariaDB 的 bin/ 均为可执行程序/脚本），并给脚本目录同样处理。
chmod -R u+rwX,go+rX "$INSTALL_DIR"
[[ -d "$INSTALL_DIR/bin" ]] && chmod -R +x "$INSTALL_DIR/bin"
[[ -d "$INSTALL_DIR/scripts" ]] && chmod -R +x "$INSTALL_DIR/scripts"
# 顺带确保 mariadbd / mysqld 主程序可执行
for exe in mariadbd mysqld mariadb-install-db mariadb; do
  [[ -f "$INSTALL_DIR/bin/$exe" ]] && chmod +x "$INSTALL_DIR/bin/$exe"
done
# SELinux（RHEL/CentOS/Anolis 等）：/opt 下二进制默认无 mysqld_exec_t 标签，
# 即便 chmod +x 也会 Permission denied。若系统启用 SELinux 则打标签。
if command -v getenforce >/dev/null 2>&1 && [[ "$(getenforce)" != "Disabled" ]]; then
  if command -v semanage >/dev/null 2>&1 && command -v restorecon >/dev/null 2>&1; then
    semanage fcontext -a -t mysqld_exec_t "${INSTALL_DIR}(/.*)?" 2>/dev/null || true
    semanage fcontext -a -t mysqld_db_t "${DATA_DIR}(/.*)?" 2>/dev/null || true
    restorecon -Rv "$INSTALL_DIR" "$DATA_DIR" >/dev/null 2>&1 || true
    log "SELinux：已为 $INSTALL_DIR / $DATA_DIR 打 mysqld 标签"
  else
    warn "检测到 SELinux 启用，但未安装 semanage（policycoreutils-python）。"
    warn "若启动仍 Permission denied，请手动：semanage fcontext -a -t mysqld_exec_t '${INSTALL_DIR}(/.*)?'; restorecon -Rv $INSTALL_DIR"
  fi
fi
log "MariaDB 程序已就位：$INSTALL_DIR/bin/mariadbd"

# ---------- 2.5) 运行前兼容性自检（关键）----------
# MariaDB 12.3.3 的 linux-systemd 二进制要求宿主 glibc ≥ 2.28、libstdc++ 带
# GLIBCXX_3.4.20+、libcrypt(XCRYPT_2.0)、libsystemd(LIBSYSTEMD_227)。
# 老系统（如 CentOS 7 / glibc 2.17、Ubuntu 18.04 / glibc 2.27）会直接报
# "... version `GLIBC_2.28' not found" 且 bootstrap 静默失败，极难看懂。
# 这里在初始化前就验证二进制能真正被加载执行，给出明确指引。
if ! out="$("$INSTALL_DIR/bin/mariadbd" --version 2>&1)"; then
  err "本节点无法运行 $INSTALL_DIR/bin/mariadbd（操作系统 / 运行时库版本过低）。缺失的符号："
  echo "$out" | grep -iE 'not found|version .* not|GLIBC|GLIBCXX|XCRYPT|LIBSYSTEMD|CXXABI' | head -8 | sed 's/^/      /' || true
  die "该节点的 glibc/libstdc++ 低于 MariaDB 12.3.3(linux-systemd) 的要求（需 glibc≥2.28 等）。"\
"此类老系统请改用 Docker 方式部署 MariaDB 12.3.3（容器自带运行时，不受宿主 glibc 限制）："\
"在本节点目录用官方镜像 mariadb:12.3.3（host 网络，挂载 ./my.cnf 与 /data/mariadb），"\
"再按裸机流程执行 init_mariadb.sql 并拉起 keepalived 客户端即可。"\
"或升级该节点系统 / glibc 后重试裸机安装。"
fi
log "二进制兼容性自检通过：$(echo "$out" | head -1)"

# ---------- 3) 初始化 data 目录 ----------
log "初始化数据目录：$DATA_DIR"
mkdir -p "$DATA_DIR"
# 已初始化过（mysql 系统库存在）则跳过，避免重跑脚本时 mariadb-install-db 因 datadir 非空报错
if [[ -d "$DATA_DIR/mysql" ]]; then
  log "检测到 $DATA_DIR/mysql 已存在，跳过初始化（保留既有数据）"
else
  # 目录存在但非空、且没有 mysql 系统库 ⇒ 只可能是上次初始化失败留下的半成品
  # （能用的库一定有 mysql/ 目录），不含任何可用数据，清理后重来；
  # 否则 mariadb-install-db 会因 datadir 非空直接失败。
  if [[ -n "$(ls -A "$DATA_DIR" 2>/dev/null || true)" ]]; then
    warn "数据目录 $DATA_DIR 非空但无 mysql 系统库，判定为上次初始化失败的残留，清理后重新初始化。残留内容："
    ls -A "$DATA_DIR" 2>/dev/null | head -20 | sed 's/^/      /' || true
    rm -rf "$DATA_DIR"/* "$DATA_DIR"/.[!.]* 2>/dev/null || true
  fi
  # 用 mariadb-install-db 初始化（MariaDB 12 用 mariadbd 而非 mysqld，但 install-db 脚本兼容）
  INSTALL_DB="$INSTALL_DIR/scripts/mariadb-install-db"
  [[ -x "$INSTALL_DB" ]] || INSTALL_DB="$INSTALL_DIR/bin/mariadb-install-db"
  [[ -x "$INSTALL_DB" ]] || die "未找到 mariadb-install-db，安装包可能不完整"
  # 创建系统用户（若不存在）
  id mysql >/dev/null 2>&1 || useradd -r -s /sbin/nologin mysql 2>/dev/null || true
  # --basedir 必须显式指定：bintar 解压到 /opt/mariadb 时，若让 install-db 自行推断，
  # 极易找不到 share/english/errmsg.sys 等语言/字典文件，bootstrap 起不来
  #（典型报错：Can't find error-message file / FATAL ERROR: Could not find ...）。
  "$INSTALL_DB" --user=mysql --basedir="$INSTALL_DIR" --datadir="$DATA_DIR" \
    --auth-root-authentication-method=normal \
    >/tmp/mariadb-install-db.log 2>&1 \
    || {
      err "mariadb-install-db 失败，日志：/tmp/mariadb-install-db.log"
      echo "---------- 关键错误行 ----------"
      grep -iE '\[error\]|\[fatal\]|error:|errno|fail|not find|cannot' \
        /tmp/mariadb-install-db.log 2>/dev/null | tail -15 || true
      echo "---------- 日志尾部（最后 40 行） ----------"
      tail -40 /tmp/mariadb-install-db.log 2>/dev/null || true
      echo "-------------------------------------------"
      err "环境自检：$("$INSTALL_DIR/bin/mariadbd" --version 2>&1 || echo 'mariadbd 无法执行')"
      err "basedir=$INSTALL_DIR  datadir=$DATA_DIR  user=mysql"
      die "请按上述关键信息修复后重跑本脚本（数据目录残留会在下次自动清理）"
    }
  log "数据目录初始化完成"
fi
chown -R mysql:mysql "$DATA_DIR"
[[ -d "$DATA_DIR/mysql" ]] || log "数据目录初始化完成"

# ---------- 4) 落盘 my.cnf（服务端生成，确保配置一致性） ----------
CNF_SRC="$SCRIPT_DIR/my.cnf"
if [[ ! -f "$CNF_SRC" ]]; then
  die "未找到服务端生成的 my.cnf（应在节点目录下：$CNF_SRC）。请重新在管理器生成节点目录。"
fi
# 备份可能已存在的 /etc/my.cnf（避免覆盖宿主既有 MySQL 配置）
[[ -f /etc/my.cnf && ! -f /etc/my.cnf.before-mariadb-ha ]] && cp -a /etc/my.cnf /etc/my.cnf.before-mariadb-ha
cp -a "$CNF_SRC" /etc/my.cnf
log "已落盘 /etc/my.cnf（由服务端生成，含 server_id/gtid_domain_id/port/datadir）"

# ---------- 5 前置：解析 root 密码（新密码取自 ha_check.conf；旧密码取自状态文件，便于重装更新） ----------
# 新密码（服务端生成，覆盖配置后用于设密）：
ROOT_PASS=""
# 注意：这里不能用 `ROOT_PASS=$(grep ... | ...)` 裸写法——set -euo pipefail 下
# grep 无匹配（文件存在但缺该键）会让赋值整条失败并**静默终止脚本**，
# 表现为「打印到上一步就没有任何输出」。conf_get 已消除该风险。
ROOT_PASS="$(conf_get "$SCRIPT_DIR/keepalived/ha_check.conf" LOCAL_ROOT_PASS || true)"
if [[ -z "$ROOT_PASS" ]]; then
  ROOT_PASS="$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 16)"
  warn "未从 ha_check.conf 读到 root 密码，已生成随机密码：$ROOT_PASS（请到管理器节点配置同步）"
fi
# 旧密码（来自上次安装写入的状态文件；重装/覆盖配置时用它先连上再改密）：
OLD_ROOT_PASS=""
OLD_ROOT_PASS="$(conf_get /etc/ha-mysql-node/install-state-mariadb LOCAL_ROOT_PASS || true)"
# 探测当前实例可用的 root 凭证（首次安装 root 无密码；重装时可能已是旧密码）。
# 命中后写入 MYSQL_PW（-pX 或空串表示免密 socket 可连）。
MYSQL_PW=""
probe_root_pw() {
  MYSQL_PW=""
  for pw in "$OLD_ROOT_PASS" "$ROOT_PASS"; do
    [[ -n "$pw" ]] || continue
    if "$INSTALL_DIR/bin/mysql" -uroot -p"$pw" --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1; then
      MYSQL_PW="-p$pw"; return 0
    fi
  done
  if "$INSTALL_DIR/bin/mysql" -uroot --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1; then
    MYSQL_PW=""; return 0
  fi
  return 1
}

# ---------- 5) systemd 单元（独立单元，不干扰宿主 MySQL） ----------
log "注册 systemd 单元 $SYSTEMD_UNIT"
cat > /etc/systemd/system/$SYSTEMD_UNIT <<EOF
[Unit]
Description=MariaDB HA Node (standalone, managed by HA-MySQL-Manager)
After=network.target
Documentation=https://mariadb.com/kb/en/mariadbd/

[Service]
Type=notify
User=mysql
Group=mysql
ExecStart=$INSTALL_DIR/bin/mariadbd --defaults-file=/etc/my.cnf --datadir=$DATA_DIR
ExecStartPost=/bin/sleep 5
Restart=on-failure
RestartSec=5
LimitNOFILE=65535
PrivateTmp=false

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable $SYSTEMD_UNIT
systemctl restart $SYSTEMD_UNIT
# 等待实例起来（兼容首次安装免密 / 重装已有密码两种情形）
for i in $(seq 1 30); do
  if "$INSTALL_DIR/bin/mysql" -uroot --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  if [[ -n "$ROOT_PASS" ]] && "$INSTALL_DIR/bin/mysql" -uroot -p"$ROOT_PASS" --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  if [[ -n "$OLD_ROOT_PASS" ]] && "$INSTALL_DIR/bin/mysql" -uroot -p"$OLD_ROOT_PASS" --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
MARIADB_OK=1
if ! "$INSTALL_DIR/bin/mysql" -uroot --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1 \
   && ! { [[ -n "$ROOT_PASS" ]] && "$INSTALL_DIR/bin/mysql" -uroot -p"$ROOT_PASS" --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1; } \
   && ! { [[ -n "$OLD_ROOT_PASS" ]] && "$INSTALL_DIR/bin/mysql" -uroot -p"$OLD_ROOT_PASS" --socket="$DATA_DIR/mysql.sock" -e "SELECT 1" >/dev/null 2>&1; }; then
  # 不硬退出：MariaDB 起不来时仍继续尝试启动 keepalived，并在结尾汇总，
  # 避免「一刀切 exit」导致 keepalived（步骤9）永远没机会执行、两个服务都看似没起。
  err "MariaDB 启动失败，请查看 journalctl -u $SYSTEMD_UNIT 与 $DATA_DIR/*.err"
  warn "将继续尝试启动 keepalived；但 MariaDB 未就绪时 VIP 不会上浮，请先修复 MariaDB。"
  MARIADB_OK=0
fi
[[ "$MARIADB_OK" -eq 1 ]] && log "MariaDB 实例已启动"

# ---------- 6) 设置 root 密码（取 ha_check.conf 的 LOCAL_ROOT_PASS；幂等：覆盖/重装时先连旧密码再改密） ----------
# ROOT_PASS 已在步骤 5 前置解析；此处先探测当前可用凭证，再 ALTER 为新密码（支持首次安装与重装更新）。
if ! probe_root_pw; then
  # 已知密码/免密都连不上（例如曾手工改密），交互询问当前 root 密码
  read -r -s -p "无法用已知密码连接 MariaDB，请输入当前 root 密码（用于更新配置/密码）：" OLD_ROOT_PASS
  echo
  MYSQL_PW="-p$OLD_ROOT_PASS"
fi
"$INSTALL_DIR/bin/mysql" -uroot $MYSQL_PW --socket="$DATA_DIR/mysql.sock" <<SQL
ALTER USER 'root'@'localhost' IDENTIFIED VIA mysql_native_password USING PASSWORD('$ROOT_PASS');
CREATE USER IF NOT EXISTS 'root'@'127.0.0.1' IDENTIFIED VIA mysql_native_password USING PASSWORD('$ROOT_PASS');
GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION;
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('$ROOT_PASS');
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SQL
log "root 密码已设置（localhost + 127.0.0.1 + '%' 远程，便于内网 Navicat 连接）"

# ---------- 6.5) 客户端工具软链 + socket 软链（使 `mysql -uroot -p` 直接可用） ----------
log "创建客户端软链与 socket 软链..."
for bin in mysql mariadb mysqldump mysqladmin mysqlshow myisamchk; do
  src="$INSTALL_DIR/bin/$bin"
  [[ -x "$src" ]] || continue
  ln -sf "$src" "/usr/local/bin/$bin"
  ln -sf "$src" "/usr/bin/$bin" 2>/dev/null || true
done
# 默认 socket 软链：让不指定 -S 的 mysql 客户端也能连上（/tmp 与 /var/run/mysqld 双保险）
ln -sf "$DATA_DIR/mysql.sock" /tmp/mysql.sock 2>/dev/null || true
mkdir -p /var/run/mysqld
ln -sf "$DATA_DIR/mysql.sock" /var/run/mysqld/mysqld.sock 2>/dev/null || true
hash -r 2>/dev/null || true
log "已创建 /usr/local/bin/mysql 等软链 + socket 软链（执行 mysql -uroot -p 即可登录）"

# ---------- 7) 跑 init_mariadb.sql + fix-node-creds.sql ----------
# 注意：必须用本地 socket 连接（VIP 此刻可能尚未上浮/由 keepalived 接管），
# 切勿用 -h127.0.0.1（会解析到 VIP 网卡），否则连不到本机 MariaDB。
MYSQL=("$INSTALL_DIR/bin/mysql" -uroot -p"$ROOT_PASS" --socket="$DATA_DIR/mysql.sock")
if [[ -f "$SCRIPT_DIR/init_mariadb.sql" ]]; then
  log "执行 init_mariadb.sql（创建 ha_monitor/replication/app + ha_local）..."
  "${MYSQL[@]}" < "$SCRIPT_DIR/init_mariadb.sql" || warn "init_mariadb.sql 执行有告警，请检查"
fi
if [[ -f "$SCRIPT_DIR/fix-node-creds.sql" ]]; then
  log "重放 fix-node-creds.sql（权限对齐，防面板/重装回退）..."
  "${MYSQL[@]}" < "$SCRIPT_DIR/fix-node-creds.sql" || warn "fix-node-creds.sql 执行有告警，请检查"
fi

# ---------- 8) 防火墙（MySQL 端口 + VRRP 112） ----------
# 同样避免裸 grep 触发 set -e 静默退出；正则兼容 "port=3307" 与 "port = 3307" 两种写法
PORT="$(grep -E '^[[:space:]]*port[[:space:]]*=' /etc/my.cnf 2>/dev/null | tail -1 | awk -F= '{print $2}' | tr -d ' ' || true)"
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

# ---------- 9) 启动 keepalived 客户端（compose） ----------
if [[ -f "$SCRIPT_DIR/docker-compose.yaml" || -f "$SCRIPT_DIR/docker-compose.yml" ]]; then
  if command -v docker >/dev/null 2>&1; then
    log "启动 keepalived 客户端容器..."
    ( cd "$SCRIPT_DIR" && docker compose up -d ) || warn "keepalived 启动失败，请手动 docker compose up -d"
  else
    warn "未安装 docker，跳过 keepalived（请安装 docker 后手动 cd $SCRIPT_DIR && docker compose up -d）"
  fi
fi

# ---------- 10) 状态文件（供 uninstall 还原） ----------
STATE_DIR=/etc/ha-mysql-node
mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
STATE="$STATE_DIR/install-state-mariadb"
: > "$STATE"
printf 'NODE_TYPE=mariadb\n' >> "$STATE"
printf 'LOCAL_ROOT_PASS=%q\n' "$ROOT_PASS" >> "$STATE"
printf 'INSTALL_DIR=%q\n'    "$INSTALL_DIR" >> "$STATE"
printf 'DATA_DIR=%q\n'       "$DATA_DIR"   >> "$STATE"
printf 'SYSTEMD_UNIT=%q\n'   "$SYSTEMD_UNIT" >> "$STATE"
printf 'PORT=%q\n'           "$PORT"      >> "$STATE"
# 程序目录指纹（文件数:总字节数），供卸载时判断是否被人工修改：
# 未修改则保留 /opt/mariadb（不删除，便于快速重装/回滚）；被改过或 -f 才删除。
FP_COUNT="$(find "$INSTALL_DIR" -type f 2>/dev/null | wc -l || true)"
FP_BYTES="$(du -sb "$INSTALL_DIR" 2>/dev/null | cut -f1 || true)"
printf 'INSTALL_DIR_FP=%s:%s\n' "$FP_COUNT" "$FP_BYTES" >> "$STATE"
chmod 600 "$STATE"
log "已记录 MariaDB 安装状态到 $STATE"

echo
log "==== MariaDB 节点安装完成 ===="
echo "  • MariaDB 程序目录 : $INSTALL_DIR"
echo "  • 数据目录         : $DATA_DIR"
echo "  • 监听端口         : $PORT"
echo "  • systemd 单元     : $SYSTEMD_UNIT（systemctl start/stop/status $SYSTEMD_UNIT）"
echo "  • 卸载方式         : sudo bash Uninstall-mariadb-Node.sh"
echo
echo
if [[ "$MARIADB_OK" -eq 1 ]]; then
  log "✅ MariaDB 已启动（systemd 单元 $SYSTEMD_UNIT）"
else
  warn "⚠️  MariaDB 未启动成功（见上方错误），请修复后重试：systemctl restart $SYSTEMD_UNIT"
fi
echo "后续：等待各节点都安装完成后，管理器会自动建复制、选主、漂移 VIP。"
echo "若需指定安装路径/端口，编辑节点目录 my.cnf（服务端生成）或设置环境变量"
echo "MARIADB_INSTALL_DIR / MARIADB_DATA_DIR / MARIADB_DOWNLOAD_URL 后重跑本脚本。"
