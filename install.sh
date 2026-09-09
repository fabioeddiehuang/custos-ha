#!/bin/sh
# ---------------------------------------------------------------------------
# Custos 服务端【交互式一键安装】
#
#   sudo sh install.sh         交互式（推荐）
#   sudo sh install.sh -y      非交互：全部用默认值 + 随机口令（脚本化部署）
#   sh install.sh -C <目录>    指定项目目录（默认：脚本所在目录，即项目根目录）
#
# 做了什么
#   1) 检查 docker / docker compose
#   2) 交互式收集部署参数（管理库、管理器对外 IP、VIP、网卡、Web 账号端口…）
#   3) 生成 .env（SECRET_KEY / WEB_PASS 自动生成强随机值，不再用 admin）
#   4) 把上述参数写回 docker-compose.yml（免手工编辑）
#   5) root 时自动把 custosctl 安装到 /usr/bin
#   6) 构建并启动，最后自检并打印访问地址与账号
#
# 幂等：已有 .env / docker-compose.yml 时，以现有值为默认值，重复执行安全。
# ---------------------------------------------------------------------------
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CUSTOS_HOME=""
ASSUME_YES=0

while [ $# -gt 0 ]; do
  case "$1" in
    -C|--dir) CUSTOS_HOME=$2; shift 2 ;;
    -y|--yes) ASSUME_YES=1; shift ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "未知参数：$1" >&2; exit 1 ;;
  esac
done

[ -n "$CUSTOS_HOME" ] || CUSTOS_HOME=$(cd "$SCRIPT_DIR" && pwd)
CUSTOS_HOME=$(cd "$CUSTOS_HOME" && pwd)
cd "$CUSTOS_HOME"

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
warn() { printf '  \033[33m! %s\033[0m\n' "$*" >&2; }

# 交互取值：ask "提示" 变量名 默认值
ask() {
  _prompt=$1; _def=$2
  if [ "$ASSUME_YES" = "1" ]; then
    printf '  %s [%s] -> %s\n' "$_prompt" "$_def" "$_def"
    _ans=$_def
  else
    printf '  %s [%s]: ' "$_prompt" "$_def"
    read -r _ans || true
    [ -n "$_ans" ] || _ans=$_def
  fi
  REPLY=$_ans
}

rand_secret() {
  if command -v openssl >/dev/null 2>&1; then openssl rand -hex 32
  else head -c 32 /dev/urandom 2>/dev/null | od -An -tx1 | tr -d ' \n'; fi
}

# 本机对外 IP / 默认网卡
detect_ip() {
  ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}' \
    || hostname -I 2>/dev/null | awk '{print $1}' || echo ""
}
detect_iface() {
  ip route 2>/dev/null | awk '/^default/{print $5; exit}' || echo "eth0"
}

# ---------------------------------------------------------------------------
say "1/6 环境检查"
command -v docker >/dev/null 2>&1 || { echo "错误：未安装 docker。" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "错误：docker compose 插件不可用（需 Docker 20.10+）。" >&2; exit 1; }
info "docker：OK"
[ -f "$CUSTOS_HOME/docker-compose.yml" ] || [ -f "$CUSTOS_HOME/docker-compose-example.yml" ] || {
  echo "错误：$CUSTOS_HOME 下找不到 docker-compose.yml 或 docker-compose-example.yml。" >&2; exit 1; }
info "项目目录：$CUSTOS_HOME"

# ---------------------------------------------------------------------------
say "2/6 读取现有配置作为默认值"
CUR_VER=$(grep -E '^VERSION=' .env 2>/dev/null | head -1 | cut -d= -f2 | tr -d ' \t\r\n')
[ -n "$CUR_VER" ] || CUR_VER=$(grep -E '^VERSION=' env.example 2>/dev/null | head -1 | cut -d= -f2 | tr -d ' \t\r\n')
[ -n "$CUR_VER" ] || CUR_VER=v2.2.20

COMPOSE_FILE=$CUSTOS_HOME/docker-compose.yml
cur_val() {  # 从现有 compose 取某个键的值
  grep -E "^[[:space:]]*$1:" "$COMPOSE_FILE" 2>/dev/null | head -1 \
    | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//' || true
}

DEF_MGMT_HOST=$(cur_val MGMT_MYSQL_HOST); [ -n "$DEF_MGMT_HOST" ] || DEF_MGMT_HOST=127.0.0.1
DEF_MGMT_PORT=$(cur_val MGMT_MYSQL_PORT); [ -n "$DEF_MGMT_PORT" ] || DEF_MGMT_PORT=3306
DEF_MGMT_USER=$(cur_val MGMT_MYSQL_USER); [ -n "$DEF_MGMT_USER" ] || DEF_MGMT_USER=ha
DEF_MGMT_PASS=$(cur_val MGMT_MYSQL_PASS)
DEF_MGMT_DB=$(cur_val MGMT_MYSQL_DB);   [ -n "$DEF_MGMT_DB" ]   || DEF_MGMT_DB=ha
DEF_MGR_IP=$(cur_val MANAGER_PUBLIC_IP); [ -n "$DEF_MGR_IP" ] || DEF_MGR_IP=$(detect_ip)
DEF_IFACE=$(cur_val INTERFACE);          [ -n "$DEF_IFACE" ] || DEF_IFACE=$(detect_iface)
DEF_VIP_PREFIX=$(cur_val VIP_PREFIX);    [ -n "$DEF_VIP_PREFIX" ] || DEF_VIP_PREFIX=24
DEF_VRID=$(cur_val VRID);                [ -n "$DEF_VRID" ] || DEF_VRID=51
DEF_WEB_PORT=$(cur_val WEB_PORT);        [ -n "$DEF_WEB_PORT" ] || DEF_WEB_PORT=28888
DEF_WEB_USER=$(cur_val WEB_USER);        [ -n "$DEF_WEB_USER" ] || DEF_WEB_USER=admin
DEF_VIP=$(grep -E '^[[:space:]]*VIP:' "$COMPOSE_FILE" 2>/dev/null | head -1 \
  | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//' || true)
[ -n "$DEF_VIP" ] || DEF_VIP=""

info "版本 $CUR_VER；将使用现有值作为默认（直接回车即采用）"

# ---------------------------------------------------------------------------
say "3/6 部署参数"
ask "管理库地址 MGMT_MYSQL_HOST" "$DEF_MGMT_HOST"; MGMT_HOST=$REPLY
ask "管理库端口" "$DEF_MGMT_PORT"; MGMT_PORT=$REPLY
ask "管理库账号" "$DEF_MGMT_USER"; MGMT_USER=$REPLY
ask "管理库密码" "${DEF_MGMT_PASS:-}"; MGMT_PASS=$REPLY
ask "管理库库名" "$DEF_MGMT_DB"; MGMT_DB=$REPLY
ask "管理器对外 IP（节点看到的来源 IP）" "$DEF_MGR_IP"; MGR_IP=$REPLY
ask "节点网卡名 INTERFACE" "$DEF_IFACE"; IFACE=$REPLY
ask "集群 VIP（可选，同网段空闲 IP；留空则稍后后台配置）" "${DEF_VIP:-}"; VIP=$REPLY
ask "VIP 掩码位" "$DEF_VIP_PREFIX"; VIP_PREFIX=$REPLY
ask "VRID（同网段多集群需不同）" "$DEF_VRID"; VRID=$REPLY
ask "Web 端口" "$DEF_WEB_PORT"; WEB_PORT=$REPLY
ask "Web 管理员账号" "$DEF_WEB_USER"; WEB_USER=$REPLY

if [ "$ASSUME_YES" = "1" ]; then
  WEB_PASS=$(rand_secret); SECRET_KEY=$(rand_secret)
  info "Web 密码与 SECRET_KEY 已随机生成"
else
  printf '  Web 管理员密码（留空=随机生成）: '
  stty -echo 2>/dev/null || true
  read -r WEB_PASS || true
  stty echo 2>/dev/null || true
  printf '\n'
  [ -n "$WEB_PASS" ] || { WEB_PASS=$(rand_secret); info "已生成随机 Web 密码"; }
fi
SECRET_KEY=$(rand_secret)

if [ -n "$VIP" ] && ping -c1 -W1 "$VIP" >/dev/null 2>&1; then
  warn "VIP $VIP 似乎已被占用（ping 通），请确认它未被其它主机使用！"
fi

# ---------------------------------------------------------------------------
say "4/6 生成 .env 与 docker-compose.yml"
if [ ! -f "$COMPOSE_FILE" ] && [ -f docker-compose-example.yml ]; then
  cp docker-compose-example.yml docker-compose.yml
  info "已由 docker-compose-example.yml 生成 docker-compose.yml"
fi

# 写回 compose（保留行尾注释）
set_yaml() {  # set_yaml KEY VALUE
  _k=$1; _v=$2
  if grep -Eq "^[[:space:]]*$_k:" "$COMPOSE_FILE"; then
    sed -i -E "s|^([[:space:]]*$_k:)[[:space:]]*[^#]*|\1 \"$_v\" |" "$COMPOSE_FILE"
  else
    warn "compose 中没有 $_k，已跳过（请手工确认）"
  fi
}
set_yaml MGMT_MYSQL_HOST "$MGMT_HOST"
set_yaml MGMT_MYSQL_PORT "$MGMT_PORT"
set_yaml MGMT_MYSQL_USER "$MGMT_USER"
set_yaml MGMT_MYSQL_PASS "$MGMT_PASS"
set_yaml MGMT_MYSQL_DB   "$MGMT_DB"
set_yaml MANAGER_PUBLIC_IP "$MGR_IP"
set_yaml INTERFACE "$IFACE"
set_yaml VIP_PREFIX "$VIP_PREFIX"
set_yaml VRID "$VRID"
set_yaml WEB_PORT "$WEB_PORT"
set_yaml WEB_USER "$WEB_USER"
set_yaml WEB_PASS "$WEB_PASS"
# VIP 可选：留空则不在 compose 写入，稍后可在后台「服务端配置」填写
if [ -n "$VIP" ]; then
  if grep -Eq '^[[:space:]]*#[[:space:]]*VIP:' "$COMPOSE_FILE"; then
    sed -i -E "s|^[[:space:]]*#[[:space:]]*(VIP:).*|      \1 \"$VIP\"|" "$COMPOSE_FILE"
    info "已启用并写入 VIP: $VIP"
  elif grep -Eq '^[[:space:]]*VIP:' "$COMPOSE_FILE"; then
    set_yaml VIP "$VIP"
  else
    warn "compose 中未找到 VIP 键，请在 Web「服务端配置」中补填 VIP"
  fi
else
  info "VIP 留空，未写入 compose；可稍后在后台「服务端配置」中填写"
fi

umask 077
cat > .env <<EOF
# Custos 运行配置（由 install.sh 生成；含敏感值，请勿入库）
VERSION=$CUR_VER
COMPOSE_PROJECT_NAME=custos
SECRET_KEY=$SECRET_KEY

# 管理库（仅管理器自身存储，非集群节点）
MGMT_MYSQL_HOST=$MGMT_HOST
MGMT_MYSQL_PORT=$MGMT_PORT
MGMT_MYSQL_USER=$MGMT_USER
MGMT_MYSQL_PASS=$MGMT_PASS
MGMT_MYSQL_DB=$MGMT_DB

# Web / 网络
WEB_PORT=$WEB_PORT
WEB_USER=$WEB_USER
WEB_PASS=$WEB_PASS
MANAGER_PUBLIC_IP=$MGR_IP
INTERFACE=$IFACE
VIP=$VIP
VIP_PREFIX=$VIP_PREFIX
VRID=$VRID
EOF
chmod 600 .env
info "已生成 .env（权限 600）"

mkdir -p data backups generated_nodes logs

# ---------------------------------------------------------------------------
say "5/6 安装命令行工具 custosctl"
if [ "$(id -u)" = "0" ]; then
  sh "$SCRIPT_DIR/scripts/install-custosctl.sh" "$CUSTOS_HOME" || warn "custosctl 安装失败，可稍后手动执行"
else
  warn "非 root，跳过安装 /usr/bin/custosctl。稍后执行：sudo sh $SCRIPT_DIR/scripts/install-custosctl.sh $CUSTOS_HOME"
fi

# ---------------------------------------------------------------------------
say "6/6 构建并启动管理器"
sh scripts/deploy.sh up -d --build || { echo "启动失败，请执行：custosctl tail 查看日志" >&2; exit 1; }

sleep 5
printf '\n'
docker compose ps || true
if command -v curl >/dev/null 2>&1; then
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$WEB_PORT/login" || echo "-")
  info "Web 探活：HTTP $code"
fi

say "安装完成"
info "访问地址：http://$MGR_IP:$WEB_PORT"
info "管理员  ：$WEB_USER / $WEB_PASS"
info "后续常用：custosctl status | custosctl tail | custosctl rebuild | custosctl uninstall"
info "提示：请确认管理库 $MGMT_HOST:$MGMT_PORT 已创建库 $MGMT_DB 且账号可写；否则容器会反复重启（custosctl tail 可见）。"
