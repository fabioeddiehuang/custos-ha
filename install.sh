#!/bin/sh
# ---------------------------------------------------------------------------
# Custos 服务端【交互式一键安装】
#
#   sudo sh install.sh         交互式（推荐）
#   sudo sh install.sh -y      非交互：全部用默认值 + 随机口令（脚本化部署）
#   sudo sh install.sh --with-db  内置管理库一键：自动启用 compose 内置 mgmt-mysql（服务名直连、端口固化 3306），
#                              无需准备任何外部 MySQL，零外部依赖一条命令起管理端（演示/小规模推荐）
#   sh install.sh -C <目录>    指定项目目录（默认：脚本所在目录，即项目根目录）
#
# 做了什么
#   1) 检查 docker / docker compose
#   2) 交互式收集部署参数（管理库、管理器对外 IP、VIP、网卡、Web 账号端口…）
#   3) 生成 .env（仅 VERSION / COMPOSE_PROJECT_NAME / WEB_USER 等非密钥项；SECRET_KEY、WEB_PASS、MGMT_MYSQL_PASS 均不写入 .env：口令类改由 docker-compose.yml 驱动）
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
WITH_DB=0          # P0-2：内置管理库一键（install.sh --with-db）

while [ $# -gt 0 ]; do
  case "$1" in
    -C|--dir) CUSTOS_HOME=$2; shift 2 ;;
    -y|--yes) ASSUME_YES=1; shift ;;
    --with-db) WITH_DB=1; shift ;;
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

# 管理库连通性预检（根治：错误/不可达的 MGMT_MYSQL_HOST 会让管理器启动即连不上管理库、
# 容器反复重启、Web 与「重新生成配置」等全部不可用，且失败被 docker-entrypoint 静默吞掉。
# 安装期校验可提前暴露，避免装完才发现服务起不来。
# 返回 0=通过；非 0=未通过（调用方据此中止安装）。
check_mgmt() {
  _h=$1; _p=$2; _u=$3; _pw=$4; _db=$5
  # P0-2：内置管理库（mgmt-mysql 等容器内服务名）仅在 docker 网络内可达，
  # 本机 TCP 探活必失败（且 mysql 客户端也不在管理端容器内）。跳过预检，
  # 启动顺序由 compose 的 depends_on(condition: service_healthy) 保证。
  case "$_h" in
    mgmt-mysql)
      info "管理库地址 $_h 为容器内服务名（本机不可直连），跳过 TCP 预检（依赖 compose depends_on + healthcheck 保障就绪）"
      return 0
      ;;
  esac
  if command -v python3 >/dev/null 2>&1; then
    if python3 - "$_h" "$_p" "$_u" "$_pw" "$_db" <<'PY'
import sys, socket
h, p, u, pw, db = sys.argv[1:6]
try:
    s = socket.create_connection((h, int(p)), timeout=3)
    s.close()
except Exception as e:
    sys.stderr.write(f"TCP 连接失败: {e}\n")
    sys.exit(2)
sys.exit(0)
PY
    then
      info "管理库 TCP 可达：$_h:$_p"
    else
      echo "错误：管理库 $_h:$_p TCP 不可达（连接失败）。" >&2
      echo "      请确认：① MGMT_MYSQL_HOST 是真实管理库所在主机（独立机，勿填 127.0.0.1、勿填集群节点 IP）；" >&2
      echo "      ② 端口正确且防火墙放行；③ 本机/管理容器能路由到该地址。" >&2
      return 1
    fi
  elif [ -n "${BASH_VERSION:-}" ] && (exec 3<>/dev/tcp/"$_h"/"$_p") 2>/dev/null; then
    exec 3>&- 3<&-
    info "管理库 TCP 可达：$_h:$_p"
  else
    warn "未检测到 python3 且非 bash 环境，跳过管理库 TCP 预检；请手动确认 $_h:$_p 可达后再继续。"
  fi
  # 账号鉴权（mysql 客户端存在才校验，失败仅告警不阻断）
  if command -v mysql >/dev/null 2>&1; then
    if MYSQL_PWD="$_pw" mysql -h "$_h" -P "$_p" -u "$_u" -e "SELECT 1" "$_db" >/dev/null 2>&1; then
      info "管理库账号鉴权通过：$_u@$_h"
    else
      warn "管理库账号鉴权失败（$_u@$_h），但 TCP 已通；若后续容器反复重启请检查账号/口令/库名。"
    fi
  fi
  return 0
}

# ---------------------------------------------------------------------------
# P0-2：启用 compose 内置 mgmt-mysql 服务（install.sh --with-db 时调用）。
# 把模板里被注释的 mgmt-mysql 服务块取消注释、写入 ha 账号口令、加入 ha_net 网络，
# 并把 custos-server 的 depends_on 改为带健康条件；端口固化 3306。
# 幂等：已启用的 compose 再次运行不会重复插入/覆盖用户口令。
enable_builtin_db() {  # enable_builtin_db COMPOSE_FILE HA_PASS ROOT_PASS
  _cf=$1; _hapw=$2; _rootpw=$3
  if ! command -v python3 >/dev/null 2>&1; then
    warn "未检测到 python3，无法自动启用内置 mgmt-mysql；请手工取消 docker-compose.yml 中 mgmt-mysql 注释并填好口令。"
    return 0
  fi
  python3 - "$_cf" "$_hapw" "$_rootpw" <<'PY'
import sys, re
cf, hapw, rootpw = sys.argv[1:4]
lines = open(cf, encoding='utf-8').read().split('\n')
out = []
in_mgmt = False
userpw_added = False
net_added = False
i = 0; n = len(lines)
while i < n:
    line = lines[i]
    # 进入 mgmt-mysql 服务块（可能仍是注释态或已启用）
    if (not in_mgmt) and line.rstrip() in ('  # mgmt-mysql:', '  mgmt-mysql:'):
        in_mgmt = True
        out.append('  mgmt-mysql:')
        i += 1
        continue
    if in_mgmt:
        if line.lstrip().startswith('custos-server:'):
            in_mgmt = False
            out.append(line); i += 1
            continue
        # 取消注释：仅对「# 映射键(ASCII冒号) / # 列表项(- )」生效，描述性注释（如
        # 「Custos（单容器…」「# ----」）保持原样，避免被误当 YAML 键导致解析失败。
        _ls = len(line) - len(line.lstrip())
        _body = line[_ls:]
        _uncommented = False
        if _body.startswith('#') and len(_body) >= 2 and _body[1] == ' ':
            _content = _body[2:]
            _c = _content.lstrip()
            if (re.match(r'^[A-Za-z0-9_\-]+:(\s|$)', _c) or _c.startswith('- ')):
                line = line[:_ls] + _content
                _uncommented = True
        # healthcheck 原写死 -pchange_me，启用内置库后 root 口令已变，须改为容器内环境变量（否则 healthcheck 失败、
        # custos-server 因 depends_on: service_healthy 永远等不到、管理器起不来）
        if '-pchange_me' in line:
            line = line.replace('-pchange_me', '-p$$MYSQL_ROOT_PASSWORD')
        # 服务内容结束后遇到描述性注释时，补 networks（让 mgmt-mysql 与 custos-server 同网，否则 DNS 不通）
        if (not net_added) and _body.startswith('#') and (not _uncommented):
            out.append('    networks:')
            out.append('      - ha_net')
            net_added = True
        if line.lstrip().startswith('networks:'):
            net_added = True
            out.append(line); i += 1
            continue
        if line.lstrip().startswith('MYSQL_USER') or line.lstrip().startswith('MYSQL_PASSWORD'):
            userpw_added = True
            out.append(line); i += 1
            continue
        if 'MYSQL_ROOT_PASSWORD' in line:
            if 'change_me' in line:
                ind = line[:len(line)-len(line.lstrip())]
                line = ind + 'MYSQL_ROOT_PASSWORD: "%s"' % rootpw
            out.append(line); i += 1
            continue
        if 'MYSQL_DATABASE' in line:
            out.append(line); i += 1
            continue
        if line.lstrip().startswith('environment:'):
            # 预检：本块是否已含 MYSQL_USER/MYSQL_PASSWORD（重跑场景，避免重复插入）
            _j = i + 1
            while _j < n and not lines[_j].lstrip().startswith('custos-server:'):
                _s = lines[_j].lstrip()
                if _s.startswith('MYSQL_USER') or _s.startswith('MYSQL_PASSWORD'):
                    userpw_added = True
                _j += 1
            out.append(line)
            if not userpw_added:
                ind = line[:len(line)-len(line.lstrip())]
                out.append(ind + '  MYSQL_USER: "ha"')
                out.append(ind + '  MYSQL_PASSWORD: "%s"' % hapw)
                userpw_added = True
            i += 1
            continue
        out.append(line); i += 1
        continue
    # custos-server 的 depends_on 取消注释并加健康条件
    if line.rstrip() == '    # depends_on:':
        out.append('    depends_on:')
        i += 1
        if i < n and lines[i].rstrip() == '    #   - mgmt-mysql':
            out.append('      mgmt-mysql:')
            out.append('        condition: service_healthy')
            i += 1
        continue
    out.append(line); i += 1
open(cf, 'w', encoding='utf-8').write('\n'.join(out))
print('[install] 内置 mgmt-mysql 已启用（ha 账号口令已写入 compose；root 口令=%s）' % rootpw)
PY
}

# ---------------------------------------------------------------------------
say "1/8 环境检查"
command -v docker >/dev/null 2>&1 || { echo "错误：未安装 docker。" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "错误：docker compose 插件不可用（需 Docker 20.10+）。" >&2; exit 1; }
info "docker：OK"
[ -f "$CUSTOS_HOME/docker-compose.yml" ] || [ -f "$CUSTOS_HOME/docker-compose-example.yml" ] || {
  echo "错误：$CUSTOS_HOME 下找不到 docker-compose.yml 或 docker-compose-example.yml。" >&2; exit 1; }
info "项目目录：$CUSTOS_HOME"

# ---------------------------------------------------------------------------
say "2/8 读取现有配置作为默认值"
CUR_VER=$(grep -E '^VERSION=' .env 2>/dev/null | head -1 | cut -d= -f2 | tr -d ' \t\r\n')
[ -n "$CUR_VER" ] || CUR_VER=$(grep -E '^VERSION=' env.example 2>/dev/null | head -1 | cut -d= -f2 | tr -d ' \t\r\n')
[ -n "$CUR_VER" ] || CUR_VER=v2.2.20

COMPOSE_FILE=$CUSTOS_HOME/docker-compose.yml
cur_val() {  # 从现有 compose 取某个键的值（先去尾随空白再剥引号，兼容重装场景）
  # 关键：值可能含 '#'（如口令 P@ss#123）。旧实现直接 s/[[:space:]]*#.*$// 会把
  # '#123"' 当成注释起点而【静默截断口令】，且 set_yaml 回写时会拼出畸形行
  # （compose 起不来）。故优先按「引号包裹的整体」提取；无引号才退回旧的去注释逻辑。
  # sed 的 t：若引号提取成功则跳到末尾，不再执行后面的去注释（避免误截）。
  grep -E "^[[:space:]]*$1:" "$COMPOSE_FILE" 2>/dev/null | head -1 \
    | sed -E 's/^[^:]*:[[:space:]]*//' \
    | sed -E 's/^"([^"]*)".*$/\1/; t; s/[[:space:]]*#.*$//; s/[[:space:]]*$//; s/^"//; s/"$//' || true
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
DEF_WEB_PASS=$(cur_val WEB_PASS)
DEF_VIP=$(grep -E '^[[:space:]]*VIP:' "$COMPOSE_FILE" 2>/dev/null | head -1 \
  | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//' || true)
[ -n "$DEF_VIP" ] || DEF_VIP=""

info "版本 $CUR_VER；将使用现有值作为默认（直接回车即采用）"

# ---------------------------------------------------------------------------
say "3/8 部署参数"
if [ "$WITH_DB" = "1" ]; then
  # P0-2：内置管理库一键 —— 用 compose 服务名直连，端口固化 3306，ha 账号口令自动生成（内部使用）。
  MGMT_HOST=mgmt-mysql
  MGMT_PORT=3306
  MGMT_USER=ha
  MGMT_DB=ha
  [ -n "$MGMT_PASS" ] || MGMT_PASS=$(rand_secret | head -c 16)
  info "已启用内置管理库（--with-db）：管理库地址=$MGMT_HOST，端口固化 $MGMT_PORT，ha 账号口令已自动生成（内部使用，无需记忆）。"
  info "启动后管理库异常会在日志给出首因，无需准备任何外部 MySQL。"
else
  ask "管理库地址 MGMT_MYSQL_HOST" "$DEF_MGMT_HOST"; MGMT_HOST=$REPLY
  # 容器的 127.0.0.1 是【容器自己】（里面没有 MySQL），填它必然导致容器连不上管理库而反复重启。
  # 这是首次安装最易踩的坑：宿主上若正好跑着 MySQL，下方的 TCP 预检还会「通过」，
  # 误导用户以为配置正确，装完才发现容器起不来。故在此直接拦截，而不是等到启动失败。
  case "$MGMT_HOST" in
    127.0.0.1|localhost|::1|0.0.0.0)
      echo "错误：MGMT_MYSQL_HOST 不能填 $MGMT_HOST。" >&2
      echo "      管理器运行在容器内，容器的 $MGMT_HOST 指向容器自身（其中没有 MySQL），" >&2
      echo "      会导致容器连不上管理库而反复重启（custosctl tail 可见）。" >&2
      echo "      请填管理库所在主机的【真实内网 IP】；若启用 compose 内置 mgmt-mysql 服务则填 mgmt-mysql。" >&2
      exit 1
      ;;
  esac
  ask "管理库端口" "$DEF_MGMT_PORT"; MGMT_PORT=$REPLY
  ask "管理库账号" "$DEF_MGMT_USER"; MGMT_USER=$REPLY
  ask "管理库密码" "${DEF_MGMT_PASS:-}"; MGMT_PASS=$REPLY
  ask "管理库库名" "$DEF_MGMT_DB"; MGMT_DB=$REPLY
fi
ask "管理器对外 IP（节点看到的来源 IP）" "$DEF_MGR_IP"; MGR_IP=$REPLY
ask "节点网卡名 INTERFACE" "$DEF_IFACE"; IFACE=$REPLY
ask "集群 VIP（可选，同网段空闲 IP；留空则稍后后台配置）" "${DEF_VIP:-}"; VIP=$REPLY
ask "VIP 掩码位" "$DEF_VIP_PREFIX"; VIP_PREFIX=$REPLY
ask "VRID（同网段多集群需不同）" "$DEF_VRID"; VRID=$REPLY
ask "Web 端口" "$DEF_WEB_PORT"; WEB_PORT=$REPLY
ask "Web 管理员账号" "$DEF_WEB_USER"; WEB_USER=$REPLY
ask "Web 管理员密码(WEB_PASS，留空=首次启动随机生成并打印日志)" "${DEF_WEB_PASS:-}"; WEB_PASS=$REPLY
# Web 密码含双引号/反斜杠时，写进 compose 的双引号标量会破坏 YAML（compose 起不来）。
# 此时退回留空：首次启动随机生成并打印到日志，之后可在 Web「修改密码」里改，不阻塞安装。
case "$WEB_PASS" in
  *'"'*|*'\'*)
    warn "Web 密码含双引号或反斜杠，无法安全写入 docker-compose.yml（会破坏 YAML）。已改为留空："
    warn "首次启动将随机生成密码并打印到日志（custosctl tail 可见），之后可在 Web「修改密码」中修改。"
    WEB_PASS=""
    ;;
esac

# WEB_PASS 改由 docker-compose.yml 驱动，install.sh 不再生成/写入 .env：
#   - compose 中 WEB_PASS 留空 → 首次启动随机生成并持久化到管理库、打印到日志；
#   - 或直接在 compose 的 environment 里填固定密码（生产建议）。
# 详见 docker-compose-example.yml 的 WEB_PASS 注释。
info "Web 密码(WEB_PASS)：上一步可直接输入固定口令（生产建议）；留空则首次启动自动生成并打印到日志。"

# 管理库连通性预检（根治：错/不可达的 MGMT_MYSQL_HOST 会让管理器起不来、regen 全挂）
if ! check_mgmt "$MGMT_HOST" "$MGMT_PORT" "$MGMT_USER" "$MGMT_PASS" "$MGMT_DB"; then
  echo "管理库预检未通过，已中止安装。请修正 MGMT_MYSQL_HOST/PORT/USER/PASS/DB 后重跑 install.sh。" >&2
  exit 1
fi

if [ -n "$VIP" ] && ping -c1 -W1 "$VIP" >/dev/null 2>&1; then
  warn "VIP $VIP 似乎已被占用（ping 通），请确认它未被其它主机使用！"
fi

# ---------------------------------------------------------------------------
say "4/8 生成 .env 与 docker-compose.yml"
if [ ! -f "$COMPOSE_FILE" ] && [ -f docker-compose-example.yml ]; then
  cp docker-compose-example.yml docker-compose.yml
  info "已由 docker-compose-example.yml 生成 docker-compose.yml"
fi

# 写回 compose（保留行尾注释；不追加多余空格，否则重装时 cur_val 解析会叠加引号导致 YAML 报错）
set_yaml() {  # set_yaml KEY VALUE
  _k=$1; _v=$2
  if grep -Eq "^[[:space:]]*$_k:" "$COMPOSE_FILE"; then
    # 用 awk 按【数据】改写，而不是 sed 正则替换：
    #  - sed 替换串中 & 代表整段匹配、\ 是转义符，值含它们会被错误展开；
    #  - sed 无法表达「引号内的 # 不是注释」，遇含 '#' 的口令会截断并拼出畸形行
    #    （compose 直接起不来）。awk 按字符处理，安全。
    #  - 注释只在「空白+#」处切分，故值内的 '#'（如 P@ss#123）不会被当注释。
    # 经 ENVIRON 传值（而非 -v），避免 awk 把值里的反斜杠当转义序列处理。
    _sy_tmp="${COMPOSE_FILE}.sy.$$"
    if SY_KEY="$_k" SY_VAL="$_v" awk '
      {
        if (!done && $0 ~ "^[ \t]*" ENVIRON["SY_KEY"] "[ \t]*:") {
          line = $0
          if (match(line, /^[ \t]*[^:]*:[ \t]*/)) {
            prefix = substr(line, 1, RLENGTH); rest = substr(line, RLENGTH + 1)
          } else { prefix = line; rest = "" }
          comment = ""
          if (substr(rest, 1, 1) == "\"") {
            # 引号包裹：注释从【闭合引号】之后开始，值内的「空格+#」不算注释
            # （否则口令 "a #b" 会被误切成 值a + 注释#b"，写出畸形行）
            q = index(substr(rest, 2), "\"")
            if (q > 0) { comment = substr(rest, q + 2) }
          } else {
            n = index(rest, " #")
            if (n > 0) { comment = substr(rest, n) }
          }
          print prefix "\"" ENVIRON["SY_VAL"] "\"" comment
          done = 1
          next
        }
        print
      }
    ' "$COMPOSE_FILE" > "$_sy_tmp"; then
      cat "$_sy_tmp" > "$COMPOSE_FILE" && rm -f "$_sy_tmp"
    else
      rm -f "$_sy_tmp"; warn "写入 $_k 失败"
    fi
  else
    warn "compose 中没有 $_k，已跳过（请手工确认）"
  fi
}
set_yaml MGMT_MYSQL_HOST "$MGMT_HOST"
set_yaml MGMT_MYSQL_PORT "$MGMT_PORT"
set_yaml MGMT_MYSQL_USER "$MGMT_USER"
set_yaml MGMT_MYSQL_PASS "$MGMT_PASS"
set_yaml MGMT_MYSQL_DB   "$MGMT_DB"
# P0-2：内置管理库一键（--with-db）：取消注释 compose 中的 mgmt-mysql 服务并写入 ha 账号口令
if [ "$WITH_DB" = "1" ]; then
  _rootpw=$(rand_secret | head -c 16)
  info "正在启用内置 mgmt-mysql 服务（compose 中取消注释 + 填入口令 + 加入 ha_net + depends_on 健康条件）…"
  enable_builtin_db "$COMPOSE_FILE" "$MGMT_PASS" "$_rootpw"
fi
set_yaml MANAGER_PUBLIC_IP "$MGR_IP"
set_yaml INTERFACE "$IFACE"
set_yaml VIP_PREFIX "$VIP_PREFIX"
set_yaml VRID "$VRID"
set_yaml WEB_PORT "$WEB_PORT"
set_yaml WEB_USER "$WEB_USER"
set_yaml WEB_PASS "$WEB_PASS"
# 半同步复制默认开启（主库提交等待从库 ACK，压缩切换数据丢失窗口）
set_yaml SEMI_SYNC "1"
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

# SECRET_KEY 升级迁移：v2.3.8 及更早的 .env 含 SECRET_KEY。重跑 install.sh 会重新
# 生成 .env（不再含 SECRET_KEY），为避免会话签名密钥丢失、进而由它派生的 ENC1: 凭据
# 解不开，这里把旧 .env 的 SECRET_KEY 落到 data/secret_key（仅当该文件尚不存在，绝不覆盖）。
# 全新部署（旧 .env 无 SECRET_KEY，或 data/secret_key 已存在）则跳过，由应用首次启动生成。
OLD_SECRET_KEY=$(grep -E '^SECRET_KEY=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d ' \t\r\n') || true
if [ -n "$OLD_SECRET_KEY" ] && [ ! -f data/secret_key ]; then
  mkdir -p data
  printf '%s' "$OLD_SECRET_KEY" > data/secret_key
  chmod 600 data/secret_key
  info "已将旧 .env 的 SECRET_KEY 迁移持久化到 data/secret_key（凭据密钥保持不变，升级无感）"
fi

# 弱口令开关：重跑 install.sh 时沿用 .env 既有值（幂等，不被重置）；全新部署/缺省 = 1（允许弱口令）
# 写法与 CUR_VER 一致：管道以 tr 收尾，grep 无匹配时退出码仍为 0，不会触发 set -e 中断。
CUR_ALLOW_WEAK=$(grep -E '^ALLOW_WEAK_PASSWORD=' .env 2>/dev/null | head -1 | cut -d= -f2 | tr -d ' \t\r\n')
[ -n "$CUR_ALLOW_WEAK" ] || CUR_ALLOW_WEAK=1

umask 077
cat > .env <<EOF
# Custos 运行配置（由 install.sh 生成；SECRET_KEY、WEB_PASS、MGMT_MYSQL_PASS 均不写入本文件：SECRET_KEY 由 data/secret_key 持久化，WEB_PASS 与 MGMT_MYSQL_PASS 由 docker-compose.yml 驱动）
VERSION=$CUR_VER
COMPOSE_PROJECT_NAME=custos

# 管理库（仅管理器自身存储，非集群节点）
MGMT_MYSQL_HOST=$MGMT_HOST
MGMT_MYSQL_PORT=$MGMT_PORT
MGMT_MYSQL_USER=$MGMT_USER
MGMT_MYSQL_DB=$MGMT_DB

# Web / 网络
WEB_PORT=$WEB_PORT
WEB_USER=$WEB_USER
# 弱口令开关：1=允许（默认，弱口令登录不强制改密、写操作不拦截）；0=安全基线（强制改密+拦截写操作）
ALLOW_WEAK_PASSWORD=$CUR_ALLOW_WEAK
MANAGER_PUBLIC_IP=$MGR_IP
INTERFACE=$IFACE
VIP=$VIP
VIP_PREFIX=$VIP_PREFIX
VRID=$VRID
EOF
chmod 600 .env
info "已生成 .env（权限 600）"

mkdir -p data backups generated_nodes logs

# 凭证密钥保底恢复（v2.3.10）：密钥若随项目目录一起丢失，库里所有 ENC1 凭证将永久解不开。
# 卸载脚本会把它镜像到项目目录之外的 /var/lib/custos/，这里在重装时自动取回 ——
# 于是「删掉整个项目目录后重装」也能自动恢复密钥，凭证照常可读。
# 仅当 data/ 下缺失时才恢复，绝不覆盖现有密钥（现有密钥必然是最近一次在用的）。
KEY_VAULT=/var/lib/custos
for k in cred.key secret_key; do
  if [ ! -f "data/$k" ] && [ -f "$KEY_VAULT/$k" ]; then
    cp -f "$KEY_VAULT/$k" "data/$k" 2>/dev/null || true
    chmod 600 "data/$k" 2>/dev/null || true
    info "已从 $KEY_VAULT/$k 恢复凭证密钥（重装后凭证加解密保持不变）"
  fi
done
# 反向：项目内已有密钥则顺手镜像一份，保证保险库始终是最新的
for k in cred.key secret_key; do
  if [ -f "data/$k" ]; then
    mkdir -p "$KEY_VAULT" 2>/dev/null || true
    cp -f "data/$k" "$KEY_VAULT/$k" 2>/dev/null || true
    chmod 600 "$KEY_VAULT/$k" 2>/dev/null || true
  fi
done

# ---------------------------------------------------------------------------
say "5/8 安装命令行工具 custosctl"
if [ "$(id -u)" = "0" ]; then
  sh "$SCRIPT_DIR/scripts/install-custosctl.sh" "$CUSTOS_HOME" || warn "custosctl 安装失败，可稍后手动执行"
else
  warn "非 root，跳过安装 /usr/bin/custosctl。稍后执行：sudo sh $SCRIPT_DIR/scripts/install-custosctl.sh $CUSTOS_HOME"
fi

# ---------------------------------------------------------------------------
say "6/8 构建并启动管理器"
sh scripts/deploy.sh up -d --build || { echo "启动失败，请执行：custosctl tail 查看日志" >&2; exit 1; }

sleep 5
printf '\n'
docker compose ps || true
if command -v curl >/dev/null 2>&1; then
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$WEB_PORT/login" || echo "-")
  info "Web 探活：HTTP $code"
fi

# ---------------------------------------------------------------------------
say "7/8 构建节点镜像包（curl 引导安装用）"
# 节点的 keepalived 镜像 custos-node:<版本> 是【本地构建】的（Docker Hub 上没有），
# agent 用的 python:3-slim 在离线环境拉不到 —— 缺了任意一个，curl 引导安装都会卡在
# `docker compose up` 这一步（表现为「mariadb/agent 都起来了，只有 keepalived 没有」）。
# 故随首次安装一并构建，落到项目根 ./images/：
#   · .gitignore  已排除 → 不入库（每个约 70MB，属构建产物）
#   · .dockerignore 已排除 → 不打进服务端镜像（避免镜像膨胀，见 backups/ 的教训）
#   · 由 compose 挂载进容器供服务端 HTTP 下发给节点（引导脚本 docker load）
# 失败不阻断安装：节点侧仍可退回本地编译 keepalived，或稍后手动补跑本脚本。
if command -v docker >/dev/null 2>&1; then
  _bbi="bash"; command -v bash >/dev/null 2>&1 || _bbi="sh"
  "$_bbi" "$SCRIPT_DIR/scripts/build_node_images.sh" all \
    || warn "节点镜像包构建失败（不影响管理器运行）：可稍后手动执行 scripts/build_node_images.sh"
else
  warn "未检测到 docker，跳过节点镜像包构建（稍后手动执行 scripts/build_node_images.sh）"
fi

# ---------------------------------------------------------------------------
say "8/8 节点包自检（custosctl node prepare）"
# 非阻断：仅报告各 generated_nodes 节点包的完整性 / 残留情况，发现问题不中止安装。
if command -v custosctl >/dev/null 2>&1; then
  echo "以下为节点包自检（如有 [WARN]/[异常?] 可重新生成节点目录，或用 --clean 清理残留节点目录）："
  custosctl node prepare || true
else
  warn "未找到 custosctl，跳过节点包自检（可稍后手动执行 custosctl node prepare）"
fi

say "安装完成"
info "访问地址：http://$MGR_IP:$WEB_PORT"
if [ -n "$WEB_PASS" ]; then
  info "管理员账号：$WEB_USER（密码：本次安装设置的固定口令）"
else
  info "管理员账号：$WEB_USER（未设固定口令，已由首次启动随机生成并打印到容器日志）"
  info "  获取初始密码：custosctl tail | grep 初始管理员密码"
  info "  或于 compose 的 WEB_PASS 填入固定口令后重启：custosctl restart"
fi
info "后续常用：custosctl status | custosctl tail | custosctl rebuild | custosctl uninstall"
info "提示：请确认管理库 $MGMT_HOST:$MGMT_PORT 已创建库 $MGMT_DB 且账号可写；否则容器会反复重启（custosctl tail 可见）。"
