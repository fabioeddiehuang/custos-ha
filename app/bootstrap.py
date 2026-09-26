"""bootstrap.py — 节点「一行引导安装」（v2.5.0 F1）。

给运维一条 curl 命令，在节点上粘贴执行即完成部署：
    下载单元 → 建目录 → 起服务 → 回执上报

为什么独立成文件
----------------
符合「一个功能一个文件」约定：引导是独立业务功能，与 node_gen（渲染单元）、
api（HTTP 层）解耦，后续接 F2 镜像分发、F3 就绪回执时只改本文件。

安全模型（重要）
----------------
引导包内含集群明文凭据（root / ha_monitor / replication / app 密码），
因此 token 必须：
  1. **绑定节点 IP**——换一台机器拿着 URL 也拿不到；
  2. **短 TTL**（默认 24h，运维可能隔天才去节点上执行）；
  3. **引导完成即失效**（ready 回执成功后作废，防止凭据长期暴露）；
  4. 全程审计（签发人、签发时间、使用来源 IP）。

「一次性」的准确定义：**完成后失效**，而非「下载一次就失效」——
引导过程中要多次请求（单元包、镜像包、回执），且失败重试需要可重复下载。
重复下载的风险由上面 1~3 收敛，而不是靠禁止重试（禁止重试只会让运维卡死）。
"""
import hashlib
import hmac
import io
import logging
import secrets
import tarfile
import time
from pathlib import Path

from .config import (NODES_DIR, WEB_PORT, get_manager_public_ip, NODE_INSTALL_BASE,
                     NODE_DIR_NAME, node_install_dest)

log = logging.getLogger(__name__)

# token 默认有效期（小时）。运维拿到命令后可能隔天才上机，24h 是可用性下限。
DEFAULT_TTL_HOURS = 24

# 节点上的安装根目录。node_gen 的单元目录约定：自包含，拷到 <base>/<name>/ 即可运行。
# 默认基目录 + 统一目录名（不再用 IP），可通过 NODE_INSTALL_BASE 环境变量覆盖，
# 或签发引导命令时按节点传入 install_dir 覆盖。
INSTALL_BASE = NODE_INSTALL_BASE

_SECRET_CACHE = None


def _secret():
    """token 签名密钥：派生自 SECRET_KEY（与登录会话同源，重启不变）。"""
    global _SECRET_CACHE
    if _SECRET_CACHE is None:
        from .config import SECRET_KEY
        _SECRET_CACHE = (SECRET_KEY or "custos-bootstrap").encode("utf-8")
    return _SECRET_CACHE


def _sig(jti, ip, exp):
    msg = ("%s.%s.%d" % (jti, ip, int(exp))).encode("utf-8")
    return hmac.new(_secret(), msg, hashlib.sha256).hexdigest()[:32]


def _jti_hash(jti):
    return hashlib.sha256(jti.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- 目录与单元

def node_dir(ip):
    """节点单元目录（容器内绝对路径），带路径穿越防护。

    ip 来自 URL/用户输入，必须 resolve 后断言仍在 NODES_DIR 之内，
    杜绝 '../../etc' 之类导致的越权读取。
    """
    base = Path(NODES_DIR).resolve()
    d = (base / str(ip).strip()).resolve()
    if base not in d.parents and d != base:
        raise ValueError("非法节点路径：%s" % ip)
    return d


# 需要可执行位的脚本/程序（其余统一 0644）
_EXEC_NAMES = ("custosctl", "node_agent.py", "prepare-logs.sh", "update-node.sh")


def _norm_mode(path, ti):
    """归一化包内权限与属主。

    为什么要归一化：单元里存在 0600 / root-only 的文件（如 keepalived 的
    ha_check.conf）。若原样打包，解压到节点后容器内进程可能读不到，
    keepalived 直接起不来——而这类故障在管理端完全看不出来。
    统一成「目录 755 / 脚本 755 / 其余 644」、属主归 0，消除环境差异。
    """
    ti.uid = 0
    ti.gid = 0
    ti.uname = ""
    ti.gname = ""
    if ti.isdir():
        ti.mode = 0o755
    elif ti.issym():
        pass                      # 符号链接无自身权限，保持原样
    elif path.suffix == ".sh" or path.name in _EXEC_NAMES:
        ti.mode = 0o755
    else:
        ti.mode = 0o644
    return ti


def build_bundle(ip):
    """把节点单元目录打成 tar.gz（内存返回，不落盘）。

    为什么现打不预生成：单元会随「重新生成」变化，预生成的包会与当前配置不一致，
    导致节点装到一份过期配置——这正是 F4 要解决的漂移问题的反面。

    读不到的文件**一律显式报错**（列出清单），绝不静默跳过：
    少一个 ha_check.conf，节点上 keepalived 就起不来，而这种缺文件在管理端
    完全看不出来，只会在真正切换时才发现 VIP 漂不动。
    """
    d = node_dir(ip)
    if not d.is_dir():
        raise FileNotFoundError("节点单元目录不存在：%s（请先生成节点）" % d)

    unreadable = []
    buf = io.BytesIO()

    def _add(tar, path, arcname):
        try:
            ti = tar.gettarinfo(str(path), arcname)
        except OSError as e:
            unreadable.append("%s（%s）" % (path, e))
            return
        if ti is None:
            return
        ti = _norm_mode(path, ti)
        try:
            if ti.isdir():
                tar.addfile(ti)
                for child in sorted(path.iterdir()):
                    _add(tar, child, "%s/%s" % (arcname, child.name))
            elif ti.issym() or not ti.isfile():
                tar.addfile(ti)                       # 链接/特殊文件：只记条目
            else:
                with open(str(path), "rb") as f:
                    tar.addfile(ti, f)
        except OSError as e:
            unreadable.append("%s（%s）" % (path, e))

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        _add(tar, d, str(ip))

    if unreadable:
        raise PermissionError(
            "打包 %s 时有 %d 个文件无法读取，节点装到的单元将不完整：%s。"
            "请确认管理器进程对该目录有读权限（容器通常以 root 运行）。"
            % (ip, len(unreadable), "；".join(unreadable[:5])))
    return buf.getvalue()


# ---------------------------------------------------------------- token 签发/校验

def issue_token(ip, cluster_id=None, actor="", ttl_hours=DEFAULT_TTL_HOURS,
                install_dir=None):
    """签发引导 token，落库并返回 token 字符串。

    token 形如 ``<jti>.<sig>``——**不含 IP**（避免 URL 泄露拓扑），
    IP 由服务端按 jti 查库得到后参与验签。

    install_dir：随 token 一并落库。必须持久化，因为节点 `curl … | bash` 拉脚本时
    走的是 GET /api/node/bootstrap/<token>，该端点此刻已拿不到「签发时传的参数」；
    不落库的话脚本里的 DEST 会回退成默认目录，运维填的自定义目录形同无效。
    """
    from .db import execute
    ip = str(ip).strip()
    if not ip:
        raise ValueError("节点 IP 不能为空")
    try:
        ttl = float(ttl_hours)
    except (TypeError, ValueError):
        ttl = float(DEFAULT_TTL_HOURS)
    if ttl <= 0:
        ttl = float(DEFAULT_TTL_HOURS)

    jti = secrets.token_urlsafe(24)
    exp = int(time.time()) + int(ttl * 3600)
    token = "%s.%s" % (jti, _sig(jti, ip, exp))
    execute(
        "INSERT INTO bootstrap_tokens "
        "(token_hash, ip, cluster_id, install_dir, expires_at, issued_by) "
        "VALUES (%s,%s,%s,%s,FROM_UNIXTIME(%s),%s)",
        (_jti_hash(jti), ip, cluster_id, install_dir or None, exp, actor or ""),
    )
    return token


def _lookup(jti):
    from .db import query
    rows = query("SELECT * FROM bootstrap_tokens WHERE token_hash=%s",
                 (_jti_hash(jti),))
    return rows[0] if rows else None


def _as_ts(v):
    """把各种驱动返回的 DATETIME 统一成时间戳。

    为什么要写得这么啰嗦：MySQL 驱动对 DATETIME 的返回类型并不稳定
    （datetime / str / 极端情况下的 int）。若简单地「取不到就当 0」，
    下面的 `if exp and ...` 会因为 exp=0 而**跳过过期判断**——
    于是 token 变成永不过期，这正是一个静默的安全漏洞。
    """
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return float(v)
    if hasattr(v, "timestamp"):
        try:
            return float(v.timestamp())
        except Exception:
            return 0
    try:
        from datetime import datetime
        return float(datetime.fromisoformat(str(v)).timestamp())
    except Exception:
        return 0


def verify_token(token, ip=None):
    """校验 token：签名 + 有效期 + 未使用（+ 可选 IP 绑定）。

    返回 token 记录 dict，任何一项不通过返回 None（不抛异常，便于 API 统一处理）。
    **fail-closed**：过期时间取不到 / 记录异常一律拒绝，绝不「判不了就放行」。
    """
    token = (token or "").strip()
    if not token or "." not in token:
        return None
    jti, _, sig = token.partition(".")
    row = _lookup(jti)
    if not row:
        return None
    if row.get("used_at"):
        return None
    exp = _as_ts(row.get("expires_at"))
    if exp <= 0:
        return None                      # 无法判定有效期 = 不可用
    if time.time() > exp:
        return None
    if not hmac.compare_digest(_sig(jti, row["ip"], exp), sig or ""):
        return None
    if ip and str(ip).strip() != str(row["ip"]).strip():
        return None
    return row


def consume_token(token, src_ip=""):
    """引导完成回执后作废 token（防止凭据长期可用）。"""
    row = verify_token(token)
    if not row:
        return None
    from .db import execute
    execute("UPDATE bootstrap_tokens SET used_at=NOW(), used_from=%s "
            "WHERE token_hash=%s", (src_ip or "", row["token_hash"]))
    return row


def purge_expired():
    """清理过期 token（避免表无限增长；过期 token 本就不可用）。"""
    try:
        from .db import execute
        execute("DELETE FROM bootstrap_tokens WHERE expires_at < NOW()")
    except Exception as e:      # 表不存在等：不影响主流程
        log.debug("清理过期引导 token 失败：%s", e)


def list_tokens(ip=None):
    from .db import query
    if ip:
        return query("SELECT ip, cluster_id, expires_at, issued_at, issued_by, "
                     "used_at, used_from FROM bootstrap_tokens "
                     "WHERE ip=%s ORDER BY issued_at DESC LIMIT 20", (ip,))
    return query("SELECT ip, cluster_id, expires_at, issued_at, issued_by, "
                 "used_at, used_from FROM bootstrap_tokens "
                 "ORDER BY issued_at DESC LIMIT 50")


# ---------------------------------------------------------------- 引导脚本

def manager_base_url():
    """管理端对外地址（节点要能访问它下载单元包）。"""
    ip = get_manager_public_ip() or ""
    return "http://%s:%d" % (ip, int(WEB_PORT))


def _install_dest(install_dir=None):
    """节点在目标机上的安装目录：install_dir 优先（视为【基目录】），否则默认 base/统一名。

    基目录语义：填 /opt → 实际装到 /opt/custos-node，节点文件不会散落在 /opt 根目录。
    """
    if install_dir:
        return node_install_dest(install_dir)
    return node_install_dest(NODE_INSTALL_BASE)


def node_install_cmds(ip, node_type, sidecar_only=False, install_dir=None):
    """按节点形态给出安装/启动命令序列。

    裸机 MariaDB（mariadb）：数据库本体由 Install-ha-mariadb-Node.sh 全量安装
    （铁律 3：不要另写数据库安装逻辑），随后只起 keepalived + agent 两个 sidecar。
    容器形态（docker / mariadb-docker）：compose 一次拉起全部服务。

    sidecar_only=True（**接管场景**）：只在已有数据库上装 keepalived + agent。
    必须跳过 Install 脚本——接管的是**正在跑业务的库**，跑安装脚本等于重装数据库。

    安装脚本位于单元目录的 <MARIADB_NODE_CONF_DIR>/scripts/ 下（与 node_gen 生成一致），
    此前误写成 scripts/ 顶层导致裸机形态引导后不自动安装，已修正。
    """
    dest = _install_dest(install_dir)
    if node_type == "mariadb" or sidecar_only:
        cmds = ["cd %s && docker compose up -d keepalived custos-node-agent" % dest]
        if node_type == "mariadb" and not sidecar_only:
            from .node_gen import MARIADB_NODE_CONF_DIR
            cmds.insert(0, "bash %s/%s/scripts/Install-ha-mariadb-Node.sh"
                        % (dest, MARIADB_NODE_CONF_DIR))
        return cmds
    return ["cd %s && docker compose up -d" % dest]


def available_images():
    """列出服务端已托管的节点镜像（F2）。

    没有镜像包时返回空列表，引导脚本会跳过 docker load——
    此时依赖节点本地构建（compose 里带 build: ./keepalived）或节点已有镜像。
    """
    import os
    try:
        from .config import IMAGES_DIR
        d = IMAGES_DIR
    except Exception:
        return []
    try:
        if not os.path.isdir(d):
            return []
        return sorted(f[:-4] for f in os.listdir(d) if f.endswith(".tar"))
    except OSError:
        return []


def render_bootstrap_script(ip, token, node_type="docker", images=None,
                            sidecar_only=False, install_dir=None):
    """渲染引导安装脚本（bash）。

    脚本是**幂等**的：重复执行会覆盖单元并重新 up，便于失败重试与后续升级
    （这正是存量老节点升级 agent 的路径）。

    sidecar_only：接管已有数据库时只装 keepalived + agent，跳过数据库安装。
    install_dir：节点在目标机上的安装目录（覆盖默认 base/统一名）。
    """
    mgr = manager_base_url()
    dest = _install_dest(install_dir)
    cmds = node_install_cmds(ip, node_type, sidecar_only=sidecar_only,
                             install_dir=install_dir)

    # 镜像分发（F2）：服务端托管 tar，节点 docker load。未配置时跳过。
    if images is None:
        images = available_images()
    img_lines = []
    for name in (images or []):
        img_lines.append(
            '  step "载入镜像 %s"' % name)
        img_lines.append(
            '  curl -fsSL "$MGR/api/node/image/%s?token=$TOKEN" -o "$TMP/%s.tar" \\\n'
            '    && docker load -i "$TMP/%s.tar"' % (name, name, name))
    img_block = "\n".join(img_lines) if img_lines else "  # 未配置服务端镜像包，节点需本地已有对应镜像"

    install_block = "\n".join('  step "%s"\n  %s' % (c, c) for c in cmds)

    # keepalived 镜像标签校正：compose 要求 `custos-node:<PKG_VERSION>`（如 v2.5.0，带 v），
    # 而服务端托管的 tar 可能是历史版本导出的 `custos-node:2.5.0`（不带 v）。
    # 标签对不上时 compose 会退回 build ./keepalived（需联网编译，离线节点直接失败，
    # 表现为「mariadb/agent 都起来了，只有 keepalived 没有」）。
    # 这里在 load 之后、compose up 之前按需补标签，旧镜像包也能直接用。
    try:
        from .node_gen import PKG_VERSION, KEEPALIVED_IMAGE_NAME
        _want = "%s:%s" % (KEEPALIVED_IMAGE_NAME, PKG_VERSION)
    except Exception:
        _want = "custos-node:v2.5.0"
    _repo = _want.split(":", 1)[0]
    tagfix_block = (
        'step "校正 keepalived 镜像标签（需要 %s）"\n'
        'if ! docker image inspect "%s" >/dev/null 2>&1; then\n'
        '  _src=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep -m1 "^%s:" || true)\n'
        '  if [ -n "$_src" ]; then\n'
        '    docker tag "$_src" "%s" && echo "  已将 $_src 另标为 %s"\n'
        '  else\n'
        '    echo "  未找到 %s 系列镜像，compose 将尝试本地编译 build: ./keepalived"\n'
        '  fi\n'
        'else\n'
        '  echo "  镜像 %s 已存在，无需处理"\n'
        'fi' % (_want, _want, _repo, _want, _want, _repo, _want)
    )

    tpl = r"""#!/usr/bin/env bash
# Custos 节点引导安装脚本（由管理端生成）
#
# ⚠ 本脚本内含一次性凭据 token，仅用于在本节点完成部署，请勿外传。
# token 绑定本机 IP、有效期有限、部署成功后自动作废。
set -euo pipefail

IP="__IP__"
TOKEN="__TOKEN__"
MGR="__MGR__"
DEST="__DEST__"
TMP="$(mktemp -d)"
UNIT="$TMP/custos-unit.tgz"

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }
die()  { printf '\n\033[1;31m[失败] %s\033[0m\n' "$1" >&2; exit 1; }
trap 'rm -rf "$TMP"' EXIT

# 1) 环境检查：本期仅支持 docker 环境（无 docker 节点的安装留后续版本）
[ "$(id -u)" = "0" ] || die "请用 root 执行（sudo bash）"
command -v docker >/dev/null 2>&1 || die "未检测到 docker。本版本仅支持已安装 docker 的节点。"
docker compose version >/dev/null 2>&1 || die "未检测到 docker compose（v2 插件）。"

step "下载节点部署单元"
curl -fsSL "$MGR/api/node/bundle/$TOKEN" -o "$UNIT" \
  || die "下载失败：token 可能已过期或已使用，请在管理端重新生成引导命令。"

__IMAGES__

__TAGFIX__

step "解压到 $DEST"
mkdir -p "$DEST"
tar -xzf "$UNIT" -C "$DEST" --strip-components=1 \
  || die "解压失败"

step "登记节点目录（供 custosctl node 任意目录调用）"
mkdir -p /etc/custos
printf '# Custos 节点包目录（custosctl node 自动发现用）\nNODE_DIR=%s\n' "$DEST" > /etc/custos/node.env
chmod 644 /etc/custos/node.env

step "安装/刷新命令行工具 custosctl"
# 必须每次引导都刷新：只写 node.env 而不换二进制的话，节点上残留的旧版 custosctl
# 不认识 node.env，仍旧只会用当前目录 —— 表现为「custosctl node 只能在节点包目录里跑」。
if [ -f "$DEST/custosctl" ]; then
  install -m 0755 "$DEST/custosctl" /usr/bin/custosctl
else
  echo "[warn] 单元内未带 custosctl，跳过安装（不影响节点服务）"
fi

__INSTALL__

step "上报部署结果"
curl -fsSL -X POST "$MGR/api/nodes/$IP/ready" \
     -d "token=$TOKEN" -d "host=$IP" -o /dev/null \
  || echo "[warn] 回执上报失败（部署本身可能已成功，请在管理端确认节点状态）"

printf '\n\033[1;32m[完成] 节点 %s 部署流程已执行完毕。\033[0m\n' "$IP"
printf '请回到管理端「节点管理」确认状态变为「在线」。\n'
"""
    out = tpl.replace("__IP__", str(ip))
    out = out.replace("__TOKEN__", str(token))
    out = out.replace("__MGR__", mgr)
    out = out.replace("__DEST__", dest)
    out = out.replace("__IMAGES__", img_block)
    out = out.replace("__TAGFIX__", tagfix_block)
    out = out.replace("__INSTALL__", install_block)
    return out


def bootstrap_command(ip, token):
    """页面展示的一行命令（k3s 风格）。"""
    return "curl -fsSL %s/api/node/bootstrap/%s | sudo bash" % (
        manager_base_url(), token)


def issue_for_node(ip, node_type="docker", cluster_id=None, actor="",
                   ttl_hours=DEFAULT_TTL_HOURS, images=None, sidecar_only=False,
                   install_dir=None):
    """一次性产出「命令 + 脚本 + token」，供 API/页面直接使用。"""
    token = issue_token(ip, cluster_id=cluster_id, actor=actor, ttl_hours=ttl_hours,
                        install_dir=install_dir)
    dest = _install_dest(install_dir)
    return {
        "ip": ip,
        "token": token,
        "command": bootstrap_command(ip, token),
        "script": render_bootstrap_script(ip, token, node_type, images,
                                          sidecar_only=sidecar_only,
                                          install_dir=install_dir),
        "expires_in_hours": ttl_hours,
        "manager": manager_base_url(),
        "install_base": dest,
        "sidecar_only": bool(sidecar_only),
    }
