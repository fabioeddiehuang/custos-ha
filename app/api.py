"""FastAPI Web 管理后台：登录鉴权、拓扑展示、手动切换、配置管理、历史、节点一键部署、改密码。"""
import hashlib
import json
import subprocess
import socket
import threading
import re
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, date, time, timedelta, timezone
from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from itsdangerous import URLSafeTimedSerializer, BadSignature
import os
import logging
from logging.handlers import RotatingFileHandler
from urllib.parse import quote

from .config import (APP_VERSION, SECRET_KEY, WEB_USER, ROLE_ADMIN, ROLE_VIEWER, rand_secret, get_manager_public_ip, get_manager_public_ip_source, get_manager_docker_subnet_wildcard, get_mgmt_host, MGMT, validate_config, hash_pwd, verify_pwd, is_weak_password, FAILOVER_COOLDOWN_SEC, ALLOW_WEAK_PASSWORD)
from .db import (query, execute, get_conn, get_setting, upsert_setting,
                 get_user_role, list_users, get_user, create_user, set_user_role,
                 delete_user, count_admins)
from .state import get_current_master
from .failover import do_failover, do_failover_to, load_creds, ensure_topology, node_root, _LAST_FAILOVER_TS
from .node_gen import (generate_node, list_nodes, delete_node, delete_node_dir,
                       sync_node_creds, render_fix_node_creds_sql,
                       render_fix_node_creds_sql_mariadb, get_next_server_id)
from .clusters import list_clusters, validate_node_version, get_cluster, create_cluster, update_cluster, delete_cluster, get_cluster_creds
from .alerts import send_alert, last_error, smtp_view
from .ha_client import (connect, heal_replication, heal_io_gtid_gap, heal_io_auth,
                       get_status, make_slave_of, is_gtid_purged_error,
                       fix_node_mysql_params, reset_replication_creds_on_master,
                       restart_slave_io, connect_robust)
from .monitor import run_once, start_in_background, _probe_all, trigger_act_once_async
from . import precheck, audit, metrics, slow_log, config_drift, docker_helper, backup, os_metrics
from . import rebuild, consistency
from .mysql_compat import detect_flavour, flavour_from_node_type, F_MARIADB

# ---------------------------------------------------------------------------
# 全站统一使用中国时间（Asia/Shanghai = UTC+8）
#   1) 强制把本进程本地时区设为上海，使 datetime.now() 等一律返回北京时间；
#   2) 日志 Formatter 单独挂 converter，即便系统时区未生效也保证日志为 +08:00。
# ---------------------------------------------------------------------------
CN_TZ_NAME = "Asia/Shanghai"
CN_TZ_OFFSET_HOURS = 8


def _set_cn_timezone():
    """把本进程本地时区固定为中国时间（不依赖宿主机/容器 TZ 设置）。"""
    try:
        os.environ.setdefault("TZ", CN_TZ_NAME)
        if hasattr(_time, "tzset"):
            _time.tzset()
    except Exception as e:  # 时区设置失败不应阻断启动
        print(f"[warn] 设置中国时区失败：{e}")


_set_cn_timezone()


def _cn_time_converter(*args):
    """logging.Formatter.converter：把 asctime 固定为北京时间(UTC+8)。"""
    return (datetime.utcnow() + timedelta(hours=CN_TZ_OFFSET_HOURS)).timetuple()


def now_cn() -> datetime:
    """当前中国时间（naive datetime，已含 +08:00 偏移）。"""
    return datetime.utcnow() + timedelta(hours=CN_TZ_OFFSET_HOURS)

# ---------------------------------------------------------------------------
# 管理器运行日志（「系统设置 → 服务端容器日志」读取此文件）
# 容器内通常不挂载 docker.sock，故改为将运行日志写入本地文件，便于查看。
# ---------------------------------------------------------------------------
_LOG_DIR = os.environ.get("LOG_DIR", "/app/data/logs")
try:
    os.makedirs(_LOG_DIR, exist_ok=True)
    _MANAGER_LOG = os.path.join(_LOG_DIR, "custos.log")
    _fh = RotatingFileHandler(_MANAGER_LOG, maxBytes=5 * 1024 * 1024,
                              backupCount=3, encoding="utf-8")
    _cn_fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s")
    # 日志时间固定为中国时间(UTC+8)
    _cn_fmt.converter = _cn_time_converter
    _fh.setFormatter(_cn_fmt)
    _fh.setLevel(logging.INFO)
    _root = logging.getLogger()
    _already = any(getattr(h, "baseFilename", "").endswith("custos.log")
                   for h in _root.handlers)
    if not _already:
        _root.addHandler(_fh)
        if _root.level == logging.NOTSET:
            _root.setLevel(logging.INFO)
except Exception:
    _MANAGER_LOG = os.environ.get("LOG_DIR", "/app/data/logs") + "/custos.log"

# ---------------------------------------------------------------------------
# 控制台日志落盘（Tee）
# 应用内大量输出走 print()（entrypoint / ha_client / monitor / failover），
# 这些不经过 logging 模块，导致 custos.log 恒为空：docker.sock 不可用时
# 「容器日志」就完全没内容可读。这里把 stdout/stderr 同步写一份到控制台日志，
# 终端输出照旧，同时保证文件兜底数据源始终有内容。
# ---------------------------------------------------------------------------
_MANAGER_CONSOLE_LOG = os.path.join(_LOG_DIR, "custos-console.log")
try:
    import sys as _sys

    class _Tee(object):
        """把输出同时写到多个流（终端 + 日志文件），任一路失败不影响其它路。"""

        def __init__(self, *streams):
            self._streams = [s for s in streams if s is not None]

        def write(self, data):
            for _s in self._streams:
                try:
                    _s.write(data)
                    _s.flush()
                except Exception:
                    pass
            return len(data or "")

        def flush(self):
            for _s in self._streams:
                try:
                    _s.flush()
                except Exception:
                    pass

        def isatty(self):
            return False

    # 超过 20MB 先截断，避免无界增长（容器重启后重新累积）
    try:
        if os.path.getsize(_MANAGER_CONSOLE_LOG) > 20 * 1024 * 1024:
            open(_MANAGER_CONSOLE_LOG, "w").close()
    except OSError:
        pass
    _console_stream = open(_MANAGER_CONSOLE_LOG, "a", encoding="utf-8", errors="replace")
    _sys.stdout = _Tee(_sys.__stdout__, _console_stream)
    _sys.stderr = _Tee(_sys.__stderr__, _console_stream)
except Exception:
    _console_stream = None

app = FastAPI()


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    """安全响应头（Phase 4 安全基线，呼应渗透测试整改）：防点击劫持/嗅探/Referrer 泄露。"""
    resp = await call_next(request)
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline'; object-src 'none'; frame-ancestors 'none'",
    )
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), camera=(), microphone=()")
    if "Server" in resp.headers:
        del resp.headers["Server"]
    return resp
TEMPLATES = Environment(
    loader=FileSystemLoader(str(Path(__file__).parent / "templates")),
    autoescape=select_autoescape(["html"]),
)
TEMPLATES.globals["APP_VERSION"] = APP_VERSION


def display_node_type(nt):
    """部署形态（node_type）对外展示名：docker 形态即「容器化 MySQL」，统一显示为 MySQL。"""
    nt = (nt or "mariadb-docker").strip()
    if nt == "docker":
        return "MySQL"
    if nt in ("mariadb", "mariadb-docker"):
        return "MariaDB"
    return nt


TEMPLATES.globals["display_node_type"] = display_node_type
serializer = URLSafeTimedSerializer(SECRET_KEY)
# 启动期安全校验：SECRET_KEY 为默认值则拒绝加载（避免会话被伪造）
validate_config()
COOKIE = "ha_session"
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")

# 随 Web 进程内常驻后台监控线程：uvicorn 加载本模块即自动拉起，无需再单独 `python -m app.monitor`。
# monitor 负责探活、选主、建复制通道、推送 current_master（keepalived 据此漂 VIP），是 HA 的核心驱动。
start_in_background()




def auth_required(request: Request):
    """校验登录态并返回用户名。

    写操作（非 GET/HEAD/OPTIONS）额外做角色校验：只读账号 role=viewer 一律 403。
    刻意在这一处统一拦截 —— 若逐个接口加装饰器，漏一个就等于「只读账号可写」，
    而本项目有上百个写接口，逐个标注不可能审计周全。
    """
    token = request.cookies.get(COOKIE)
    if not token:
        raise HTTPException(status_code=307, headers={"Location": "/login"})
    try:
        user = serializer.loads(token, max_age=86400 * 7)
    except BadSignature:
        raise HTTPException(status_code=307, headers={"Location": "/login"})
    if request.method not in ("GET", "HEAD", "OPTIONS"):
        if get_user_role(user) == ROLE_VIEWER:
            raise HTTPException(
                status_code=403,
                detail="当前登录账号为「只读」（viewer），无权执行写操作"
                       "（重新生成配置 / 故障切换 / 备份恢复 / 修改设置 等）。"
                       "请退出登录后，改用管理员账号重新登录再试。",
            )
        # 首登弱口令强制改密：弱口令未改前，所有写操作被拦截（Phase 4 安全基线）
        # ⚠ 但「修改密码」接口本身必须放行，否则登录弱口令被打上 ha_must_change=1 后，
        #   该 cookie 又把改密接口自己挡掉 → 用户永远无法改密解锁（死锁），所有写操作永久不可用。
        #   改密接口仍需校验「当前密码」，放行不会造成越权，是安全且必要的逃生口。
        if request.cookies.get("ha_must_change") == "1" and request.url.path != "/change_password":
            raise HTTPException(status_code=403,
                                detail="请先修改密码（当前为默认/弱口令），改密后方可操作")
    return user


def render(name, request, **ctx):
    ctx["request"] = request
    return HTMLResponse(TEMPLATES.get_template(name).render(ctx))


def _json_default(o):
    """json.dumps 兜底：datetime 等类型序列化为字符串，杜绝裸 500。"""
    if isinstance(o, (datetime, date, time)):
        return o.isoformat()
    return str(o)


# 模板里的 `{{ obj|tojson }}` 默认只走 json.dumps，遇到 datetime / Decimal 会
# 抛 TypeError → 整页 500（集群页正是因此打不开）。这里给 Jinja 环境挂上
# json.dumps_kwargs 策略，让模板内所有 tojson 都用同一套兜底序列化。
try:
    TEMPLATES.policies["json.dumps_kwargs"] = {
        "default": _json_default,
        "ensure_ascii": False,
        "sort_keys": False,
    }
except Exception:
    pass


def _json_ok(payload):
    """安全序列化 JSON 响应：任何不可序列化类型用 default 兜底，并把序列化异常
    也转成 ok:False（HTTP 200），避免「Internal Server Error」裸 500（前端表现为
    「同步失败: HTTP 500 非JSON响应: Internal Server Error」）。"""
    try:
        body = json.dumps(payload, default=_json_default, ensure_ascii=False)
    except Exception as e:
        body = json.dumps({"ok": False, "error": f"序列化失败: {type(e).__name__}: {e}"},
                          ensure_ascii=False)
    return Response(content=body, media_type="application/json")


def _parse_downstream(s):
    """把 hosts.downstream（JSON 文本）解析为从库列表；解析失败返回空列表。"""
    if not s:
        return []
    try:
        v = json.loads(s)
        return v if isinstance(v, list) else []
    except Exception:
        return []


def _build_status_payload():
    """构造与仪表盘同源的状态载荷（含 hosts.err / display_role / is_cm）。

    直接返回普通 dict，由 _json_ok 负责序列化，避免 FastAPI 在框架层
    jsonable_encoder 抛 500（函数内 try 捕获不到序列化阶段异常）。"""
    hosts = query(
        "SELECT h.host, h.port, h.cluster_id, h.role, h.status, h.is_current_master, "
        "h.initial_role, h.repl_delay, h.slave_io, h.slave_sql, h.last_seen, h.err, "
        "h.semi_sync, h.semi_clients, h.semi_no_times, h.semi_no_tx, h.semi_yes_tx, "
        "h.repl_state, h.upstream, h.downstream "
        "FROM hosts h ORDER BY h.id"
    )
    cms = {}
    for r in query("SELECT cluster_id, host, port FROM current_master"):
        cms[r["cluster_id"]] = f"{r['host']}:{r['port']}"
    hosts_out = []
    for h in hosts:
        cm_str = cms.get(h["cluster_id"])
        is_cm = (f"{h['host']}:{h['port']}" == cm_str)
        hh = dict(h)
        hh["display_role"] = "master" if is_cm else "slave"
        hh["is_cm"] = 1 if is_cm else 0
        hh["downstream"] = _parse_downstream(h.get("downstream"))
        hosts_out.append(hh)
    clusters = [{"id": c["id"], "name": c.get("name") or f"集群{c['id']}"}
                for c in list_clusters()]
    switch_history = query(
        "SELECT h.id, h.from_host, h.from_port, h.to_host, h.to_port, h.reason, "
        "h.cluster_id, c.name AS cluster_name, h.created_at "
        "FROM switch_history h LEFT JOIN clusters c ON c.id=h.cluster_id "
        "ORDER BY h.id DESC LIMIT 100"
    )
    return {
        "ok": True,
        "current_masters": cms,
        "hosts": hosts_out,
        "clusters": clusters,
        "switch_history": switch_history,
    }


def _vip_online(host, port, timeout=2.0):
    """TCP 连通性探测：VIP 是否在线（能连上 VIP:vip_port 即视为在线）。

    纯展示用辅助函数，不依赖、也不改动 HA 核心逻辑（failover/monitor/clusters）。
    仅做 TCP 三次握手探测，不涉及任何 MySQL 鉴权或复制操作。
    """
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def _port_online(host, port, timeout=2.0):
    """TCP 连通性探测：节点 MySQL 端口是否可达（管理器→节点 段）。"""
    return _vip_online(host, port, timeout)


@app.get("/login")
def login_page(request: Request):
    return render("login.html", request)


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    try:
        conn = get_conn()
        try:
            with conn.cursor() as c:
                c.execute("SELECT password_hash FROM users WHERE username=%s", (username,))
                row = c.fetchone()
        finally:
            conn.close()
        if not row or not verify_pwd(row["password_hash"], password):
            return render("login.html", request, error="用户名或密码错误")
    except Exception as e:
        # 管理库异常（DB 中断/连接超时等）必须落日志——否则登录失败在 custos.log 毫无痕迹，
        # 运维无法区分「口令错」与「系统错」，DB 抖动等故障极难定位（2026-10-08 实测发现）。
        try:
            logging.getLogger("custos.api").exception(
                "登录失败：管理库异常(user=%s): %s", username, e)
        except Exception:
            pass
        return render("login.html", request, error=f"管理库错误: {e}")
    resp = RedirectResponse("/", status_code=303)
    resp.set_cookie(COOKIE, serializer.dumps(username), httponly=True)
    # 首登弱口令强制改密：弱口令登录后打标记，后端拦截写操作，前端弹改密框
    #   允许弱口令模式（ALLOW_WEAK_PASSWORD=1，内网/测试自便）下不打标记，写操作不被拦截。
    if is_weak_password(password) and not ALLOW_WEAK_PASSWORD:
        resp.set_cookie("ha_must_change", "1", httponly=True, max_age=60 * 60 * 24 * 365)
    else:
        resp.delete_cookie("ha_must_change")
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie(COOKIE)
    return resp


@app.get("/")
def dashboard(request: Request, user=Depends(auth_required)):
    """仪表盘：按集群分组展示各集群拓扑、状态与当前主库。"""
    try:
        clusters = list_clusters()
        dash = []
        for cl in clusters:
            cid = cl["id"]
            hosts = query(
                "SELECT h.host, h.port, h.role, h.status, h.is_current_master, h.initial_role, "
                "h.repl_delay, h.slave_io, h.slave_sql, h.last_seen, h.err, "
                "h.semi_sync, h.semi_clients, h.semi_no_times, h.semi_no_tx, h.semi_yes_tx, "
                "h.repl_state, h.upstream, h.downstream "
                "FROM hosts h WHERE h.cluster_id=%s ORDER BY h.id", (cid,)
            )
            cm = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cid,))
            current_master = f"{cm[0]['host']}:{cm[0]['port']}" if cm else "未选举"
            # 显示用角色以 current_master 表为准（权威），避免 monitor 依「复制通道有无」
            # 判定的 hosts.role（如残留主库/旧主重入仍挂通道）与真实主库打架
            annotated = []
            for h in hosts:
                is_cm = 1 if (f"{h['host']}:{h['port']}" == current_master) else 0
                hh = dict(h)
                hh["display_role"] = "master" if is_cm else "slave"
                hh["is_cm"] = is_cm
                hh["downstream"] = _parse_downstream(h.get("downstream"))
                annotated.append(hh)
            dash.append(dict(
                cluster_id=cid, name=cl.get("name") or f"集群{cid}",
                vip=cl.get("vip") or "—",
                vip_port=cl.get("vip_port") or 3307,
                total_nodes=len(annotated),
                online=sum(1 for h in annotated if h["status"] == "up"),
                offline=len(annotated) - sum(1 for h in annotated if h["status"] == "up"),
                masters=sum(1 for h in annotated if h["is_cm"]),
                slaves=sum(1 for h in annotated if not h["is_cm"]),
                current_master=current_master,
                hosts=annotated,
            ))
        history = query(
            "SELECT h.id, h.from_host, h.from_port, h.to_host, h.to_port, h.reason, "
            "h.cluster_id, c.name AS cluster_name, h.created_at "
            "FROM switch_history h LEFT JOIN clusters c ON c.id=h.cluster_id "
            "ORDER BY h.id DESC LIMIT 100"
        )
    except Exception as e:
        dash, history = [], []
        err = str(e)
    else:
        err = None
    # 版本号唯一来源：.env 的 VERSION=（经 config.APP_VERSION 读取）
    version = APP_VERSION
    return render("dashboard.html", request, user=user, clusters=dash,
                  history=history, error=err, version=version)


@app.get("/api/status")
def api_status(user=Depends(auth_required)):
    try:
        return _json_ok(_build_status_payload())
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/selfheal/stats")
def api_selfheal_stats(user=Depends(auth_required)):
    """自愈动作统计（进程内计数，管理器重启清零）。

    自愈做得越多，越需要能回答「它到底动过什么、成功率如何」——否则自愈本身
    就成了黑盒，出问题时无从判断是它没做、还是做了但做错了。
    """
    try:
        from .monitor import _SELFHEAL_STATS
        stats = dict(_SELFHEAL_STATS)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"})
    stats["total"] = sum(v for v in stats.values() if isinstance(v, int))
    return JSONResponse({"ok": True, "stats": stats})


@app.post("/api/refresh")
def api_refresh(user=Depends(auth_required)):
    """立即并发探活并刷新状态。手动「刷新全部」按钮调用。

    提速设计：① 仅做并发探活（_probe_all，≈单超时而非 N×超时）后立即回读状态返回，
    不再等待互斥锁与切换/自愈等重操作；② 拓扑维护（建复制/漂VIP/自愈）交给后台常驻
    monitor（约 3s 内自动收敛），并额外非阻塞触发一次（trigger_act_once_async），
    既快速响应刷新，又尽快对齐复制，刷新按钮不再卡顿。

    返回与 /api/status 同构的载荷，便于前端直接替换；任何 datetime 字段由 _json_ok 兜底序列化，
    杜绝裸 500。探活失败不抛异常，仅标记 ok=False 让前端展示错误而非崩溃。
    """
    err = None
    try:
        _probe_all()
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        print(f"[api] /api/refresh 探活异常(可忽略): {e}")
    # 非阻塞触发拓扑维护（建复制/漂VIP/自愈），不拖慢本次刷新响应
    try:
        trigger_act_once_async()
    except Exception:
        pass
    try:
        payload = _build_status_payload()
    except Exception as e:
        payload = {"ok": False, "error": f"{type(e).__name__}: {e}", "hosts": [],
                   "clusters": [], "switch_history": [], "current_masters": {}}
    # ok 仅在「探活无异常」且「状态读取成功」时为真；否则透出错误信息
    payload["ok"] = (err is None) and payload.get("ok", True)
    if err:
        payload["msg"] = "探活过程中出错: " + err
    return _json_ok(payload)


@app.get("/api/vip_status")
def api_vip_status(user=Depends(auth_required)):
    """仪表盘集群卡片顶部 VIP 在线检测（TCP 连通 VIP:vip_port）。

    新增展示功能，不改动 HA 核心逻辑（failover/monitor/clusters）。
    各集群并发探测（线程池），返回 {cluster_id: {online, vip, vip_port}}。
    """
    try:
        rows = query(
            "SELECT id, vip, vip_port FROM clusters WHERE vip IS NOT NULL AND vip <> ''"
        )
        result = {}

        def probe(r):
            cid = r["id"]
            vip = r["vip"]
            vip_port = int(r["vip_port"]) if r.get("vip_port") else 3307
            # 候选探测端口：裸机节点原生 MySQL 仅监听「节点 MySQL 端口」（无容器端口映射），
            # VIP 只能经节点端口访问；docker 节点 vip_port 与节点端口均可。
            # 统一以「节点端口优先、vip_port 兜底」探测，避免裸机 VIP 被误报离线。
            nrows = query("SELECT ip, port, node_type FROM nodes WHERE cluster_id=%s", (cid,))
            cand_ports = []
            for nr in nrows:
                try:
                    p = int(nr["port"])
                except (TypeError, ValueError):
                    p = 0
                if p and p not in cand_ports:
                    cand_ports.append(p)
            if vip_port and vip_port not in cand_ports:
                cand_ports.append(vip_port)
            online = False
            eff_port = None
            for p in cand_ports:
                if _vip_online(vip, p):
                    online = True
                    eff_port = p
                    break
            # 管理器可能不在 VIP 同一 L2 段（跨子网/跨机），TCP 直探 VIP 会失败，
            # 但 VIP 实际由同段节点持有、节点侧可 ping 通。此时以「节点可达」兜底判定
            # VIP 在线（避免误报离线），并标记 via_nodes 供前端提示。
            via_nodes = False
            if not online:
                for nr in nrows:
                    try:
                        p = int(nr["port"])
                    except (TypeError, ValueError):
                        p = 0
                    if p and _port_online(nr["ip"], p):
                        online = True
                        via_nodes = True
                        if not eff_port:
                            eff_port = p
                        break
            info = {"vip": vip, "vip_port": eff_port if eff_port else vip_port,
                    "online": online, "via_nodes": via_nodes, "nodes": []}
            # 每节点端口可达性（管理器→节点 MySQL 端口 TCP 探活）
            for nr in nrows:
                try:
                    p = int(nr["port"])
                except (TypeError, ValueError):
                    p = 0
                info["nodes"].append({
                    "host": nr["ip"], "port": p,
                    "node_type": nr.get("node_type") or "mariadb-docker",
                    "reachable": _port_online(nr["ip"], p) if p else False,
                })
            return cid, info

        if rows:
            with ThreadPoolExecutor(max_workers=min(len(rows), 8)) as ex:
                for fut in as_completed([ex.submit(probe, r) for r in rows]):
                    cid, info = fut.result()
                    result[cid] = info
        return _json_ok({"ok": True, "vips": result})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


# ---------------- v2.1：预检 / 历史趋势 / 慢日志 / 配置漂移 / 审计 ----------------

@app.post("/api/fix_node")
def api_fix_node(cluster_id: int = Form(...), host: str = Form(...), port: int = Form(0),
                 user=Depends(auth_required)):
    """一键自愈单节点复制参数（gtid_mode/enforce_gtid_consistency/server_id）。

    通过 SET PERSIST 写入 mysqld-auto.cnf，重启后仍生效，彻底解决裸机 my.cnf 片段
    被面板/重启覆盖导致的反复 gtid_mode=OFF、server_id 默认 1 问题。仅在线修改动态参数。
    """
    try:
        cid = int(cluster_id)
        port = int(port)
        ru, rp = node_root(host, cid)
        row = query("SELECT server_id FROM nodes WHERE cluster_id=%s AND ip=%s AND port=%s",
                    (cid, host, port))
        sid = int(row[0]["server_id"]) if row and row[0].get("server_id") else None
        ok, rep = fix_node_mysql_params(host, port, ru, rp, target_server_id=sid)
        return _json_ok({"ok": ok, **rep})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/precheck")
def api_precheck(cluster_id: int = Form(...), host: str = Form(""), port: int = Form(0),
                 user=Depends(auth_required)):
    """节点/集群预检（仅告警）。可用 host/port 指定单节点，否则整集群。"""
    try:
        if host and port:
            creds = get_cluster_creds(cluster_id)
            role = None
            rr = query("SELECT role FROM hosts WHERE cluster_id=%s AND host=%s AND port=%s",
                       (cluster_id, host, int(port)))
            if rr:
                role = rr[0].get("role")
            findings = precheck.check_node(host, int(port), creds.get("ha_user"),
                                           creds.get("ha_pass"), role)
            return _json_ok({"ok": True, "findings": [{"host": host, "port": int(port), **f}
                                                       for f in findings]})
        findings = precheck.check_cluster(cluster_id)
        return _json_ok({"ok": True, "findings": findings})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/metric_history")
def api_metric_history(cluster_id: int = 0, host: str = "", port: int = 0, hours: int = 24,
                       user=Depends(auth_required)):
    """指标历史快照（趋势图数据源）。"""
    try:
        rows = metrics.get_metric_history(int(cluster_id), host or None, port or None, int(hours))
        return _json_ok({"ok": True, "rows": rows})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


def _slow_log_hint(cid):
    """慢日志为空时的诊断提示（v2.3.5）。

    三种形态共有的空结果原因只有三类：采集开关没开、节点没开慢日志、
    或开了但没写表（log_output 不含 TABLE，采集读的是 mysql.slow_log）。
    """
    parts = []
    try:
        if not slow_log.slowlog_enabled():
            parts.append("采集开关 SLOWLOG_ENABLED 未开启（系统设置 → 慢日志采集）")
    except Exception:
        pass
    try:
        from .ha_client import connect_robust
        from .clusters import get_cluster_creds
        creds = get_cluster_creds(cid) or {}
        cm = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cid,))
        if cm and creds.get("ha_pass"):
            conn = connect_robust(cm[0]["host"], int(cm[0]["port"]),
                                  creds.get("ha_user") or "", creds.get("ha_pass") or "",
                                  timeout=3)
            if conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT @@slow_query_log, @@log_output, @@long_query_time")
                        r = cur.fetchone() or ("", "", "")
                        sq, lo, lqt = (str(r[0] or ""), str(r[1] or ""), str(r[2] or ""))
                        if sq.upper() not in ("1", "ON", "YES"):
                            parts.append("节点 slow_query_log=%s（未开启慢日志）" % (sq or "OFF"))
                        if "TABLE" not in lo.upper():
                            parts.append("节点 log_output=%s（不含 TABLE，慢日志没写进 mysql.slow_log，采集读不到）"
                                         % (lo or "空"))
                        parts.append("long_query_time=%ss" % (lqt or "?"))
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
    except Exception:
        pass
    return "；".join(parts) if parts else "暂无慢日志（也可能是本时段确无超阈值查询）"


@app.get("/api/slow_log")
def api_slow_log(cluster_id: int = 0, host: str = "", port: int = 0, limit: int = 200,
                 min_ms: int = 0, user=Depends(auth_required)):
    """慢日志明细（按耗时倒序）。

    v2.3.5：为空时附带「为什么空」的诊断（慢日志开关 / log_output / 采集开关），
    避免三形态下都只看到一句「暂无慢日志」而无从下手。
    """
    try:
        rows = slow_log.get_slow_log(int(cluster_id), host or None, port or None, int(limit),
                                     int(min_ms) if min_ms else None)
        out = {"ok": True, "rows": rows}
        if not rows:
            out["hint"] = _slow_log_hint(int(cluster_id))
        return _json_ok(out)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/config_drift")
def api_config_drift(cluster_id: int = 0, user=Depends(auth_required)):
    """最新一次配置一致性巡检结果。"""
    try:
        rows, drift = config_drift.get_latest_drift(int(cluster_id))
        return _json_ok({"ok": True, "rows": rows, "drift": drift})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/config_drift/remediate")
def api_config_drift_remediate(cluster_id: int = Form(0), user=Depends(auth_required)):
    """一键修复最近一次巡检的 DRIFT 项（在线 SET PERSIST，仅可在线修复项；其余标 manual）。"""
    try:
        res = config_drift.remediate_latest(int(cluster_id))
        # 修复改变了在线节点状态，重跑一次巡检刷新 config_drift 表，
        # 让前端「重新加载」后能看到真实结果（已修复项会变为 OK，未达标项仍 DRIFT）。
        try:
            config_drift.check_cluster(int(cluster_id))
        except Exception:
            pass  # 巡检刷新失败不影响已下发的修复结果
        return _json_ok(res)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/os_metrics")
def api_os_metrics(cluster_id: int = 0, user=Depends(auth_required)):
    """拉取指定集群各节点的 OS 指标（来自节点 agent /stats）。agent 不可达时 ok=False。"""
    try:
        nodes = os_metrics.get_cluster_os_metrics(int(cluster_id))
        return _json_ok({"ok": True, "cluster_id": cluster_id, "nodes": nodes})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/audit")
def api_audit(limit: int = 200, cluster_id: int = 0, user=Depends(auth_required)):
    """操作审计日志列表。"""
    try:
        rows = audit.list_audit(int(limit), 0, int(cluster_id) if cluster_id else None)
        return _json_ok({"ok": True, "rows": rows})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


# 后台采集锁：保证同一时刻只有一次手动采集在进行，避免并发打满节点连接。
_collect_lock = threading.Lock()
_collect_state = {"active": False, "error": None, "last": None}


@app.post("/api/collect")
def api_collect(target: str = Form("all"), cluster_id: int = Form(0), user=Depends(auth_required)):
    """手动触发采集：metrics / slow / drift / all。

    改为后台线程非阻塞执行：请求立即返回 accepted，采集在守护线程中跑。
    原因：原实现在单 worker 内同步执行 collect_metrics/collect_slow_log/check_cluster，
    节点不可达时 connect_robust 超时叠加，会长时间阻塞唯一 worker，请求连接被上游/
    浏览器重置，前端表现为 fetch 的 NetworkError。后台化后彻底消除该失败模式。
    """
    def _run():
        with _collect_lock:
            _collect_state["active"] = True
            _collect_state["error"] = None
            try:
                done = []
                cid = int(cluster_id) if cluster_id else None
                if target in ("all", "metrics"):
                    metrics.collect_metrics(cid)
                    done.append("metrics")
                if target in ("all", "slow"):
                    slow_log.collect_slow_log(cid)
                    done.append("slow")
                if target in ("all", "drift"):
                    if cid:
                        config_drift.check_cluster(cid)
                    else:
                        for c in list_clusters():
                            config_drift.check_cluster(c["id"])
                    done.append("drift")
                _collect_state["last"] = done
            except Exception as e:
                _collect_state["error"] = f"{type(e).__name__}: {e}"
                print(f"[collect] 后台采集异常: {e}", flush=True)
                import traceback
                traceback.print_exc()
            finally:
                _collect_state["active"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return _json_ok({"ok": True, "accepted": True, "msg": "采集任务已提交，正在后台执行…"})


@app.get("/api/collect/state")
def api_collect_state(user=Depends(auth_required)):
    """查询后台采集任务状态（供前端轮询，给出真实成败而非盲目「已提交」）。"""
    return _json_ok({"ok": True,
                     "active": bool(_collect_state.get("active")),
                     "error": _collect_state.get("error"),
                     "last": _collect_state.get("last")})


@app.post("/api/consistency/check")
def api_consistency_check(cluster_id: int = Form(...), mode: str = Form("rows"),
                          wait_sec: int = Form(-1),
                          user=Depends(auth_required)):
    """主从数据一致性校验（纯只读）。

    mode=rows     比对各业务表行数（轻量，可定期跑）
    mode=checksum CHECKSUM TABLE 逐表比对（较重，能发现行数相同但内容不同）

    wait_sec：候选差异复核前，等待从库追平的秒数上限（-1 = 用模块默认）。
    结论会持久化（consistency.save_result）——集群页据此显示最近一次结论徽章，
    使「有未处置差异」与「页面显示正常」不可能同时成立。
    """
    try:
        res = consistency.check_consistency(
            cluster_id, mode, wait_sec=(None if int(wait_sec) < 0 else int(wait_sec)))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    if res.get("ok"):
        res["result_id"] = consistency.save_result(cluster_id, res)
    return _json_ok(res)


@app.get("/api/consistency/last")
def api_consistency_last(cluster_id: int = 0, user=Depends(auth_required)):
    """最近一次一致性校验结论（集群页徽章/横幅用；从未校验过返回 has=False）。

    为什么必须持久化：校验结论原本只活在一个弹窗里，刷新即丢 —— 于是「刚校验出 2 处差异」
    与「页面显示复制正常」可以同时成立，用户无从判断哪个库可信。
    """
    try:
        res = consistency.last_result(int(cluster_id))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    return _json_ok(res)


@app.get("/api/consistency/check")
def api_consistency_check_get(cluster_id: int = 0, mode: str = "rows",
                              user=Depends(auth_required)):
    """GET 形式的一致性校验（便于前端/脚本直接调用）。结论同样持久化。"""
    try:
        res = consistency.check_consistency(cluster_id, mode)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    if res.get("ok"):
        res["result_id"] = consistency.save_result(cluster_id, res)
    return _json_ok(res)



@app.post("/api/consistency/row_diff")
def api_consistency_row_diff(cluster_id: int = Form(...), host: str = Form(...),
                            port: int = Form(...), table: str = Form(...),
                            user=Depends(auth_required)):
    """展开某张差异表的行级差异（主键 + 两侧值）。table 形如 'db.tbl'。"""
    try:
        if "." in table:
            schema, tbl = table.split(".", 1)
        else:
            return _json_ok({"ok": False, "error": "表名需含库名（db.tbl）"})
        res = consistency.row_diff(cluster_id, host, port, schema, tbl)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    return _json_ok(res)


@app.post("/api/consistency/export")
def api_consistency_export(cluster_id: int = Form(...), host: str = Form(...),
                          port: int = Form(...), table: str = Form(...),
                          user=Depends(auth_required)):
    """生成差异处置 SQL 并落 quarantine（卷内回收表，可一键回灌主库）。"""
    try:
        if "." in table:
            schema, tbl = table.split(".", 1)
        else:
            return _json_ok({"ok": False, "error": "表名需含库名（db.tbl）"})
        res = consistency.export_divergent_rows(cluster_id, host, port, schema, tbl)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    return _json_ok(res)


@app.post("/api/consistency/remaster")
def api_consistency_remaster(recycle_id: int = Form(...), user=Depends(auth_required)):
    """把回收表中的从库独有行回灌到当前主库（救回分裂窗口内的真实写入）。"""
    try:
        res = consistency.remaster_from_recycle(recycle_id)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    return _json_ok(res)


@app.post("/api/node/rebuild")
def api_node_rebuild(cluster_id: int = Form(...), host: str = Form(...),
                     port: int = Form(...), user=Depends(auth_required)):
    """从库在线重建（异步）：主库 dump → 从库 restore（关 binlog）→ GTID 重链。

    用于复制出现真实数据冲突（1062/1032 且自愈无法修复）时，替代「停机手工重建」。
    严禁对当前主库执行 —— 那等于用主库数据覆盖主库本身。
    """
    try:
        cur_h, cur_p = get_current_master(cluster_id)
        if cur_h == host and int(cur_p or 0) == int(port):
            return _json_ok({"ok": False, "error": "不能重建当前主库（会抹掉整个集群数据）"})
        task = rebuild.start_rebuild(cluster_id, host, port, actor=str(user))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    # 节点重建会抹掉数据：若它曾是迁移目标，残留的迁移锁/running 任务会让仪表盘
    # 永久显示「迁移中」并让 failover 跳过该节点拓扑维护 —— 顺手清掉（失败不影响重建）。
    try:
        _migration.force_clear_node(host, port, clear_tasks=True)
    except Exception:
        pass
    return _json_ok({"ok": True, "task": task})


@app.get("/api/node/rebuild_status")
def api_rebuild_status(task: str = "", user=Depends(auth_required)):
    """查询重建任务进度（status/pct/msg）。"""
    return _json_ok(rebuild.get_task(task))


# ==================================================================
# 节点引导安装（v2.5.0 F1）：一行 curl 完成部署
#
# 下面三个「下载类」端点**刻意不要求登录**——节点上是 curl 在跑，没有登录态。
# 安全性完全由一次性 token 保证：绑定节点 IP + 短 TTL + 部署完成即失效。
# 这与「下载自愈 SQL」是同一类端点：URL 本身就是凭据，必须短命。
# ==================================================================
@app.post("/api/node/bootstrap")
def api_node_bootstrap(ip: str = Form(...), ttl_hours: int = Form(24),
                       install_dir: str = Form(""),
                       user=Depends(auth_required)):
    """生成节点引导命令（含一次性 token）。

    ttl_hours 默认 24 小时：运维拿到命令后可能隔天才上机，太短会白跑一趟；
    安全性由「绑定 IP + 完成后作废」兜底，而不是靠把有效期压到几分钟。
    install_dir：可选，指定节点在目标机上的安装目录（覆盖默认 base/统一名 custos-node）；
    必须是绝对路径，否则忽略回退默认。
    """
    from . import bootstrap
    from .config import NODE_INSTALL_BASE, NODE_DIR_NAME
    ip = (ip or "").strip()
    if not ip:
        return _json_ok({"ok": False, "error": "节点 IP 不能为空"})
    install_dir = (install_dir or "").strip()
    if install_dir and not install_dir.startswith("/"):
        install_dir = ""          # 仅接受绝对路径，避免落到不可预期的相对目录
    try:
        ttl = int(ttl_hours)
    except (TypeError, ValueError):
        ttl = 24
    ttl = max(1, min(ttl, 24 * 7))
    try:
        rows = query("SELECT node_type, cluster_id FROM nodes WHERE ip=%s", (ip,))
        node_type = (rows[0].get("node_type") if rows else "") or "mariadb-docker"
        cluster_id = (rows[0].get("cluster_id") if rows else None)
    except Exception:
        node_type, cluster_id = "docker", None
    try:
        info = bootstrap.issue_for_node(ip, node_type=node_type, cluster_id=cluster_id,
                                        actor=str(user), ttl_hours=ttl,
                                        install_dir=install_dir or None)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    try:
        audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                        "BOOTSTRAP", ip, f"签发节点引导命令（有效期 {ttl}h）", cluster_id)
    except Exception:
        pass
    return _json_ok({"ok": True, **info})


@app.get("/api/node/bootstrap/{token}")
def api_bootstrap_script(token: str, request: Request):
    """引导安装脚本本体（节点 `curl … | bash` 拉的就是它）。

    不要求登录；token 无效/过期/已用一律 403，且不透露具体原因
    （避免把「该 IP 有有效 token」这类信息泄露给探测者）。
    """
    from . import bootstrap
    row = bootstrap.verify_token(token)
    if not row:
        return Response("引导命令无效或已过期，请在管理端重新生成。",
                        status_code=403, media_type="text/plain; charset=utf-8")
    ip = row["ip"]
    try:
        rows = query("SELECT node_type, compose_path, mysql_version FROM nodes WHERE ip=%s", (ip,))
        node_type = (rows[0].get("node_type") if rows else "") or "mariadb-docker"
        node_dir_val = (rows[0].get("compose_path") if rows else "") or ""
        # 版本跟随：引导脚本里的安装脚本路径必须按节点版本取目录名（mariadb11.4.13/...），
        # 否则 11.4/10.6 裸机节点引导后根本找不到 Install 脚本。
        _node_ver = (rows[0].get("mysql_version") if rows else "") or ""
    except Exception:
        node_type = "docker"
        node_dir_val = ""
    # 安装目录来源（优先级）：签发引导命令时填的 install_dir（随 token 落库）
    # → 添加节点时填的「节点安装目录」（nodes.compose_path）→ 默认 base/统一名。
    # 此前这里根本没传 install_dir，导致运维填的自定义目录无效、实际仍装到默认目录。
    _dir = (row.get("install_dir") or "").strip() or (node_dir_val or "").strip()
    if _dir and not _dir.startswith("/"):
        _dir = ""                      # 仅接受绝对路径，脏数据回退默认
    try:
        # sidecar_only 必须随 token 读回：接管场景只装 keepalived + agent，
        # 不传的话会回落成全量安装（跑 Install 脚本）并被旧库自身端口卡死。
        _sidecar = bool(int(row.get("sidecar_only") or 0))
        script = bootstrap.render_bootstrap_script(ip, token, node_type,
                                                   install_dir=_dir or None,
                                                   sidecar_only=_sidecar,
                                                   mysql_version=_node_ver or None)
    except Exception as e:
        return Response(f"生成引导脚本失败：{e}", status_code=500,
                        media_type="text/plain; charset=utf-8")
    return Response(script, media_type="text/plain; charset=utf-8",
                    headers={"Content-Disposition": f'inline; filename="custos-node-{ip}.sh"'})


@app.get("/api/node/bundle/{token}")
def api_bootstrap_bundle(token: str, request: Request):
    """节点部署单元包（tar.gz），含集群凭据——故必须凭 token 下载。"""
    from . import bootstrap
    row = bootstrap.verify_token(token)
    if not row:
        return Response("引导命令无效或已过期，请在管理端重新生成。",
                        status_code=403, media_type="text/plain; charset=utf-8")
    try:
        data = bootstrap.build_bundle(row["ip"])
    except Exception as e:
        # 打包失败必须明确报出（少一个 ha_check.conf，节点上 keepalived 就起不来，
        # 而这种缺失在管理端完全看不出来，只会在真正切换时才发现 VIP 漂不动）。
        return Response(f"打包节点单元失败：{e}", status_code=500,
                        media_type="text/plain; charset=utf-8")
    return Response(data, media_type="application/gzip",
                    headers={"Content-Disposition":
                             f'attachment; filename="custos-node-{row["ip"]}.tar.gz"'})


@app.get("/api/node/image/{name}")
def api_node_image(name: str, token: str = ""):
    """节点镜像 tar（v2.5.0 F2）：引导脚本下载后 `docker load`。

    解决内网节点拉不到 `custos-node:<ver>`（本地构建镜像，公网没有）
    与 `python:3-slim`（公共镜像，离线拉不到）的问题。
    """
    from . import bootstrap
    import os
    from .config import IMAGES_DIR
    # 防路径穿越：只取文件名，且必须落在 IMAGES_DIR 内
    safe = os.path.basename((name or "").strip())
    if not safe.endswith(".tar"):
        safe += ".tar"
    row = bootstrap.verify_token(token)
    if not row:
        return Response("引导命令无效或已过期，请在管理端重新生成。",
                        status_code=403, media_type="text/plain; charset=utf-8")
    path = os.path.join(IMAGES_DIR, safe)
    if not os.path.isfile(path):
        # 404 分层诊断：「容器内目录不存在＝缺 ./images 挂载」与「目录在但无该
        # tar＝未构建」是两种完全不同的修法，必须在报错里区分开。
        if not os.path.isdir(IMAGES_DIR):
            hint = ("服务端未托管该镜像包：%s。管理器容器内 %s 目录不存在——"
                    "docker-compose.yml 的 ./images 挂载未生效，请重建管理器容器："
                    "./scripts/deploy.sh up -d --force-recreate custos-server"
                    % (safe, IMAGES_DIR))
        else:
            try:
                _have = ", ".join(sorted(os.listdir(IMAGES_DIR))) or "（目录为空）"
            except OSError:
                _have = "（目录存在但不可读，请检查挂载目录权限）"
            hint = ("服务端未托管该镜像包：%s（%s 内现有：%s）。"
                    "请在服务端宿主机执行 scripts/build_node_images.sh 生成"
                    % (safe, IMAGES_DIR, _have))
        return Response(hint, status_code=404, media_type="text/plain; charset=utf-8")
    # 流式下发（FileResponse）：镜像包可能上百 MB，read() 整读进内存会在
    # 多节点同时引导时挤爆管理器容器内存；FileResponse 边读边发并自动带
    # Content-Length / Content-Disposition(attachment)。
    return FileResponse(path, media_type="application/x-tar", filename=safe)


@app.post("/api/nodes/{ip}/ready")
def api_node_ready(ip: str, request: Request, token: str = Form(""),
                   status: str = Form("online"), error: str = Form("")):
    """节点部署回执（引导脚本最后一步调用，F3）。

    回执成功即**作废 token**——凭据已完成使命，不该继续可用。
    这也是 deploy_state 由 registered → online 的唯一入口。
    """
    from . import bootstrap
    src_ip = ""
    try:
        src_ip = request.client.host if request.client else ""
    except Exception:
        pass
    row = bootstrap.verify_token(token, ip=ip)
    if not row:
        return _json_ok({"ok": False, "error": "token 无效、已过期或 IP 不匹配"})
    state = "online" if str(status).strip().lower() in ("online", "ok", "success", "") else "failed"
    try:
        execute("UPDATE nodes SET deploy_state=%s, last_ready_at=NOW(), deploy_error=%s "
                "WHERE ip=%s", (state, (error or "")[:250], ip))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"回执写入失败：{e}"})
    bootstrap.consume_token(token, src_ip=src_ip)
    # 节点接入自检（best-effort，不阻断就绪）：校验关键特权，漂移可见
    try:
        from .selfcheck import selfcheck_node
        r = query("SELECT cluster_id, port FROM nodes WHERE ip=%s", (ip,))
        if r:
            cid = r[0].get("cluster_id")
            nport = int(r[0].get("port") or 3306)
            creds = load_creds(cid) if cid else {}
            ok, issues = selfcheck_node(ip, nport, creds.get("ha_user"), creds.get("ha_pass"))
            for level, msg in issues:
                print("[node-ready][selfcheck][%s] %s" % (level, msg), flush=True)
            try:
                upsert_setting("node_selfcheck_%s" % ip,
                               json.dumps([list(x) for x in issues], ensure_ascii=False)[:500])
            except Exception:
                pass
    except Exception as e:
        print("[node-ready] 接入自检异常(可忽略): %s" % e, flush=True)
    return _json_ok({"ok": True, "deploy_state": state})


# ==================================================================
# agent 心跳与指令通道（v2.5.0 F5）
#
# 心跳方向是 agent → manager，因此**不要求登录**，凭节点独立 token 认证。
# 指令只允许白名单动词（agent 侧二次校验），通用 exec 默认关闭。
# ==================================================================
@app.post("/api/agent/heartbeat")
def api_agent_heartbeat(ip: str = Form(""), token: str = Form(""),
                        agent_version: str = Form(""), facts: str = Form("")):
    """agent 心跳：上报存活并取回待执行指令。

    为什么不用登录态：节点上跑的是 daemon，没有浏览器会话。
    安全性由「每节点独立 token + 白名单动词」保证。
    """
    from . import agent_hub
    ip = (ip or "").strip()
    if not ip:
        return _json_ok({"ok": False, "error": "缺少 ip"})
    if not agent_hub.verify_agent(ip, token):
        # 不区分「token 错」还是「节点不存在」，避免泄露拓扑
        return _json_ok({"ok": False, "error": "认证失败"})
    try:
        facts_d = json.loads(facts) if facts else {}
    except Exception:
        facts_d = {}
    res = agent_hub.on_heartbeat(ip, {"version": agent_version, "facts": facts_d})
    # 心跳顺带把 agent 版本记下来（排障时一眼看出哪些节点还是旧 agent）
    if agent_version:
        try:
            upsert_setting("agent_version_%s" % ip, str(agent_version)[:32])
        except Exception:
            pass
    # 记录「最近一次收到该节点心跳」的本地时间戳（v2.5.0 F11 无保护窗口提示用）
    try:
        upsert_setting("agent_last_seen_%s" % ip, str(int(_time.time())))
    except Exception:
        pass
    return _json_ok(res)


@app.post("/api/agent/cmd_result")
def api_agent_cmd_result(ip: str = Form(""), token: str = Form(""),
                         id: int = Form(0), ok: str = Form("0"),
                         message: str = Form("")):
    """agent 回报指令执行结果。"""
    from . import agent_hub
    ip = (ip or "").strip()
    if not ip or not agent_hub.verify_agent(ip, token):
        return _json_ok({"ok": False, "error": "认证失败"})
    return _json_ok(agent_hub.on_result(ip, int(id), str(ok) in ("1", "true", "True"),
                                        message))


@app.post("/api/agent/commands")
def api_agent_enqueue(ip: str = Form(...), verb: str = Form(...),
                      payload: str = Form("{}"), ttl_sec: int = Form(3600),
                      dedupe_key: str = Form(""), user=Depends(auth_required)):
    """下发一条指令给节点（只入队，由心跳拉取执行）。

    verb 必须在白名单内，否则直接拒绝——agent 有 root + docker.sock，
    绝不允许服务端下发任意命令。
    """
    from . import agent_hub
    try:
        pl = json.loads(payload) if payload else {}
    except Exception as e:
        return _json_ok({"ok": False, "error": "payload 不是合法 JSON：%s" % e})
    try:
        cid = agent_hub.enqueue(ip, verb, pl,
                                actor=(user.get("username") if isinstance(user, dict) else str(user)),
                                ttl_sec=ttl_sec, dedupe_key=dedupe_key)
    except Exception as e:
        return _json_ok({"ok": False, "error": str(e)})
    return _json_ok({"ok": True, "command_id": cid})


@app.get("/api/agent/commands")
def api_agent_list(ip: str = "", limit: int = 30, user=Depends(auth_required)):
    """查询指令下发与执行情况（排障用）。"""
    from . import agent_hub
    return _json_ok({"ok": True, "commands": agent_hub.list_commands(ip or None, limit),
                     "pending": agent_hub.pending_count(ip or None)})


# ==================================================================
# 无保护窗口提示（v2.5.0 F11）
#
# 管理器停机 = 集群进入「无保护窗口」：自动切换 / 复制自愈 / 告警 / 备份全部失效。
# 本端点用于在管理器恢复后，根据各节点「最近一次心跳」时间戳，算出管理器曾离线多久，
# 并在仪表盘就此发出醒目警告，避免运维误以为保护仍在生效。
# ==================================================================
APP_STARTED_AT = _time.time()


@app.get("/api/system/protection")
def api_system_protection(user=Depends(auth_required)):
    """返回各节点最近心跳时间与管理器离线时长评估。"""
    now = int(_time.time())
    rows = query("SELECT ip, deploy_state FROM nodes ORDER BY ip") or []
    gaps = []
    for r in rows:
        ip = r["ip"]
        try:
            lst = get_setting("agent_last_seen_%s" % ip)
        except Exception:
            lst = None
        seen = int(lst) if lst and str(lst).isdigit() else 0
        ago = (now - seen) if seen else None
        gaps.append({"ip": ip, "deploy_state": r.get("deploy_state"), "last_seen_sec_ago": ago})
    # 「无保护窗口」判定：管理器本次已连续运行足够久（>2min），但某节点的最近心跳
    # 仍停在更早之前（>2min），说明管理器确实离线过一段时间。
    uptime = int(now - APP_STARTED_AT)
    offline_nodes = [g for g in gaps
                     if g["last_seen_sec_ago"] is not None
                     and g["last_seen_sec_ago"] > 120
                     and uptime > 120]
    max_gap = max([g["last_seen_sec_ago"] for g in offline_nodes], default=0)
    return _json_ok({
        "ok": True,
        "manager_uptime_sec": uptime,
        "manager_was_offline": bool(offline_nodes),
        "offline_sec": max_gap,
        "offline_nodes": offline_nodes,
        "nodes": gaps,
    })


# ==================================================================
# 强制同步 / 切主就绪判定（v2.5.0 F7）
#
# 长任务**绝不阻塞请求**：立即返回 task_id，前端 1.5s 轮询 /api/sync/status。
# 首帧必须已有阶段文字与进度，否则用户会以为卡死。
# ==================================================================
@app.post("/api/sync/force")
def api_sync_force(cluster_id: int = Form(...), timeout_sec: int = Form(120),
                   grace_sec: int = Form(300), user=Depends(auth_required)):
    """强制同步：停写 → 等从库追平 → 报告「可以安全切主」。

    超时/取消/宽限期到都会**自动恢复主库写入**，绝不让业务长期停写。
    """
    from . import syncgate
    try:
        r = syncgate.start_forced_sync(
            int(cluster_id), int(timeout_sec), int(grace_sec),
            actor=(user.get("username") if isinstance(user, dict) else str(user)))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    return _json_ok({"ok": True, **r})


@app.get("/api/sync/status")
def api_sync_status(task: str = "", user=Depends(auth_required)):
    """查询强制同步进度（status/stage/progress/ready/locked）。"""
    from . import syncgate
    return _json_ok(syncgate.get_task(task))


@app.get("/api/sync/readiness")
def api_sync_readiness(cluster_id: int = 0, user=Depends(auth_required)):
    """只读判定：现在能不能安全切主（不动任何状态）。"""
    from . import syncgate
    try:
        return _json_ok(syncgate.readiness(int(cluster_id)))
    except Exception as e:
        return _json_ok({"ok": False, "ready": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/sync/cancel")
def api_sync_cancel(task: str = Form(...), user=Depends(auth_required)):
    """取消强制同步：立即恢复主库写入。"""
    from . import syncgate
    return _json_ok(syncgate.cancel(task))


# ==================================================================
# 免 SSH 改密（v2.5.0 F6）
#
# 允许**只改单个账号**（UI 勾选哪个改哪个）；失败自动回滚，绝不留下半套新密码。
# ==================================================================
@app.post("/api/passwords/change")
def api_passwords_change(cluster_id: int = Form(...),
                         ha_pass: str = Form(""), repl_pass: str = Form(""),
                         app_pass: str = Form(""), root_pass: str = Form(""),
                         sync_files: int = Form(1),
                         user=Depends(auth_required)):
    """修改集群账号密码（可只改其中几个，留空=不改）。

    流程：主库 ALTER USER → 更新凭证记录 → 等复制下发 → 同步节点凭据文件
          → 重启 keepalived。任一环节失败即回滚。
    """
    from . import passmgr
    changes = {}
    if ha_pass:
        changes[passmgr.ACCT_HA] = ha_pass
    if repl_pass:
        changes[passmgr.ACCT_REPL] = repl_pass
    if app_pass:
        changes[passmgr.ACCT_APP] = app_pass
    if root_pass:
        changes[passmgr.ACCT_ROOT] = root_pass
    if not changes:
        return _json_ok({"ok": False, "error": "请至少填写一个要修改的账号密码"})
    try:
        res = passmgr.change(int(cluster_id), changes,
                             actor=(user.get("username") if isinstance(user, dict) else str(user)),
                             sync_node_files=bool(int(sync_files)))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    try:
        audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                        "change_password", f"cluster={cluster_id}",
                        "修改账号：" + "、".join(sorted(changes.keys())), cluster_id=int(cluster_id))
    except Exception:
        pass
    return _json_ok(res)


# ==================================================================
# 定时备份策略（集群级，v2.5.0 F9）
#
# 它现在是「节点纯净重建」的数据源前提：没有定期备份，重建就只能走全量导出。
# ==================================================================
@app.get("/api/backup/policy")
def api_backup_policy_get(cluster_id: int = 0, user=Depends(auth_required)):
    """读取集群定时备份策略 + 下次执行时间 + 当前占用（页面展示用）。"""
    from . import backup_policy
    cid = int(cluster_id)
    pol = backup_policy.get_policy(cid)
    ok_due, overdue = backup_policy.due(cid, pol.get("interval_hours"))
    return _json_ok({
        "ok": True, "policy": pol,
        "last_run": backup_policy.last_run(cid),
        "due": ok_due,
        "next_run_at": backup_policy.next_run_at(cid, pol.get("interval_hours")),
        "overdue_sec": overdue,
        "usage_bytes": backup_policy.usage_bytes(cid),
    })


@app.post("/api/backup/policy")
def api_backup_policy_set(cluster_id: int = Form(...), enabled: bool = Form(False),
                          interval_hours: int = Form(24), retention_days: int = Form(7),
                          retention_count: int = Form(7), target: str = Form("slave"),
                          with_data: bool = Form(True), wait_catch_up: bool = Form(True),
                          disk_alert_gb: int = Form(0),
                          user=Depends(auth_required)):
    """设置集群定时备份策略（未配置的字段保持原值）。

    enabled/with_data/wait_catch_up 用 bool 声明：前端 checkbox 经
    URLSearchParams 会把 True 编码成字符串 "true"，若按 int 解析会 422，
    导致用户在页面勾选保存策略必失败（功能不可用）。
    """
    from . import backup_policy
    pol = backup_policy.set_policy(
        int(cluster_id), enabled=bool(enabled),
        interval_hours=int(interval_hours), retention_days=int(retention_days),
        retention_count=int(retention_count), target=(target or "slave").lower(),
        with_data=bool(with_data), wait_catch_up=bool(wait_catch_up),
        disk_alert_gb=int(disk_alert_gb))
    return _json_ok({"ok": True, "policy": pol})


# ==================================================================
# 集群接管（v2.5.0 F10）
#
# 接管 = **纳管**，不是重建：只读探测 → 建账号 → 登记 → 用引导命令装 sidecar。
# 全程不碰数据、不重装数据库。
# ==================================================================
@app.post("/api/adopt/probe")
def api_adopt_probe(nodes: str = Form(...), l2_prefix: int = Form(24),
                    user=Depends(auth_required)):
    """只读探测一组存量节点（幂等，可反复跑，绝不写入老库）。

    nodes: JSON 数组字符串
        [{"host":"192.0.2.50","port":3306,"user":"root","password":"pwd"}, ...]
    """
    from . import adopt
    try:
        arr = json.loads(nodes)
        if not isinstance(arr, list) or not arr:
            raise ValueError("nodes 必须是非空 JSON 数组")
    except Exception as e:
        return _json_ok({"ok": False, "error": "nodes 解析失败：%s" % e})
    try:
        return _json_ok(adopt.probe_cluster(arr, l2_prefix=int(l2_prefix)))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/adopt/fix_switch")
def api_adopt_fix_switch(host: str = Form(...), port: int = Form(3306),
                         db_user: str = Form("root"), db_pass: str = Form(""),
                         server_id: int = Form(0),
                         user=Depends(auth_required)):
    """接管向导「快速修复」：在线设 server_id；log_bin 只能给命令由人工重启。

    用**探测时填的那套账号**（第 1 步节点表），它已连通且权限已探明；
    第 3 步的 root 口令只用于建 Custos 账号，不在这里用。
    """
    from . import adopt
    try:
        return _json_ok(adopt.apply_switch_fix(
            host, int(port), db_user, db_pass, int(server_id)))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/adopt/test_root")
def api_adopt_test_root(host: str = Form(...), port: int = Form(3306),
                        db_user: str = Form("root"), db_pass: str = Form(""),
                        user=Depends(auth_required)):
    """验证某台节点填的数据库 root 口令：连不连得上、权限够不够（只读，不建账号）。

    接管要用这套口令在该节点上建 Custos 账号，先测通再执行，避免带病接管。
    """
    from . import adopt
    try:
        return _json_ok(adopt.test_root_credential(host, int(port), db_user, db_pass))
    except Exception as e:
        return _json_ok({"ok": False, "msg": f"{type(e).__name__}: {e}"})


@app.post("/api/adopt/fix_cmd")
def api_adopt_fix_cmd(host: str = Form(...), port: int = Form(3306),
                      db_user: str = Form("root"), db_pass: str = Form(""),
                      server_id: int = Form(0), container: str = Form(""),
                      user=Depends(auth_required)):
    """按形态（裸机 / 容器）重新生成修复命令。**只读**，不改任何东西。

    container 填容器名（Docker / 1Panel 面板装的都算）→ 生成「进容器执行」的命令；
    留空 → 生成裸机那条（宿主机配置目录 + systemctl restart）。
    """
    from . import adopt
    try:
        return _json_ok(adopt.fix_command_for(
            host, int(port), db_user, db_pass,
            server_id=(int(server_id) or None),
            container=(container or "").strip()))
    except Exception as e:
        return _json_ok({"ok": False, "msg": f"{type(e).__name__}: {e}"})


@app.post("/api/adopt/check_vip")
def api_adopt_check_vip(vip: str = Form(""), cluster_id: int = Form(0),
                        user=Depends(auth_required)):
    """VIP 占用检测（第 1、2 层）。

    ① 登记比对：与已登记节点 IP / 已有集群 VIP 冲突 → 硬阻断；
    ② 管理器 ping：有响应 → 警告（禁 ICMP 的机器 ping 不通，故"不通"不等于空闲）；
    ③ 节点本机 arping（最准）由引导脚本在装 keepalived 之前执行，此处不做
       —— 管理器通常不在该二层网络，arping 结果不可信。
    """
    v = (vip or "").strip()
    if not v:
        return _json_ok({"ok": False, "level": "block", "msg": "请先填写 VIP"})
    try:
        for r in (query("SELECT ip FROM nodes") or []):
            if str(r.get("ip") or "").strip() == v:
                return _json_ok({"ok": True, "level": "block",
                                 "msg": "该 IP 已被登记为节点，不能作为 VIP"})
        for r in (query("SELECT id, vip FROM clusters") or []):
            if str(r.get("vip") or "").strip() != v:
                continue
            if int(cluster_id or 0) and int(r.get("id") or 0) == int(cluster_id):
                continue
            return _json_ok({"ok": True, "level": "block",
                             "msg": "该 IP 已被集群 %s 用作 VIP" % r.get("id")})
    except Exception:
        pass
    try:
        import subprocess
        rc = subprocess.run(["ping", "-c", "1", "-W", "2", v],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            timeout=6)
        if rc.returncode == 0:
            return _json_ok({"ok": True, "level": "warn",
                             "msg": "该 IP 有响应（可能已被占用，请确认）；"
                                    "最终以节点本机 arping 为准（冲突将中止安装）"})
    except Exception:
        pass
    return _json_ok({"ok": True, "level": "ok",
                     "msg": "登记比对通过，ping 无响应。最终以节点本机 arping 为准"})


@app.post("/api/adopt/check_vrid")
def api_adopt_check_vrid(vrid: str = Form(""), vip: str = Form(""),
                         prefix: int = Form(24), cluster_id: int = Form(0),
                         user=Depends(auth_required)):
    """VRID 同网段冲突检测。

    VRRP 只在同二层广播域内冲突，故判据是"同网段 + 同 vrid"，而不是全局唯一。
    冲突时直接推荐一个空闲值，减少用户试错。
    """
    import ipaddress as _ip
    v = (vrid or "").strip()
    if not v:
        return _json_ok({"ok": False, "level": "block", "msg": "请填写 VRID"})
    try:
        iv = int(v)
        if not (1 <= iv <= 255):
            return _json_ok({"ok": True, "level": "block",
                             "msg": "VRID 必须是 1–255 之间的整数"})
    except ValueError:
        return _json_ok({"ok": True, "level": "block", "msg": "VRID 必须是数字"})

    net = None
    if (vip or "").strip():
        try:
            net = _ip.ip_network("%s/%d" % (vip.strip(), int(prefix)), strict=False)
        except Exception:
            net = None
    try:
        rows = query("SELECT id, vip, vrid FROM clusters") or []
        for r in rows:
            if str(r.get("vrid") or "").strip() != v:
                continue
            if int(cluster_id or 0) and int(r.get("id") or 0) == int(cluster_id):
                continue
            if net is not None:
                try:
                    cn = _ip.ip_network(
                        "%s/%d" % (str(r.get("vip") or "").strip(), int(prefix)), strict=False)
                except Exception:
                    cn = None
                if cn is not None and cn != net:
                    continue
            used = {str(x.get("vrid") or "").strip() for x in rows}
            sug = next((str(i) for i in range(1, 256) if str(i) not in used), "")
            return _json_ok({"ok": True, "level": "block", "suggest": sug,
                             "msg": "同网段已有集群使用 VRID %s，已推荐空闲值 %s" % (v, sug)})
    except Exception:
        pass
    return _json_ok({"ok": True, "level": "ok", "msg": "VRID %s 在同网段内未被占用" % v})


@app.post("/api/adopt/release_read_only")
def api_adopt_release_read_only(host: str = Form(...), port: int = Form(3306),
                                db_user: str = Form("root"), db_pass: str = Form(""),
                                user=Depends(auth_required)):
    """在线解除 read_only / super_read_only（用于建 Custos 账号 + 作为主库可写）。

    ⚠ 前提：该实例**不能是别人的从库**。在从库上解除只读并写入会造成主从数据
    不一致，故此处先探测 role，仍是 slave 时直接拒绝，要求先断开复制。
    """
    from . import adopt
    try:
        p = adopt.probe_node(host, int(port), db_user, db_pass)
        if p.get("ok") and p.get("role") == "slave":
            src = (p.get("replica") or {}).get("source_host") or "未知主库"
            return _json_ok({"ok": False, "msg":
                "该实例仍在从 %s 复制，禁止解除只读（会造成主从数据不一致）。"
                "请先断开复制：STOP REPLICA; RESET REPLICA ALL;" % src})
        return _json_ok(adopt.release_read_only(host, int(port), db_user, db_pass))
    except Exception as e:
        return _json_ok({"ok": False, "msg": f"{type(e).__name__}: {e}"})


@app.post("/api/adopt/run")
def api_adopt_run(nodes: str = Form(...), cluster_id: int = Form(0),
                  name: str = Form(""), vip: str = Form(""),
                  vip_prefix: str = Form("24"), vip_port: int = Form(3306),
                  root_user: str = Form("root"), root_pass: str = Form(""),
                  node_roots: str = Form(""),
                  deploy_form: str = Form(""), container: str = Form(""),
                  vrid: str = Form(""), interface: str = Form(""),
                  user=Depends(auth_required)):
    """执行接管：建 Custos 账号 → 登记 nodes/hosts → 生成 sidecar 引导命令。

    cluster_id=0 时按 name/vip 新建集群。返回每个节点的引导命令。

    node_roots: 每台节点各自的数据库 root 凭据（生产环境各机口令常不同），
        JSON 数组 [{"host":"...","port":3306,"user":"root","password":"..."}, ...]；
        没给到的节点回落到统一的 root_user / root_pass。
    """
    from . import adopt
    try:
        arr = json.loads(nodes)
        if not isinstance(arr, list) or not arr:
            raise ValueError("nodes 必须是非空 JSON 数组")
    except Exception as e:
        return _json_ok({"ok": False, "error": "nodes 解析失败：%s" % e})

    actor = user.get("username") if isinstance(user, dict) else str(user)
    cid = int(cluster_id or 0)

    # ---- 部署形态 → 登记形态 ----
    # 裸机 mariadb / 容器 mariadb-docker。未显式指定时按端口推断（非 3306 多为面板/容器化部署）。
    _form = (deploy_form or "").strip().lower()
    try:
        _p0 = int((arr[0] or {}).get("port") or 3306)
    except Exception:
        _p0 = 3306
    if _form not in ("bare", "container"):
        _form = "container" if _p0 != 3306 else "bare"
    _ntype = "mariadb" if _form == "bare" else "mariadb-docker"
    # ---- VIP 端口 ----
    # 裸机形态 keepalived 只漂 IP，不做端口映射（容器形态才是 compose 端口映射），
    # 所以接管场景 VIP 端口默认必须等于旧库实际端口，否则业务连不上。
    _vip_port = int(vip_port or 0) or _p0 or 3306

    if not cid:
        if not (name or "").strip():
            return _json_ok({"ok": False, "error": "新建集群必须给名称"})
        if not (vip or "").strip():
            return _json_ok({"ok": False, "error": "接管必须指定 VIP（节点同网段空闲 IP）"})
        try:
            cid = create_cluster((name or "").strip(), vip=(vip or "").strip(),
                                 vip_prefix=(vip_prefix or "24").strip(),
                                 vip_port=_vip_port,
                                 vrid=(vrid or "").strip() or None,
                                 interface=(interface or "").strip() or None,
                                 node_type=_ntype)
        except Exception as e:
            return _json_ok({"ok": False, "error": "创建集群失败：%s" % e})

    # 1) 只读探测（先确认能不能接）
    probe = adopt.probe_cluster(arr)
    if probe.get("verdict") == adopt.VERDICT_UNSUPPORTED:
        return _json_ok({"ok": False, "error": "探测判定不支持接管", "probe": probe})
    if not any(n.get("ok") for n in probe.get("nodes", [])):
        return _json_ok({"ok": False, "error": "所有节点都无法连接", "probe": probe})

    # 容器形态：把容器名回填给探测结果，使 fix_cmd 生成「进容器执行」的分支
    if _form == "container" and (container or "").strip():
        for n in probe.get("nodes", []):
            n["container"] = container.strip()
    # 集群版本 = 旧库实际版本（后续加从库按此版本匹配，避免版本错配）
    try:
        _ver = ""
        for n in probe.get("nodes", []):
            if n.get("ok") and n.get("version"):
                _ver = str(n["version"])[:48]
                break
        if _ver:
            update_cluster(cid, mysql_version=_ver)
    except Exception:
        pass

    # 防呆：接管（adopt）是「纳管外部已有数据库」，目标应是【新建集群】或【空集群】，
    # 不允许把节点塞进一个已经有节点的存量集群。否则单机实例接入已有主从集群会让
    # monitor 误判复制状态、甚至自动拉起错误的复制通道（如 177→213），详见 DESIGN-006。
    if cid:
        try:
            _ex = query("SELECT COUNT(*) AS c FROM nodes WHERE cluster_id=%s", (cid,))
            _cnt = int((_ex[0].get("c") if _ex and _ex[0] else 0) or 0)
        except Exception:
            _cnt = 0
        if _cnt > 0:
            return _json_ok({"ok": False, "error":
                "接管不能把节点接入已有节点的集群（cluster_id=%s 已含 %d 个节点）。"
                "接管＝纳管外部已有库，请选「＋ 新建集群」把它纳管为独立集群"
                "（例如把单机 177 纳管成独立集群），或先清空该集群再接入。" % (cid, _cnt),
                "probe": probe})

    # 每台节点各自的 root 凭据（生产环境各机口令常不同）；解析失败不阻断，
    # 全部回落统一的 root_user / root_pass。
    per_node = {}
    if not (node_roots or "").strip():
        # 未单独提供时，**回落到第 ① 步填的那套账号**：它已在探测阶段验证过
        # 能连通且权限足够（root_like）。此前不回落会导致用空口令去建账号 →
        # 1045 Access denied，接管卡在"建账号失败"，而用户明明填对了口令。
        for it in arr:
            if not isinstance(it, dict):
                continue
            h = str(it.get("host") or "").strip()
            u = str(it.get("user") or "").strip()
            if not h or not u:
                continue
            try:
                pt = int(it.get("port") or 3306)
            except Exception:
                pt = 3306
            per_node["%s:%d" % (h, pt)] = {
                "user": u, "password": it.get("password") or ""}
    if (node_roots or "").strip():
        try:
            nr = json.loads(node_roots)
            if isinstance(nr, list):
                for it in nr:
                    if not isinstance(it, dict):
                        continue
                    h = str(it.get("host") or "").strip()
                    u = str(it.get("user") or "").strip()
                    if not h or not u:
                        continue
                    try:
                        pt = int(it.get("port") or 3306)
                    except Exception:
                        pt = 3306
                    per_node["%s:%d" % (h, pt)] = {
                        "user": u, "password": it.get("password") or ""}
        except Exception:
            per_node = {}

    # 2) 建 Custos 账号（复用 ensure_ha_creds，自动处理 root 回退与 host 集合）
    creds = get_cluster_creds(cid) or {}
    cred_res = adopt.adopt_cluster(probe, root_user, root_pass, creds,
                                   per_node_roots=per_node)

    # 3) 登记 + 生成引导命令（sidecar only，绝不重装数据库）
    reg = adopt.register_and_bootstrap(probe, cid, root_user=root_user,
                                       root_pass=root_pass, actor=actor,
                                       per_node_roots=per_node)
    try:
        audit.add_audit(actor, "adopt", "cluster=%s" % cid,
                        "接管节点：" + "、".join(r["ip"] for r in reg), cluster_id=cid)
    except Exception:
        pass
    return _json_ok({"ok": True, "cluster_id": cid, "probe": probe,
                     "credentials": cred_res, "bootstrap": reg})


# ==================================================================
# 滚动升级 + 节点修复分档（v2.5.0 F8）
#
# L1 重启 / L2 重建复制 / L3 纯净重建（停服务→清数据目录→重建）。
# L3 严禁对当前主库执行——那等于清空整个集群的数据。
# ==================================================================
@app.post("/api/upgrade/start")
def api_upgrade_start(cluster_id: int = Form(...), actions: str = Form("{}"),
                      new_master: str = Form(""), do_switch: int = Form(1),
                      user=Depends(auth_required)):
    """启动滚动升级。actions 为 JSON：{"192.0.2.213":"restart", ...}

    取值：restart（重启）/ rebuild（重建复制）/ pure（纯净重建）
    """
    from . import rolling
    try:
        acts = json.loads(actions or "{}")
        if not isinstance(acts, dict):
            raise ValueError("actions 必须是 JSON 对象")
        bad = [k for k, v in acts.items() if v not in rolling.ACTIONS]
        if bad:
            return _json_ok({"ok": False,
                             "error": "非法动作（仅 %s）：%s"
                                      % ("、".join(rolling.ACTIONS), "、".join(map(str, bad)))})
    except Exception as e:
        return _json_ok({"ok": False, "error": "actions 解析失败：%s" % e})
    actor = user.get("username") if isinstance(user, dict) else str(user)
    try:
        r = rolling.start_upgrade(int(cluster_id), acts, new_master or "", actor,
                                  bool(int(do_switch)))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})
    return _json_ok({"ok": True, **r})


@app.get("/api/upgrade/status")
def api_upgrade_status(task: str = "", user=Depends(auth_required)):
    from . import rolling
    return _json_ok(rolling.get_task(task))


@app.post("/api/upgrade/cancel")
def api_upgrade_cancel(task: str = Form(...), user=Depends(auth_required)):
    from . import rolling
    return _json_ok(rolling.cancel(task))


@app.get("/api/passwords/accounts")
def api_password_accounts(cluster_id: int = 0, user=Depends(auth_required)):
    """列出本集群可修改的账号（供 UI 勾选展示）。"""
    creds = get_cluster_creds(int(cluster_id)) or {}
    return _json_ok({"ok": True, "accounts": [
        {"key": "ha_monitor", "label": "监控账号", "user": creds.get("ha_user") or "ha_monitor"},
        {"key": "replication", "label": "复制账号", "user": creds.get("repl_user") or "replication"},
        {"key": "app", "label": "业务账号", "user": creds.get("app_user") or "app"},
        {"key": "root", "label": "数据库 root", "user": "root"},
    ]})


# 手动「修复复制」的点击冷却（秒）。monitor 自动巡检有 _heal_fail 退避，
# 但手动点击此前完全没有限频：连点会反复 STOP/START SLAVE、CHANGE MASTER，
# 并每次无脑新增一条 unresolved 告警 —— 「报错不断」的观感正来源于此。
_HEAL_CLICK_COOLDOWN_SEC = 15
_HEAL_CLICK_AT = {}


def _dedup_heal_alert(cluster_id, host, port, msg):
    """记录复制自愈失败告警：同一 (集群,主机,端口) 只保留一条 unresolved。

    此前每次失败都 INSERT 一条新的 unresolved，点 10 次多 10 条，
    告警列表被同一件事刷屏，运维无法判断到底有几个真问题。
    """
    try:
        rows = query("SELECT id FROM alerts WHERE cluster_id=%s AND host=%s AND port=%s "
                     "AND alert_type='selfheal' AND status='unresolved' ORDER BY id DESC LIMIT 1",
                     (cluster_id, host, int(port)))
    except Exception:
        rows = []
    if rows:
        try:
            execute("UPDATE alerts SET message=%s, created_at=NOW() WHERE id=%s",
                    (msg, (rows[0] or {}).get("id")))
            return
        except Exception:
            pass
    execute("INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,created_at) "
            "VALUES(%s,%s,%s,'selfheal','unresolved',%s,NOW())",
            (cluster_id, host, int(port), msg))


@app.post("/api/heal")
def api_heal(request: Request, user=Depends(auth_required),
             host: str = Form(...), port: int = Form(...), cluster_id: int = Form(...)):
    """手动触发某节点的复制自愈（SQL 线程冲突跳过 / IO 线程 GTID 缺口补齐 / 通道错位重指向）。

    双线程独立自愈：不再把 IO 自愈门控在 SQL 自愈失败之后；SQL 已正常时也会检查并修复 IO。
    """
    try:
        creds = load_creds(cluster_id)
        ru, rp = node_root(host, cluster_id)
        p = int(port)
        audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                        "heal", f"{host}:{p}", f"cluster={cluster_id}", cluster_id)

        # 点击冷却：连点只会反复重启复制线程并刷告警，不会让修复更快
        import time as _time
        _ck = (int(cluster_id), str(host), int(p))
        _gap = _time.time() - (_HEAL_CLICK_AT.get(_ck) or 0)
        if 0 < _gap < _HEAL_CLICK_COOLDOWN_SEC:
            return JSONResponse({
                "ok": False,
                "msg": "操作过于频繁：复制自愈需 %d 秒冷却（距上次点击仅 %.0f 秒）。"
                       "连点会反复重启复制线程并刷告警，请稍候再试；"
                       "后台自动巡检每 30 秒也会尝试一次。"
                       % (_HEAL_CLICK_COOLDOWN_SEC, _gap)})
        _HEAL_CLICK_AT[_ck] = _time.time()

        def live():
            return get_status(host, p, creds["ha_user"], creds["ha_pass"], ru, rp, timeout=10)

        st = live()
        if not st["alive"]:
            return JSONResponse({"ok": False, "msg": "节点不可达：" + str(st.get("error") or "探活失败")})
        mode = st.get("mode")
        io_ok = st.get("slave_io") == "Yes"
        sql_ok = st.get("slave_sql") == "Yes"

        # 主库无复制通道 / 双线程均正常：无需自愈
        if mode == "master" or (io_ok and sql_ok):
            return JSONResponse({"ok": True, "msg": "复制正常，无需自愈"})

        # 主库 binlog 已清理（1236）：任何自愈都无效，直接给出可操作的指引，
        # 避免用户反复点「修复复制」却每次都失败（体验上等同于卡死）。
        if is_gtid_purged_error(st.get("slave_io_err") or ""):
            return JSONResponse({
                "ok": False,
                "msg": ("主库 binlog 已清理，该从库请求的 GTID 在主库中已不存在，复制无法自愈"
                        "（重挂载 / 注入空事务均无效）。\n"
                        "请对该节点点击【重建】：用主库数据重做该从库并重新挂载复制。")})

        steps = []
        cm = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cluster_id,))
        cur_h = cm[0]["host"] if cm else None
        cur_p = int(cm[0]["port"]) if cm and cm[0].get("port") is not None else None

        # 1) 若从库指向的源并非当前主库（通道错位/指错旧主），先重指向当前主库
        if cur_h and cur_p and (st.get("master_host") != cur_h
                                or int(st.get("master_port") or 0) != int(cur_p or 0)):
            try:
                mok, merr = make_slave_of(
                    host, p, creds["ha_user"], creds["ha_pass"],
                    cur_h, cur_p, creds["repl_user"], creds["repl_pass"], ru, rp)
                steps.append("重指向当前主库 %s:%s：%s" % (cur_h, cur_p, "成功" if mok else merr))
                if mok:
                    st = live()
                    io_ok = st.get("slave_io") == "Yes"
                    sql_ok = st.get("slave_sql") == "Yes"
            except Exception as e:
                steps.append("重指向当前主库异常: %s" % e)

        # 2) 修 SQL 线程（幂等冲突注入空事务 / 重启）
        if not sql_ok:
            try:
                ok_s, msg_s = heal_replication(host, p, creds["ha_user"], creds["ha_pass"], ru, rp)
                steps.append("SQL线程: " + msg_s)
            except Exception as e:
                steps.append("SQL线程自愈异常: %s" % e)

        # 3) 修 IO 线程（GTID 缺口 / 重连），无论 SQL 是否修复都尝试
        if not io_ok and cur_h and cur_p:
            try:
                mru, mrp = node_root(cur_h, cluster_id)
                ok_i, msg_i = heal_io_gtid_gap(
                    host, p, creds["ha_user"], creds["ha_pass"], ru, rp,
                    cur_h, cur_p, mru, mrp)
                steps.append("IO线程: " + msg_i)
            except Exception as e:
                steps.append("IO线程自愈异常: %s" % e)

            # 3.5) IO 仍失败且为「Access denied」——主库 replication 账号密码漂移。
            # 从库侧密码是对的（make_slave_of 已用库存 repl_pass），问题在主库账号，
            # 故到主库重置 replication 密码（mysql_native_password + 同密从库 IP 账号），
            # 再重启从库 IO 线程立即重连。
            try:
                st_io = get_status(host, p, creds["ha_user"], creds["ha_pass"], ru, rp, timeout=10)
                io_err_now = (st_io.get("slave_io_err") or "").strip()
                if st_io.get("slave_io") != "Yes" and "Access denied for user" in io_err_now:
                    mru, mrp = node_root(cur_h, cluster_id)
                    rok, rmsg = reset_replication_creds_on_master(
                        cur_h, cur_p, creds["ha_user"], creds["ha_pass"], mru, mrp,
                        creds["repl_user"], creds["repl_pass"], slave_ip=host)
                    steps.append("主库重置复制密码: " + rmsg)
                    if rok:
                        sok, smsg = restart_slave_io(
                            host, p, creds["ha_user"], creds["ha_pass"], ru, rp)
                        steps.append(smsg)
            except Exception as e:
                steps.append("复制密码重置异常: %s" % e)

            # 3.6) IO 仍失败且为「认证类」错误（典型：caching_sha2_password 报
            #      “Authentication requires secure connection”）。这类错误此前会被
            #      heal_io_gtid_gap 判为「非 GTID 缺失类」直接跳过，导致一键修复无效。
            #      处理：重建复制通道并带 SOURCE_PUBLIC_KEY=1；仍不行则到源端把复制
            #      账号降级为 mysql_native_password。
            try:
                st_io2 = get_status(host, p, creds["ha_user"], creds["ha_pass"], ru, rp, timeout=10)
                io_err_2 = (st_io2.get("slave_io_err") or "").strip()
                if st_io2.get("slave_io") != "Yes" and io_err_2:
                    mru2, mrp2 = node_root(cur_h, cluster_id)
                    ok_a, msg_a = heal_io_auth(
                        host, p, creds["ha_user"], creds["ha_pass"], ru, rp,
                        repl_user=creds.get("repl_user"), repl_pass=creds.get("repl_pass"),
                        master_host=cur_h, master_port=cur_p,
                        master_root_user=mru2, master_root_pass=mrp2)
                    steps.append("IO认证自愈: " + msg_a)
            except Exception as e:
                steps.append("IO认证自愈异常: %s" % e)

        # 重新读取，给出最终状态与真实错误（而非掩盖为「复制正常」）
        st2 = live()
        io2, sql2 = st2.get("slave_io"), st2.get("slave_sql")
        if io2 == "Yes" and sql2 == "Yes":
            # 自愈成功：记录「已自愈」状态（供告警列表状态标记）
            try:
                execute("INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,created_at) "
                        "VALUES(%s,%s,%s,'selfheal','healed',%s,NOW())",
                        (cluster_id, host, int(port), "；".join(steps) if steps else "复制已修复"))
            except Exception:
                pass
            return JSONResponse({"ok": True, "msg": "复制已修复。" + ("；".join(steps) if steps else "")})
        io_err = (st2.get("slave_io_err") or "").strip()
        sql_err = (st2.get("slave_sql_err") or "").strip()
        detail = "；".join([x for x in [io_err, sql_err] if x])
        if detail:
            tail = "真实错误: " + detail
        else:
            tail = "真实错误未知（建议检查 %s → %s 的网络连通、复制账号(%s)权限与 GTID 集合）" % (
                host, cur_h, creds.get("repl_user"))
        # 自愈失败：记录「未恢复」状态（同一节点去重，避免连点刷屏）
        try:
            _dedup_heal_alert(cluster_id, host, int(port), tail)
        except Exception:
            pass
        return JSONResponse({"ok": False,
                              "msg": "复制仍未恢复（IO=%s, SQL=%s）。%s [%s]" % (
                                  io2, sql2, tail, "；".join(steps))})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"})


def _failover_cooldown_sec():
    """故障切换冷却秒数：优先取运行时设置（Web / API 可即时调整），未设置则回退环境变量默认值。

    与 monitor 的「延迟/压力自动切换」共用同一个 key，保证两条路径口径一致。
    """
    try:
        v = get_setting("FAILOVER_COOLDOWN_SEC", "")
        if v not in ("", None):
            return int(v)
    except (TypeError, ValueError):
        pass
    return FAILOVER_COOLDOWN_SEC


@app.post("/api/failover")
def api_failover(cluster_id: int = Form(...), user=Depends(auth_required)):
    try:
        run_once()  # 先按现状刷新，确保切换基于最新拓扑
    except Exception:
        pass
    # 手动切换冷却：避免与刚发生的切换（自动或手动）在冷却期内重复触发
    _last = _LAST_FAILOVER_TS.get(cluster_id, 0)
    _cd = _failover_cooldown_sec()
    if _time.time() - _last < _cd:
        return JSONResponse({"ok": False,
                             "msg": f"切换冷却中：距上次切换仅 {int(_time.time() - _last)}s"
                                    f"（冷却 {_cd}s），请稍后再试"})
    try:
        ok = do_failover(cluster_id, "manual")
        msg = "已触发手动切换" if ok else "切换未执行(检查配置/日志)"
    except Exception as e:
        ok, msg = False, f"切换失败: {e}"
    audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                    "failover", f"cluster={cluster_id}", msg, cluster_id)
    try:
        run_once()  # 切换后立即重建拓扑并回写状态，使仪表盘即时刷新
    except Exception:
        pass
    audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                    "failover", f"cluster={cluster_id}", "manual", cluster_id)
    return JSONResponse({"ok": ok, "msg": msg})


@app.post("/api/failover/to")
def api_failover_to(host: str = Form(...), port: int = Form(...),
                    cluster_id: int = Form(0), user=Depends(auth_required)):
    cid = int(cluster_id) if cluster_id else None
    if not cid:
        # 未传 cluster_id 时按 host 反查其所属集群
        row = query("SELECT cluster_id FROM hosts WHERE host=%s AND port=%s", (host, int(port)))
        cid = row[0]["cluster_id"] if row else None
    if not cid:
        return JSONResponse({"ok": False, "msg": "无法确定节点所属集群"})
    try:
        run_once()
    except Exception:
        pass
    # 手动切换冷却：避免与刚发生的切换（自动或手动）在冷却期内重复触发
    _last = _LAST_FAILOVER_TS.get(cid, 0)
    _cd = _failover_cooldown_sec()
    if _time.time() - _last < _cd:
        print(f"[failover] 手动切换被冷却拦截: cluster={cid} 距上次切换 {int(_time.time() - _last)}s "
              f"(冷却 {_cd}s)", flush=True)
        return JSONResponse({"ok": False,
                             "msg": f"切换冷却中：距上次切换仅 {int(_time.time() - _last)}s"
                                    f"（冷却 {_cd}s），请稍后再试"})
    try:
        ok = do_failover_to(cid, host, int(port), "manual: 指定主库")
        msg = ("已将 %s:%s 设为主库" % (host, port)) if ok else "设置失败（详见服务端日志：failover/权限/连接）"
    except Exception as e:
        ok, msg = False, f"切换失败: {e}"
    try:
        run_once()  # 设主后立即重建拓扑并回写状态，使仪表盘即时刷新
    except Exception:
        pass
    audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                    "failover_to", f"{host}:{port}", f"cluster={cid}", cid)
    return JSONResponse({"ok": ok, "msg": msg})


def _load_alert_emails():
    """返回全部告警邮箱及其绑定的集群列表（用于「提醒邮箱」横排展示与编辑）。"""
    emails = query("SELECT id, email, is_default, enabled FROM alert_emails ORDER BY id")
    for e in emails:
        e["clusters"] = [
            {"id": r["cluster_id"], "name": r["cluster_name"]}
            for r in query(
                "SELECT m.cluster_id AS cluster_id, c.name AS cluster_name "
                "FROM alert_email_map m LEFT JOIN clusters c ON c.id=m.cluster_id "
                "WHERE m.email_id=%s ORDER BY m.cluster_id", (e["id"],))
        ]
    return emails


@app.get("/settings")
def settings_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    """服务端配置：SMTP 与多邮箱告警。各集群的管理/复制账号与密码请在「集群管理」中设置。"""
    # 用 smtp_view()：与发信口径(_cfg)一致，DB 优先 + 环境变量回退。
    # 原写法只读 DB，导致「用环境变量配 SMTP」时邮件能发、页面却显示未配置。
    cfg = smtp_view()
    alert_emails = _load_alert_emails()
    clusters = [{"id": c["id"], "name": c.get("name") or f"集群{c['id']}"}
                for c in list_clusters()]
    # 高级切换 / 监控开关与阈值（全局，默认与 monitor 守护进程一致）
    adv = dict(
        auto_failover=get_setting("AUTO_FAILOVER", "1") == "1",
        # 默认与 monitor.py 运行时一致（get_setting("AUTO_REPAIR","1")），避免 UI 显示「关」但实际在自愈
        auto_repair=get_setting("AUTO_REPAIR", "1") == "1",
        lag_failover=get_setting("LAG_FAILOVER_ENABLED", "0") == "1",
        pressure_failover=get_setting("PRESSURE_FAILOVER_ENABLED", "0") == "1",
        # 危险：允许自愈跳过 1062/1032/1146 数据冲突（会造成主从数据永久不一致）
        skip_data_conflict=get_setting("SKIP_DATA_CONFLICT", "0") == "1",
        lag_failover_sec=get_setting("LAG_FAILOVER_SEC", "60"),
        lag_failover_cycles=get_setting("LAG_FAILOVER_CYCLES", "3"),
        pressure_threads_running=get_setting("PRESSURE_THREADS_RUNNING", "50"),
        pressure_row_lock_waits=get_setting("PRESSURE_ROW_LOCK_WAITS", "50"),
        failover_cooldown_sec=get_setting("FAILOVER_COOLDOWN_SEC", "120"),
        # binlog 空间守卫
        binlog_alert_mb=get_setting("BINLOG_SIZE_ALERT_MB", "20480"),
        auto_purge_binlog=get_setting("AUTO_PURGE_BINLOG", "0") == "1",
        binlog_keep_hours=get_setting("BINLOG_KEEP_HOURS", "240"),
        # 切换前交叉验证（防脑裂）
        require_witness=get_setting("REQUIRE_WITNESS_FOR_FAILOVER", "1") == "1",
    )
    # 管理器内网 IP：页面可编辑的值 + 实际生效值与来源
    # （环境变量优先于页面设置，故环境变量存在时页面填写不会生效 —— 必须明示，
    #  否则运维会遇到「填不填都没差异」的困惑）
    _eff_ip, _eff_src = get_manager_public_ip_source()
    _src_text = {"env": "环境变量 MANAGER_PUBLIC_IP（页面填写不生效）",
                 "db": "本页设置",
                 "auto": "自动探测（未显式配置，建议填写）"}.get(_eff_src, _eff_src)
    return render("settings.html", request, user=user, cfg=cfg, msg=msg, error=error,
                  alert_emails=alert_emails, clusters=clusters,
                  check_interval=get_setting("CHECK_INTERVAL", "2"),
                  manager_ip=get_setting("MANAGER_PUBLIC_IP", ""),
                  manager_ip_effective=_eff_ip, manager_ip_source=_src_text,
                  manager_ip_readonly=(_eff_src == "env"),
                  adv=adv)


@app.post("/settings")
def settings_save(
    request: Request,
    smtp_host: str = Form(""),
    smtp_port: str = Form(""),
    smtp_user: str = Form(""),
    smtp_pass: str = Form(""),
    smtp_from: str = Form(""),
    check_interval: str = Form(""),
    manager_ip: str = Form(""),
    # 高级切换 / 监控（全局；未传则不改动，避免覆盖）
    auto_failover: str = Form(""),
    auto_repair: str = Form(""),
    lag_failover_enabled: str = Form(""),
    pressure_failover_enabled: str = Form(""),
    skip_data_conflict: str = Form(""),
    require_witness: str = Form(""),
    auto_purge_binlog: str = Form(""),
    binlog_alert_mb: str = Form(""),
    binlog_keep_hours: str = Form(""),
    lag_failover_sec: str = Form(""),
    lag_failover_cycles: str = Form(""),
    pressure_threads_running: str = Form(""),
    pressure_row_lock_waits: str = Form(""),
    failover_cooldown_sec: str = Form(""),
    user=Depends(auth_required),
):
    # SMTP：【仅当表单实际提交且非空】才写入。
    # 原因：/settings 是多个表单（SMTP / 管理器IP与轮询 / 高级切换）共用的提交端点，
    # 而 saveMgr()/saveAdv() 只提交各自字段，smtp_* 取 Form("") 默认空串。此前无条件
    # upsert 会把已保存的 SMTP 配置【静默清空】——生产上表现为「明明配过，却显示未配置」。
    # 与 SMTP_PASS 既有的「留空保持原密码」语义保持一致。
    if smtp_host.strip():
        upsert_setting("SMTP_HOST", smtp_host.strip())
    if smtp_port.strip():
        upsert_setting("SMTP_PORT", smtp_port.strip())
    if smtp_user.strip():
        upsert_setting("SMTP_USER", smtp_user.strip())
    if smtp_pass.strip():
        upsert_setting("SMTP_PASS", smtp_pass.strip())
    # 发件人地址（可选）：留空则发信时回退到 SMTP_USER
    if smtp_from.strip():
        upsert_setting("SMTP_FROM", smtp_from.strip())
    # 监控轮询间隔（秒）：动态生效，monitor 循环每次读取
    ci = (check_interval or "").strip()
    if ci:
        try:
            iv = int(ci)
            if iv >= 1:
                upsert_setting("CHECK_INTERVAL", str(iv))
        except ValueError:
            pass
    # 管理器内网 IP（记录用；运行时以环境变量 MANAGER_PUBLIC_IP 为准）
    if manager_ip.strip():
        upsert_setting("MANAGER_PUBLIC_IP", manager_ip.strip())
    # 高级切换 / 监控开关与阈值（全局）
    uname = user.get("username") if isinstance(user, dict) else str(user)
    _bools = {
        "AUTO_FAILOVER": auto_failover,
        "AUTO_REPAIR": auto_repair,
        "LAG_FAILOVER_ENABLED": lag_failover_enabled,
        "PRESSURE_FAILOVER_ENABLED": pressure_failover_enabled,
        "SKIP_DATA_CONFLICT": skip_data_conflict,
        "REQUIRE_WITNESS_FOR_FAILOVER": require_witness,
        "AUTO_PURGE_BINLOG": auto_purge_binlog,
    }
    for k, v in _bools.items():
        s = (v or "").strip()
        if not s:
            continue  # 未传（如未勾选也不影响）则跳过
        val = "1" if s in ("1", "true", "on", "True") else "0"
        upsert_setting(k, val)
        audit.add_audit(uname, "setting_set", k, val, None)
    # 有「业务含义下界」的阈值：越界时【保持原值并明确提示】，而不是静默归零。
    # 实测教训（2026-10-02）：填 -5 被 clamp 成 0 后，LAG_FAILOVER_SEC=0 会让
    # 「从库延迟 ≥ 0s」恒成立 —— 延迟切换几乎每个周期都达标，集群被反复自动切换，
    # 而页面上看不出阈值已经失效。故：小于下界视为无效输入，拒绝写入。
    _MIN_INT = {
        "LAG_FAILOVER_SEC": 10,       # 低于 10s 的延迟阈值没有实际意义
        "LAG_FAILOVER_CYCLES": 1,
        "FAILOVER_COOLDOWN_SEC": 30,  # 冷却过短会让切换反复发生
    }
    _rejected = []
    _ints = {
        "LAG_FAILOVER_SEC": lag_failover_sec,
        "LAG_FAILOVER_CYCLES": lag_failover_cycles,
        "PRESSURE_THREADS_RUNNING": pressure_threads_running,
        "PRESSURE_ROW_LOCK_WAITS": pressure_row_lock_waits,
        "FAILOVER_COOLDOWN_SEC": failover_cooldown_sec,
        "BINLOG_SIZE_ALERT_MB": binlog_alert_mb,
        "BINLOG_KEEP_HOURS": binlog_keep_hours,
    }
    for k, v in _ints.items():
        s = (v or "").strip()
        if not s:
            continue
        try:
            iv = int(s)
        except ValueError:
            continue
        _lo = _MIN_INT.get(k)
        if _lo is not None and iv < _lo:
            # 越界：保持原值，并在返回里明确告知（不静默改成 0）
            _rejected.append("%s 不能小于 %d（已保持原值 %s）"
                             % (k, _lo, get_setting(k, str(_lo))))
            continue
        if iv < 0:
            iv = 0
        upsert_setting(k, str(iv))
        audit.add_audit(uname, "setting_set", k, str(iv), None)
    if _rejected:
        return JSONResponse({"ok": True,
                             "msg": "✓ 已保存配置；以下项未生效：" + "；".join(_rejected)})
    return JSONResponse({"ok": True, "msg": "✓ 已保存配置"})


@app.get("/api/node_resources")
def api_node_resources(cluster_id: int = 0, host: str = "", port: int = 0,
                       user=Depends(auth_required)):
    """返回某节点 MySQL 视角的资源指标（连接/缓冲池/数据量/QPS）。

    说明：远程节点无 SSH/agent，OS 级 CPU/内存/磁盘使用率不可得，
    故用 MySQL 可观测指标近似：连接数%、InnoDB 缓冲池使用%、数据量(字节)、QPS。
    """
    try:
        creds = get_cluster_creds(cluster_id)
        if not creds:
            return _json_ok({"ok": False, "error": "集群不存在或无凭证"})
        ru, rp = node_root(host, int(cluster_id))
        conn = connect_robust(host, int(port), creds["ha_user"], creds["ha_pass"], ru, rp, timeout=3)
        if conn is None:
            return _json_ok({"ok": False, "error": "无法连接节点"})
        try:
            cur = conn.cursor()

            def _val(sql):
                try:
                    cur.execute(sql)
                    row = cur.fetchone()
                    return row[1] if row else None
                except Exception:
                    return None

            connections = _val("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            max_conn = _val("SHOW VARIABLES LIKE 'max_connections'")
            buf_data = _val("SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_bytes_data'")
            buf_size = _val("SHOW VARIABLES LIKE 'innodb_buffer_pool_size'")
            data_bytes = _val("SELECT SUM(DATA_LENGTH+INDEX_LENGTH) FROM information_schema.TABLES")
            queries = _val("SHOW GLOBAL STATUS LIKE 'Queries'")
            uptime = _val("SHOW GLOBAL STATUS LIKE 'Uptime'")
            slow_q = _val("SHOW GLOBAL STATUS LIKE 'Slow_queries'")
            try:
                conn_pct = round(int(connections) / int(max_conn) * 100, 1) if (connections and max_conn) else None
            except Exception:
                conn_pct = None
            try:
                buf_pct = round(int(buf_data) / int(buf_size) * 100, 1) if (buf_data and buf_size) else None
            except Exception:
                buf_pct = None
            try:
                qps = round(int(queries) / int(uptime), 1) if (queries and uptime) else None
            except Exception:
                qps = None
            return _json_ok({
                "ok": True,
                "host": host, "port": int(port),
                "connections": int(connections) if connections is not None else None,
                "max_connections": int(max_conn) if max_conn is not None else None,
                "conn_pct": conn_pct,
                "buf_data": int(buf_data) if buf_data is not None else None,
                "buf_size": int(buf_size) if buf_size is not None else None,
                "buf_pct": buf_pct,
                "data_bytes": int(data_bytes) if data_bytes else 0,
                "qps": qps,
                "slow_queries": int(slow_q) if slow_q is not None else 0,
            })
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/activity_feed")
def api_activity_feed(limit: int = 12, user=Depends(auth_required)):
    """聚合实时活动：故障切换(switch_history) + 操作审计(audit_log)，按时间倒序。"""
    try:
        events = []
        for r in query(
            "SELECT h.created_at, h.from_host, h.from_port, h.to_host, h.to_port, h.reason, "
            "h.cluster_id, c.name AS cluster_name "
            "FROM switch_history h LEFT JOIN clusters c ON c.id=h.cluster_id "
            "ORDER BY h.created_at DESC LIMIT %s", (int(limit),)):
            events.append({
                "ts": r["created_at"], "type": "failover", "level": "warn",
                "cluster_id": r["cluster_id"], "cluster_name": r["cluster_name"] or "—",
                "text": "主库切换 " + (f"{r['from_host']}:{r['from_port']}" if r["from_host"] else "—")
                        + f" → {r['to_host']}:{r['to_port']}（{r['reason'] or ''}）",
            })
        for a in query(
            "SELECT actor, action, target, detail, cluster_id, created_at "
            "FROM audit_log ORDER BY created_at DESC LIMIT %s", (int(limit),)):
            events.append({
                "ts": a["created_at"], "type": a["action"], "level": "info",
                "cluster_id": a["cluster_id"], "cluster_name": "—",
                "text": (f"{a['actor']} {a['action']} {a['target'] or ''} {a['detail'] or ''}").strip(),
            })
        events.sort(key=lambda e: (e["ts"] or ""), reverse=True)
        return _json_ok({"ok": True, "events": events[:int(limit)]})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/settings/test_email")
def test_email(user=Depends(auth_required)):
    """发送测试邮件，验证 SMTP 配置与收件地址是否正确（发往全部已配置邮箱）。"""
    try:
        emails = [r["email"] for r in query("SELECT email FROM alert_emails")]
        if not emails:
            legacy = (get_setting("ALERT_EMAIL") or "").strip()
            if legacy:
                emails = [x.strip() for x in legacy.split(",") if x.strip()]
        if not emails:
            return JSONResponse({"ok": False,
                                  "msg": "✗ 尚未添加任何接收邮箱，请先在「提醒邮箱」中新建"},
                                 status_code=200)
        ok = send_alert("邮件告警测试", "测试邮件，告警通道正常！", recipients=emails, force=True)
        if ok:
            return JSONResponse({"ok": True, "msg": "✓ 发送成功：测试邮件已发出，请检查收件箱"})
        detail = last_error or "SMTP 配置不正确或收件地址无效"
        return JSONResponse({"ok": False, "msg": f"✗ 发送失败：{detail}"}, status_code=200)
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 发送失败：{e}"}, status_code=200)


@app.post("/settings/email/add")
def email_add(email: str = Form(""), user=Depends(auth_required)):
    email = (email or "").strip().lower()
    if not email or "@" not in email:
        return JSONResponse({"ok": False, "msg": "✗ 邮箱格式不正确"}, status_code=200)
    try:
        execute("INSERT INTO alert_emails (email, is_default) VALUES (%s, 0)", (email,))
        return JSONResponse({"ok": True, "msg": f"✓ 已添加 {email}"})
    except Exception as e:
        # 重复邮箱是常态操作（用户忘了已加过），直接把 MySQL 的 1062 原文抛给用户
        # 既不专业也吓人；这里识别成「已存在」的明确提示。
        _s = str(e)
        if "1062" in _s or "Duplicate" in _s or "duplicate" in _s:
            return JSONResponse({"ok": False, "msg": f"✗ 邮箱 {email} 已存在，无需重复添加"},
                                status_code=200)
        return JSONResponse({"ok": False, "msg": f"✗ 添加失败：{_s}"}, status_code=200)


@app.post("/settings/email/delete")
def email_delete(id: int = Form(...), user=Depends(auth_required)):
    try:
        execute("DELETE FROM alert_email_map WHERE email_id=%s", (id,))
        execute("DELETE FROM alert_emails WHERE id=%s", (id,))
        return JSONResponse({"ok": True, "msg": "✓ 已删除"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 删除失败：{e}"}, status_code=200)


@app.post("/settings/email/set_default")
def email_set_default(id: int = Form(...), is_default: int = Form(0), user=Depends(auth_required)):
    try:
        execute("UPDATE alert_emails SET is_default=%s WHERE id=%s", (int(is_default), id))
        return JSONResponse({"ok": True, "msg": "✓ 已更新"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 更新失败：{e}"}, status_code=200)


@app.post("/settings/email/toggle")
def email_toggle(id: int = Form(...), enabled: int = Form(1), user=Depends(auth_required)):
    """切换某邮箱是否接收告警邮件。enabled=0 时即使已绑定集群也不发送。"""
    try:
        execute("UPDATE alert_emails SET enabled=%s WHERE id=%s", (int(enabled), id))
        return JSONResponse({"ok": True, "msg": "✓ 已更新"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 更新失败：{e}"}, status_code=200)


@app.post("/settings/email/map")
def email_map(id: int = Form(...), cluster_ids: str = Form(""), user=Depends(auth_required)):
    """保存某邮箱绑定的集群列表（逗号分隔的 cluster_id）。"""
    try:
        ids = [int(x) for x in (cluster_ids or "").split(",") if x.strip()]
        execute("DELETE FROM alert_email_map WHERE email_id=%s", (id,))
        for cid in ids:
            execute("INSERT IGNORE INTO alert_email_map (email_id, cluster_id) VALUES (%s, %s)",
                    (id, cid))
        return JSONResponse({"ok": True, "msg": "✓ 已保存集群绑定"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 保存失败：{e}"}, status_code=200)


@app.get("/change_password")
def change_password_page(request: Request, user=Depends(auth_required)):
    return render("change_password.html", request, user=user)


@app.post("/change_password")
def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    user=Depends(auth_required),
):
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("SELECT password_hash FROM users WHERE username=%s", (user,))
            row = c.fetchone()
            if not row or not verify_pwd(row["password_hash"], current_password):
                return render("change_password.html", request, user=user,
                              msg="", error="当前密码错误")
            c.execute("UPDATE users SET password_hash=%s WHERE username=%s",
                      (hash_pwd(new_password), user))
        conn.commit()
    finally:
        conn.close()
    resp = RedirectResponse("/settings?msg=密码已修改", status_code=303)
    resp.delete_cookie("ha_must_change")
    return resp


# ---------------- 集群管理（多集群：每集群独立账号/复制账号/密码/VIP） ----------------
def _cluster_view(cid: int):
    """构造单个集群在页面上的完整视图（总览卡片与单集群详情页共用）。

    返回 None 表示集群不存在。字段：
      id/name/vip/...（集群表原始列）
      node_count 节点总数 / online 在线数 / current_master / slaves
      host_rows  节点表格行 / has_issue 是否存在异常 / creds 集群凭证
    """
    c = get_cluster(cid)
    if not c:
        return None
    n = query("SELECT COUNT(*) AS n FROM hosts WHERE cluster_id=%s", (cid,))
    node_count = int(n[0]["n"]) if n else 0
    cm_rows = query("SELECT host, port FROM current_master WHERE cluster_id=%s", (cid,))
    cm = f"{cm_rows[0]['host']}:{cm_rows[0]['port']}" if cm_rows else ""
    hs = query(
        "SELECT host, port, status, role, slave_io, slave_sql, err, repl_state, upstream, downstream, "
        "repl_delay AS seconds_behind "
        "FROM hosts h WHERE h.cluster_id=%s", (cid,))
    host_rows = []
    slaves = []
    online = 0
    for h in hs:
        hk = f"{h['host']}:{h['port']}"
        is_cm = (hk == cm)
        if not is_cm:
            slaves.append(hk)
        repl_bad = (not is_cm) and (
            h.get("slave_io") not in ("Yes", None) or
            h.get("slave_sql") not in ("Yes", None))
        down = h.get("status") != "up"
        if not down:
            online += 1
        host_rows.append({
            "host": h["host"], "port": h["port"],
            "role": "主库" if is_cm else "从库",
            "status": h.get("status"),
            "slave_io": h.get("slave_io") or "—",
            "slave_sql": h.get("slave_sql") or "—",
            "err": (h.get("err") or "").strip(),
            "seconds_behind": h.get("seconds_behind"),
            "repl_state": h.get("repl_state"),
            "upstream": h.get("upstream"),
            "downstream": _parse_downstream(h.get("downstream")),
            "bad": bool(repl_bad or down),
        })
    has_issue = (online != node_count) or any(r["bad"] for r in host_rows)
    cc = dict(
        c, node_count=node_count,
        current_master=cm or "未选举",
        online=online,
        slaves=slaves,
        host_rows=host_rows,
        has_issue=has_issue,
    )
    try:
        cc["creds"] = get_cluster_creds(cid)
    except Exception:
        cc["creds"] = None
    # v2.3.3：维护窗口状态（页面显著提示，避免「开了忘了关」）
    try:
        from .maintenance import summary as _maint_summary
        cc["maint"] = _maint_summary(cid)
    except Exception:
        cc["maint"] = None
    cc["js"] = _cluster_js(cc)
    return cc


def _cluster_js(c):
    """供前端 JS 使用的精简集群信息（编辑弹窗回填用）。

    只取渲染需要的标量字段：
      - 密码一并下发，供「⚙ 修改配置/密码」弹窗显示/复制（该页需管理员登录，
        且 /api/node_detail 早已把同样三个密码下发给前端，此处不新增暴露面）；
      - 剔除 created_at/updated_at 等 datetime（tojson 序列化不稳定）；
      - 时间戳统一转字符串，任何异常类型都兜底成字符串。
    """
    cr = c.get("creds") or {}

    def _s(v):
        if v is None:
            return ""
        if isinstance(v, (datetime, date, time)):
            return v.isoformat()
        if isinstance(v, (str, int, float, bool)):
            return v
        return str(v)

    return {
        "id": c.get("id"),
        "name": _s(c.get("name")),
        "vip": _s(c.get("vip")),
        "vrid": _s(c.get("vrid")),
        "vip_port": _s(c.get("vip_port")),
        "vip_prefix": _s(c.get("vip_prefix")),
        "interface": _s(c.get("interface")),
        "node_type": _s(c.get("node_type") or "mariadb-docker"),
        "mysql_version": _s(c.get("mysql_version")),
        "creds": {
            "ha_user": _s(cr.get("ha_user")),
            "ha_pass": _s(cr.get("ha_pass")),
            "repl_user": _s(cr.get("repl_user")),
            "repl_pass": _s(cr.get("repl_pass")),
            "app_user": _s(cr.get("app_user")),
            "app_pass": _s(cr.get("app_pass")),
        },
    }


@app.get("/clusters")
def clusters_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    """集群管理 · 总览页：所有集群的状态卡片列表。"""
    clusters = []
    for row in list_clusters():
        v = _cluster_view(row["id"])
        if v:
            clusters.append(v)
    return render("clusters.html", request, user=user, clusters=clusters,
                  msg=msg, error=error,
                  auto_failover=get_setting("AUTO_FAILOVER", "1") == "1",
                  auto_repair=get_setting("AUTO_REPAIR", "1") == "1")


@app.get("/clusters/{cid}")
def cluster_detail_page(request: Request, cid: int, user=Depends(auth_required),
                        msg: str = "", error: str = ""):
    """集群管理 · 单集群详情页（左侧集群子菜单进入）。

    顶栏标题由模板的 page_title 块给出："集群管理-{集群名称}"。
    """
    c = _cluster_view(cid)
    if not c:
        return RedirectResponse("/clusters?error=集群不存在或已被删除", status_code=303)
    return render("cluster_detail.html", request, user=user, cluster=c,
                  clusters=[c], msg=msg, error=error,
                  auto_failover=get_setting("AUTO_FAILOVER", "1") == "1",
                  auto_repair=get_setting("AUTO_REPAIR", "1") == "1",
                  lag_failover=get_setting("LAG_FAILOVER_ENABLED", "0") == "1",
                  pressure_failover=get_setting("PRESSURE_FAILOVER_ENABLED", "0") == "1",
                  slowlog_enabled=slow_log.slowlog_enabled())


# ==================================================================
# 备份 / 恢复（全库 / 指定库）
# 纯 Python 实现，容器内无需 mysqldump；产物落在 ./backups/<集群ID>/。
# ==================================================================
@app.get("/adopt")
def adopt_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    """存量集群接管向导（v2.5.0 F10）：只读探测 → 建账号 → 登记 → 装 sidecar。"""
    return render("adopt.html", request, user=user, clusters=list_clusters(),
                  msg=msg, error=error)


@app.get("/clusters/{cid}/upgrade")
def cluster_upgrade_page(request: Request, cid: int, user=Depends(auth_required),
                         msg: str = "", error: str = ""):
    """集群滚动升级页（v2.5.0 F8）。"""
    c = _cluster_view(cid)
    if not c:
        return RedirectResponse("/clusters?error=集群不存在或已被删除", status_code=303)
    try:
        mh, mp = get_current_master(int(cid))
    except Exception:
        mh, mp = "", 0
    return render("upgrade.html", request, user=user, cluster=c,
                  nodes=list_nodes(int(cid)), master=mh or "", msg=msg, error=error)


@app.get("/clusters/{cid}/backup")
def cluster_backup_page(request: Request, cid: int, user=Depends(auth_required),
                        msg: str = "", error: str = ""):
    """集群备份 / 恢复页。"""
    c = _cluster_view(cid)
    if not c:
        return RedirectResponse("/clusters?error=集群不存在或已被删除", status_code=303)
    return render("backup.html", request, user=user, cluster=c,
                  backups=backup.list_backups(cid),
                  backup_dir=backup._cluster_dir_name(cid),
                  blog=backup.read_backup_log(cid), msg=msg, error=error)


@app.get("/api/backup/databases")
def api_backup_databases(cluster_id: int = 0, host: str = "", port: int = 3306,
                         user=Depends(auth_required)):
    """列出该节点上可备份的业务库。"""
    try:
        r = backup.list_databases(int(cluster_id), host.strip(), int(port))
    except Exception as e:
        r = {"ok": False, "error": str(e)}
    return JSONResponse(r)


@app.post("/api/backup/run")
def api_backup_run(cluster_id: int = Form(...), host: str = Form(...),
                   port: int = Form(3306), scope: str = Form("all"),
                   databases: str = Form(""), with_data: bool = Form(True),
                   user=Depends(auth_required)):
    """执行备份。scope=all 全库；scope=custom 时 databases 为逗号分隔的库名。"""
    dbs = [d.strip() for d in (databases or "").split(",") if d.strip()]
    r = backup.run_backup(int(cluster_id), host.strip(), int(port),
                          databases=dbs if scope == "custom" else None,
                          with_data=bool(with_data), scope=scope)
    if r.get("ok"):
        m = r.get("meta", {})
        r["msg"] = (f"备份完成：{m.get('tables', 0)} 张表 / {m.get('views', 0)} 个视图 / "
                    f"{m.get('rows', 0)} 行，文件 {m.get('file', '')} "
                    f"（{round(m.get('size', 0) / 1024, 1)} KB）")
    else:
        # 备份失败必须留痕并告警：静默失败的备份等于没有备份，
        # 真正需要恢复时才发现不可用就太晚了。
        _err = r.get("error", "未知错误")
        try:
            send_alert("集群备份失败",
                       f"集群 {cluster_id} 对 {host}:{port} 的备份失败：{_err}。"
                       f"本次未生成可用备份文件，请检查节点连通性、账号权限与磁盘空间。"
                       f"注意：无法恢复的备份等同于没有备份。",
                       cluster_id=int(cluster_id))
        except Exception:
            pass
        r["msg"] = f"备份失败：{_err}"
    # 落盘备份记录（无论成功失败都记，便于审计与「备份记录」列表展示）
    try:
        uname = user.get("username") if isinstance(user, dict) else str(user)
        m = r.get("meta") or {}
        dbs_req = dbs if scope == "custom" else (m.get("databases") or [])
        backup.record_backup(int(cluster_id), {
            "time": backup.now_cn().strftime("%Y-%m-%d %H:%M:%S"),
            "cluster_id": int(cluster_id),
            "cluster_name": backup._cluster_dir_name(cluster_id),
            "source": f"{host.strip()}:{port}",
            "file": m.get("file", "") if r.get("ok") else "",
            "scope": scope,
            "databases": dbs_req,
            "with_data": bool(with_data),
            "ok": bool(r.get("ok")),
            "db_total": len(dbs_req) if dbs_req else (m.get("tables") or 0),
            "tables": m.get("tables", 0),
            "views": m.get("views", 0),
            "rows": m.get("rows", 0),
            "operator": uname,
            "error": r.get("error", "") if not r.get("ok") else "",
        })
    except Exception:
        pass
    # 操作日志（备份页「操作日志」页签按 id=<cid> 过滤）——此前该页签恒为空，
    # 根因是这些埋点从未被调用过。
    try:
        uname = user.get("username") if isinstance(user, dict) else str(user)
        m = r.get("meta") or {}
        backup._log_event(
            f"id={int(cluster_id)} 集群={backup._cluster_dir_name(cluster_id)} 备份 "
            f"节点={host.strip()}:{port} 范围={scope} "
            f"文件={m.get('file', '') or '—'} "
            f"表={m.get('tables', 0)} 行={m.get('rows', 0)} "
            f"大小={round(m.get('size', 0) / 1024, 1)}KB "
            f"操作者={uname} 结果={'成功' if r.get('ok') else '失败'}"
        )
    except Exception:
        pass
    return JSONResponse(r)


@app.get("/api/backup/list")
def api_backup_list(cluster_id: int = 0, user=Depends(auth_required)):
    """列出该集群的历史备份。"""
    try:
        return JSONResponse({"ok": True, "backups": backup.list_backups(int(cluster_id))})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "backups": []})


@app.get("/api/backup/history")
def api_backup_history(cluster_id: int = 0, user=Depends(auth_required)):
    """读取该集群的备份记录（倒序，最新在前）。"""
    try:
        return JSONResponse({"ok": True, "records": backup.list_backup_records(int(cluster_id))})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "records": []})


@app.post("/api/backup/restore")
def api_backup_restore(cluster_id: int = Form(...), host: str = Form(...),
                       port: int = Form(3306), file: str = Form(...),
                       databases: str = Form(""), user=Depends(auth_required)):
    """把备份恢复到指定节点（默认恢复备份内的全部库，可指定只恢复部分库）。

    v2.3.6：恢复改为**后台任务**执行，本接口立即返回 task_id；前端轮询
    /api/backup/restore_progress 展示进度，避免长恢复被误认为卡死。
    恢复记录 / 操作日志 / 审计改由 backup._run_restore_task 在任务结束时落库。
    """
    dbs = [d.strip() for d in (databases or "").split(",") if d.strip()]
    uname = user.get("username") if isinstance(user, dict) else str(user)
    try:
        tid = backup.start_restore_task(int(cluster_id), host.strip(), int(port),
                                        file, databases=dbs or None, operator=uname)
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"启动恢复任务失败：{e}"})
    return JSONResponse({"ok": True, "task_id": tid,
                         "msg": "已开始恢复，请等待进度完成…"})


@app.get("/api/backup/restore_progress")
def api_backup_restore_progress(task_id: str = "", cluster_id: int = 0,
                                user=Depends(auth_required)):
    """查询恢复任务进度。task_id 为空时返回该集群最近的一次恢复任务。"""
    try:
        if task_id:
            t = backup.get_restore_task(task_id)
            if not t:
                return JSONResponse({"ok": False, "error": "任务不存在或已过期"})
            return JSONResponse({"ok": True, "task": t})
        arr = backup.list_restore_tasks(20)
        if cluster_id:
            arr = [t for t in arr
                   if int(t.get("cluster_id") or 0) == int(cluster_id)]
        return JSONResponse({"ok": True, "task": (arr[0] if arr else None)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/api/backup/log")
def api_backup_log(cluster_id: int = 0, limit: int = 200,
                   user=Depends(auth_required)):
    """读取备份/恢复操作日志（倒序）。此前页面直接展示未传值导致页签恒为空。"""
    try:
        return JSONResponse({"ok": True,
                             "log": backup.read_backup_log(cluster_id or None, limit)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "log": []})


@app.get("/api/restore/history")
def api_restore_history(cluster_id: int = 0, user=Depends(auth_required)):
    """读取该集群的恢复记录（倒序，最新在前）。"""
    try:
        return JSONResponse({"ok": True, "records": backup.list_restores(int(cluster_id))})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "records": []})


@app.post("/api/backup/delete")
def api_backup_delete(cluster_id: int = Form(...), file: str = Form(...),
                      user=Depends(auth_required)):
    try:
        return JSONResponse(backup.delete_backup(int(cluster_id), file))
    except Exception as e:
        return JSONResponse({"ok": False, "msg": str(e)})


@app.get("/api/backup/preview")
def api_backup_preview(cluster_id: int = 0, file: str = "",
                       user=Depends(auth_required)):
    try:
        return JSONResponse({"ok": True, "head": backup.read_backup_head(int(cluster_id), file)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/api/backup/download")
def api_backup_download(cluster_id: int = 0, file: str = "",
                        user=Depends(auth_required)):
    """下载备份产物。

    ⚠ 必须同时支持两种形态（v2.3.4 起）：
      - 单文件备份：<name>.sql.gz
      - 分库集备份：<name>/ 目录（全库备份且业务库 >= 2 个时走这种，
        内部每库一个 .sql.gz + index.json，kind="set"）
    旧实现统一 `p.read_bytes()`：对目录必然抛 IsADirectoryError([Errno 21])，
    且该异常在 try 之外 → 直接 500，「备份/恢复」页下载按钮点了没反应
    （生产 1.100 实测：All-Databases_20260929-020834 即目录形态，无法下载）。
    现在：目录 → 打包成 <name>.tar.gz 流式下发；单文件 → 同样改流式，
    避免把整份备份（可能上 GB）一次性读进管理器容器内存。
    """
    try:
        p = backup.get_backup_path(int(cluster_id), file)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})
    try:
        if p.is_dir():
            import tarfile
            import tempfile
            from starlette.background import BackgroundTask
            tmp = tempfile.NamedTemporaryFile(
                prefix="bkdl-", suffix=".tar.gz", delete=False)
            tmp.close()
            try:
                # arcname 用目录名：解压后得到同名目录，与备份目录结构一致
                with tarfile.open(tmp.name, "w:gz") as tf:
                    tf.add(str(p), arcname=p.name)
            except Exception as _te:
                try:
                    os.remove(tmp.name)
                except OSError:
                    pass
                return JSONResponse(
                    {"ok": False, "error": f"打包分库集备份失败：{_te}"})
            return FileResponse(
                tmp.name, media_type="application/gzip",
                filename="%s.tar.gz" % p.name,
                background=BackgroundTask(os.remove, tmp.name))
        return FileResponse(str(p), media_type="application/gzip", filename=p.name)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/api/cred_status")
def api_cred_status(user=Depends(auth_required)):
    """凭证加密状态 + 凭证体检（系统设置「凭证加密」卡片展示用）。"""
    from . import crypto as _crypto
    from .db import credential_health
    d = _crypto.cred_status()
    d["enabled"] = _crypto.enabled()      # cred_status() 恒 True，这里给真实开关值
    d["last_error"] = _crypto.last_error()
    try:
        d["health"] = credential_health()
    except Exception as e:
        d["health"] = {"total": 0, "broken": 0, "items": [], "error": str(e)}
    return JSONResponse(d)


@app.post("/api/cred_encryption")
def api_cred_encryption(action: str = Form(...), user=Depends(auth_required)):
    """凭证加密开关 / 重置密钥（Web「设置 → 凭证加密」）。

    action:
      - ``disable``   关闭加密：可解的一律还原明文（保底，此后零密钥依赖）
      - ``enable``    开启加密：明文重新加密入库
      - ``reset_key`` 重置密钥：先强制全部转明文 → 换新密钥 → 再重新加密

    重要：密钥丢失后**关闭加密无法让已损坏的密文恢复**，只能止血；这类凭证必须在
    「集群设置 / 节点」重新录入。故每个操作都返回体检结果，由前端明确列出损坏项。
    """
    import os as _os
    from . import crypto as _crypto
    from .db import (upsert_setting, migrate_encrypt_credentials,
                     migrate_decrypt_credentials, credential_health)

    action = (action or "").strip().lower()
    if action not in ("enable", "disable", "reset_key"):
        return JSONResponse({"ok": False, "msg": "未知操作"}, status_code=400)

    if action == "disable":
        upsert_setting("CRED_ENCRYPTION", "off")
        conv = migrate_decrypt_credentials()
        h = credential_health()
        return JSONResponse({
            "ok": True, "health": h,
            "msg": f"已关闭凭证加密：{conv} 条凭证还原为明文，此后不再依赖任何密钥。"
                   + (f"但仍有 {h.get('broken', 0)} 条解不开，必须重新录入。"
                      if h.get("broken") else "")})

    if action == "enable":
        upsert_setting("CRED_ENCRYPTION", "on")
        n = migrate_encrypt_credentials()
        h = credential_health()
        return JSONResponse({
            "ok": True, "health": h,
            "msg": f"已开启凭证加密：{n} 条明文凭证已加密入库。"
                   + (f"注意仍有 {h.get('broken', 0)} 条此前已损坏、需重新录入。"
                      if h.get("broken") else "")})

    # reset_key：先把「当前还能解开的」全部转明文保住，再换新密钥重新加密。
    # 顺序不可颠倒 —— 直接换密钥会让原本可解的凭证变成永久死数据。
    conv = migrate_decrypt_credentials(force=True)
    try:
        p = _crypto._key_path()
        if _os.path.exists(p):
            _os.remove(p)
    except Exception as e:
        print(f"[api] 删除旧凭证密钥失败(继续): {e}")
    _crypto._fernet = None                # 清缓存，下次取密钥时按规则重建
    upsert_setting("CRED_ENCRYPTION", "on")
    n = migrate_encrypt_credentials()
    h = credential_health()
    return JSONResponse({
        "ok": True, "health": h,
        "msg": f"已重置凭证密钥：{conv} 条转明文、{n} 条用新密钥重新加密。"
               + (f"仍有 {h.get('broken', 0)} 条解不开，必须重新录入。"
                  if h.get("broken") else "")})


@app.post("/api/cred_recover")
def api_cred_recover(user=Depends(auth_required)):
    """从节点生成包取回明文口令并回填（依赖文件，不连节点 DB）。

    重装/重建管理器导致管理库凭证密文解不开、复制中断时的「一键恢复」：节点包里含
    节点 MySQL 当前实际使用的明文口令，回填后即可让复制恢复，无需旧密钥、无需重设节点。
    """
    from .db import recover_creds_from_node_packages
    try:
        return JSONResponse(recover_creds_from_node_packages())
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"从节点包回填失败: {e}"},
                            status_code=500)


@app.post("/api/cred_reset_repl")
def api_cred_reset_repl(user=Depends(auth_required)):
    """批量重置复制口令：生成新 repl_pass 落库，并在每个节点重置复制账号密码、对从库以新口令重挂。

    ⚠ 依赖 manager 能连上节点（ha/root 口令须可用）；root 也解不开时请改用「从节点包回填明文」。
    """
    from .db import reset_repl_password
    from . import clusters as _clusters
    try:
        applied, skipped = [], []
        any_ok = False
        for c in _clusters.list_clusters():
            r = reset_repl_password(c["id"])
            any_ok = any_ok or r["ok"]
            applied.extend(r["applied"])
            skipped.extend(r["skipped"])
        msg = "已批量重置复制口令：%d 项成功" % len(applied)
        if skipped:
            msg += "，%d 项跳过" % len(skipped)
        return JSONResponse({"ok": any_ok, "msg": msg, "applied": applied, "skipped": skipped})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"批量重置复制口令失败: {e}"},
                            status_code=500)


@app.post("/api/setting_set")
def api_setting_set(key: str = Form(...), value: str = Form(...),
                    user=Depends(auth_required)):
    """保存一个开关/配置项（如 AUTO_FAILOVER / AUTO_REPAIR）。"""
    allowed = {"AUTO_FAILOVER", "AUTO_REPAIR", "ALERT_COOLDOWN_SEC",
               "ALERT_MERGE", "ALERT_LAG_THRESHOLD", "ALERT_RECEIVERS",
               "SLOWLOG_ENABLED",
               # 高延迟 / 压力自动切换（默认关闭）
               "LAG_FAILOVER_ENABLED", "LAG_FAILOVER_SEC", "LAG_FAILOVER_CYCLES",
               "PRESSURE_FAILOVER_ENABLED", "PRESSURE_THREADS_RUNNING",
               "PRESSURE_ROW_LOCK_WAITS", "FAILOVER_COOLDOWN_SEC",
               # 自愈 / 半同步 / binlog 守卫（默认保守，页面与 /api/setting_set 共用白名单）
               "SKIP_DATA_CONFLICT", "REQUIRE_WITNESS_FOR_FAILOVER",
               "AUTO_PURGE_BINLOG", "BINLOG_SIZE_ALERT_MB",
               "BINLOG_KEEP_HOURS", "SEMI_SYNC",
               # 部署期等待「预设主库」上线的时长（秒，默认 1200=20 分钟）
               "PROVISION_WAIT_SEC",
               # 告警自愈观察期（秒）与「binlog 已清理时自动重建」（默认关闭）
               "ALERT_GRACE_SEC", "AUTO_REBUILD_ON_PURGE"}
    if key not in allowed:
        return JSONResponse({"ok": False, "error": "不允许修改该项"})
    upsert_setting(key, value)
    audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                    "setting_set", key, value, None)
    return JSONResponse({"ok": True, "msg": "已保存"})


def _jsonable(v):
    from datetime import datetime as _dt, date as _d, time as _t, timedelta as _td
    from decimal import Decimal as _Dec
    if isinstance(v, (bytes,)):
        return v.decode("utf-8", "replace")
    if isinstance(v, (_dt, _d, _t, _td)):
        return v.isoformat()
    if isinstance(v, _Dec):
        return str(v)
    return v
_READONLY_RE = re.compile(r"^\s*(SELECT|SHOW|EXPLAIN|DESCRIBE|DESC|WITH|USE|HELP)\b", re.IGNORECASE)


# 错误日志行首时间戳。要同时兼容三种写法：
#   MySQL 8.0 : 2026-09-11T09:15:32.123456+08:00  /  2026-09-11T09:15:32.123456
#   MariaDB   : 2026-09-11  9:15:32（空格分隔，小时用空格补齐，可能 1~2 位）
#   MariaDB 老: 260911  9:15:32（YYMMDD）
_ERR_TS_RE = re.compile(
    r'^('
    r'\d{4}-\d{2}-\d{2}[ T]\s?\d{1,2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:?\d{2})?'
    r'|\d{6}\s+\d{1,2}:\d{2}:\d{2}'
    r')'
)
_ERR_TS_ISO_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2})[ T]\s?(\d{1,2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}:?\d{2})?$')
_ERR_LV_RE = re.compile(r'\[([A-Za-z]+)\]')
_CN_TZ = timezone(timedelta(hours=8))        # Asia/Shanghai（中国无夏令时，固定 +08:00）


def _err_ts_to_cn(t):
    """把错误日志行里的时间戳规整为界面统一格式（Asia/Shanghai 口径）。

    时区处理差异（重要，MariaDB 与 MySQL 不同）：
      - MySQL 8.0：log_timestamps 默认 UTC，文件里是 `2026-09-11T09:15:32.123`
        （T 分隔、无时区标记）→ 按 UTC 解析并换算到 +08:00，否则面板时间会早 8 小时。
      - MariaDB：错误日志时间戳是**本地时间**且用空格分隔（`2026-09-11  9:15:32`，
        小时可能空格补齐）→ 不做时区换算，只规整显示，否则时间会多 8 小时。
      - 带显式时区偏移（如 `+08:00`）→ 按偏移解析后再换算。
    """
    if not t:
        return t
    try:
        s0 = str(t).strip()
        # MariaDB 老格式 YYMMDD HH:MM:SS（本地时间）
        m6 = re.match(r'^(\d{2})(\d{2})(\d{2})\s+(\d{1,2}:\d{2}:\d{2})', s0)
        if m6:
            return "20%s-%s-%s %s" % (m6.group(1), m6.group(2), m6.group(3),
                                      m6.group(4).zfill(8))
        m = _ERR_TS_ISO_RE.match(s0)
        if not m:
            return t
        date_s = m.group(1)
        time_s = m.group(2).zfill(8)          # "9:15:32" -> "09:15:32"
        frac = m.group(3) or ""
        off = m.group(4) or ""
        vals = "%s %s%s" % (date_s, time_s, frac)
        fmt = "%Y-%m-%d %H:%M:%S" + (".%f" if frac else "")
        if off:
            tz = timezone.utc
            try:
                tz = datetime.strptime(off, "%z").tzinfo or timezone.utc
            except ValueError:
                pass
            dt = datetime.strptime(vals, fmt).replace(tzinfo=tz).astimezone(_CN_TZ)
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        if "T" in s0[:11]:                    # MySQL：T 分隔 + 朴素时间 => UTC
            dt = datetime.strptime(vals, fmt).replace(tzinfo=timezone.utc)
            return dt.astimezone(_CN_TZ).strftime("%Y-%m-%dT%H:%M:%S.%f")
        return vals                           # MariaDB：本地时间，原样展示
    except Exception:
        return t


def _parse_err_line(line):
    """解析节点原始错误日志行，尽量抽出 (time, level, message)。"""
    t = ""
    m = _ERR_TS_RE.match(line)
    if m:
        t = m.group(1)
    rest = line[m.end():].lstrip() if m else line
    lv = ""
    lm = _ERR_LV_RE.search(rest)
    if lm:
        lv = lm.group(1).upper()
        msg = rest[lm.end():].lstrip()
    else:
        msg = rest
    return t, lv, msg


# 「启动段」边界：一次实例启动从 BEGIN 标记开始，到 ready for connections 结束。
# 为什么需要它：面板的「启动日志」标签若只是「取最新 N 行再按关键字过滤启动字样」，
# 实例稳态运行一段时间后，最新 N 行里根本不含启动行 → 该标签恒为空（表现为"启动日志没有获取到"）。
# 正确语义是「最近一次启动的完整片段」，故在更大读取窗口内定位最后一次启动边界后整段返回。
_STARTUP_BEGIN_RE = re.compile(
    r'Starting\s+(?:MariaDB|MySQL|mariadbd|mysqld)'      # MariaDB/MySQL 容器启动首行
    r'|mysqld_safe\s+Starting'                            # 裸机 MariaDB（mysqld_safe 包装）
    r'|starting\s+as\s+process',                          # MySQL 8.0 启动首行
    re.I)
_STARTUP_END_RE = re.compile(r'ready\s+for\s+connections', re.I)


def _extract_last_startup(rows, cap=400):
    """从（最新在前）错误日志行里抽取「最近一次启动段」。

    返回按时间正序排列的启动段（从 Starting… 到 ready for connections），便于像看引导日志一样阅读。
    读取窗口内找不到启动标记时返回 []（说明近期日志里确实没有启动记录）。
    """
    if not rows:
        return []
    chron = list(reversed(rows))          # 转时间正序，便于「取最后一次」
    start = None
    for i, r in enumerate(chron):
        if _STARTUP_BEGIN_RE.search(r.get("message") or ""):
            start = i                     # 持续覆盖 → 取最后一次启动
    if start is None:
        return []
    seg = []
    for r in chron[start:start + int(cap)]:
        seg.append(r)
        if _STARTUP_END_RE.search(r.get("message") or ""):
            break
    return seg


@app.post("/api/node_error_log")
def api_node_error_log(cluster_id: int = Form(...), host: str = Form(...), port: int = Form(...),
                       limit: int = Form(500), segment: str = Form(""), user=Depends(auth_required)):
    """读取节点错误日志（文件 LOAD_FILE 为主，MySQL 8.0 额外支持错误日志表）。

    一级（仅 MySQL 8.0）：错误日志表 mysql.error_log（log_sink_table 组件写入，
    纯 SELECT，容器/裸机通用，无需 FILE 权限，不受 secure_file_priv 限制）。
    二级（MySQL 8.0 / MariaDB 通用）：原始错误日志文件（LOAD_FILE @@log_error）。
    注意：MariaDB 不支持错误日志写表（仅 general/slow 日志可写表），故 MariaDB
    节点必须落盘到文件并放开 secure_file_priv，面板才能读到（见 node_gen 配置）。

    segment="startup" 时返回「最近一次启动段」（先按更大窗口读取，再定位最后一次启动边界整段返回），
    解决「启动日志」标签只过滤最新 N 行导致实例稳态运行时该标签恒空的问题。
    """
    want_startup = str(segment or "").strip().lower() == "startup"
    _flimit = 2000 if want_startup else int(limit)
    try:
        port = int(port)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "端口无效"})
    node = query("SELECT root_user, root_pass FROM nodes WHERE cluster_id=%s AND ip=%s AND port=%s",
                 (cluster_id, host, port))
    ru = (node[0]["root_user"] if node else "root") or "root"
    rp = node[0]["root_pass"] if node else ""
    try:
        conn = connect(host, port, ru, rp, timeout=5)
    except Exception as e:
        return JSONResponse({"ok": False, "error": "无法连接节点: " + str(e)})
    try:
        cur = conn.cursor()
        rows = []
        source = None
        # 一级：错误日志表 mysql.error_log（容器/裸机通用，纯 SELECT，不受 secure_file_priv 限制）
        try:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.columns "
                "WHERE table_schema='mysql' AND table_name='error_log'"
            )
            cols = {r[0].lower() for r in cur.fetchall()}
            if "time" in cols and "message" in cols:
                level_col = next((c for c in ("prio", "level", "priority") if c in cols), None)
                sel = ("time, %s, message" % level_col) if level_col else "time, message"
                cur.execute(
                    "SELECT %s FROM mysql.error_log ORDER BY time DESC LIMIT %%s" % sel,
                    (int(_flimit),),
                )
                for r in cur.fetchall():
                    t = r[0]; lv = r[1] if level_col else ""; msg = r[-1]
                    rows.append({"time": (str(t) if t is not None else ""),
                                 "level": (lv or "").upper(),
                                 "message": msg or ""})
                if rows:
                    source = "table"
        except Exception:
            rows = []
            source = None
        # 二级：原始错误日志文件（LOAD_FILE），兼容未开启表日志的节点
        path = ""
        _load_err = ""
        if not rows:
            # 错误日志时间戳的落盘时区：MySQL 默认 log_timestamps=UTC，
            # 直接展示会比 Asia/Shanghai 早 8 小时，需按实际值换算。
            _log_ts_utc = False
            try:
                cur.execute("SELECT @@log_timestamps")
                _lt = (cur.fetchone() or ("",))[0] or ""
                _log_ts_utc = str(_lt).strip().upper().startswith("UTC")
            except Exception:
                # v2.3.5：MariaDB **没有** log_timestamps 变量（MySQL 专属），
                # 上面必然抛异常。MariaDB 的错误日志按 system_time_zone 落盘：
                # 容器已设 TZ=Asia/Shanghai，通常为本地时间；但若宿主时区是 UTC，
                # 落盘就是 UTC，展示会整整差 8 小时——必须换算，不能静默不处理。
                try:
                    cur.execute("SELECT @@system_time_zone")
                    _stz = (cur.fetchone() or ("",))[0] or ""
                    _log_ts_utc = str(_stz).strip().upper() in ("UTC", "GMT", "UTC0")
                except Exception:
                    pass
            cur.execute("SELECT @@log_error")
            path = (cur.fetchone() or ("",))[0] or ""
            # 相对路径按数据目录解析（部分部署 @@log_error 仅返回文件名）
            if path and not path.startswith("/"):
                try:
                    cur.execute("SELECT @@datadir")
                    dd = (cur.fetchone() or ("",))[0] or ""
                    if dd:
                        path = (dd.rstrip("/") + "/" + path)
                except Exception:
                    pass
            if path:
                try:
                    cur.execute("SELECT LOAD_FILE(%s)", (path,))
                    blob = (cur.fetchone() or ("",))[0] or ""
                    if isinstance(blob, (bytes, bytearray)):
                        blob = bytes(blob).decode("utf-8", "replace")
                    if blob:
                        lines = [l for l in str(blob).split("\n") if l.strip()]
                        lines = lines[-int(_flimit):]
                        for ln in reversed(lines):
                            t, lv, msg = _parse_err_line(ln)
                            rows.append({"time": _err_ts_to_cn(t) if _log_ts_utc else t,
                                         "level": lv, "message": msg})
                        source = "file"
                except Exception as e2:
                    # 无 FILE 权限 / secure_file_priv 限制：不再直接返回失败，
                    # 交给三级（节点 agent）兜底，避免 MariaDB 面板永远是空的。
                    _load_err = str(e2)
        # 三级（v2.3.6）：节点 agent 直读宿主机日志文件——MariaDB 的根治通道。
        #   根因：MariaDB **没有** mysql.error_log 表，只能靠 LOAD_FILE 读 @@log_error
        #   指向的文件；但 mysqld 生成的 error.log 权限是 0660，LOAD_FILE 对 0660 文件
        #   恒返回 NULL（实测 660→NULL / 644→有内容），另外还受 secure_file_priv 限制。
        #   custos-node-agent 以 root 直接读宿主机文件（绕开 FILE 权限、secure_file_priv
        #   与 0660 权限），只要节点上 agent 在跑就能拿到错误日志（启动段同源同文件）。
        if not rows:
            try:
                _ag = os_metrics.fetch_node_log(host, name="error", lines=int(_flimit))
                _ag_lines = list(_ag.get("lines") or [])
                if _ag.get("ok") and _ag_lines:
                    for ln in reversed(_ag_lines):
                        t, lv, msg = _parse_err_line(ln)
                        rows.append({"time": _err_ts_to_cn(t) if _log_ts_utc else t,
                                     "level": lv, "message": msg})
                    source = "agent"
                    if _ag.get("path"):
                        path = _ag["path"]
                elif not path and _ag.get("path"):
                    path = _ag["path"]
                else:
                    _load_err = _load_err or (_ag.get("error") or "")
            except Exception as e3:
                _load_err = _load_err or str(e3)
        # 「启动段」：在大窗口内定位【最近一次】启动边界后整段返回（时间正序，便于阅读）。
        if rows and want_startup:
            rows = _extract_last_startup(rows)
            if not rows:
                # 日志通道本身可读，只是窗口内没有启动记录（实例近期未重启 / 日志已轮转）。
                return JSONResponse({
                    "ok": True, "source": source or "none", "rows": [], "path": path,
                    "segment": "startup",
                    "note": ("日志通道可读，但当前读取窗口内未找到「启动段」"
                             "（未匹配到 Starting… / ready for connections）。"
                             "通常是实例近期未重启、启动记录已远在窗口之外，或错误日志已轮转；"
                             "可先切到「错误日志」标签确认通道是否正常。")})
        if not rows:
            # v2.3.5：把「为什么读不到」讲清楚。三种形态根因各不相同：
            #   MySQL 8.0 容器  -> 有 mysql.error_log 表（log_error_services 含 log_sink_internal）
            #   MariaDB 裸机/容器 -> **没有**该表，只能 LOAD_FILE 读 log_error 指向的文件；
            #                       secure_file_priv 非空 或 log_error 未配置 → 恒为空
            _sfp, _ler = "", path
            try:
                cur.execute("SELECT @@secure_file_priv")
                _sfp = (cur.fetchone() or ("",))[0] or ""
            except Exception:
                pass
            if not _ler:
                try:
                    cur.execute("SELECT @@log_error")
                    _ler = (cur.fetchone() or ("",))[0] or ""
                except Exception:
                    pass
            _why = []
            if _sfp:
                _why.append("secure_file_priv=%s（非空时 LOAD_FILE 只能读取该目录）" % _sfp)
            if not _ler:
                _why.append("log_error 未配置（官方镜像默认写 stdout，磁盘上根本没有错误日志文件）")
            if not _why:
                _why.append("文件为空，或账号缺少 FILE 权限")
            if _load_err:
                _why.append("节点 agent 读取也失败：%s" % str(_load_err)[:160])
            else:
                _why.append("节点 agent 未返回内容（agent 未运行，或仍是旧版本、"
                            "需重建/重启 custos-node-agent 才能读取日志）")
            note = ("未开启错误日志表（MariaDB 无 mysql.error_log，该表仅 MySQL 8.0 支持），"
                    "也未能读到错误日志文件：" + "；".join(_why) + "。"
                    " 三级读取顺序：错误日志表 → LOAD_FILE → 节点 agent（直读宿主机文件）。"
                    " 修复：确保节点 my.cnf / docker-my.cnf 已设 log_error=<统一日志目录>/error.log，"
                    "并让 custos-node-agent 运行（它会以 root 直读宿主机日志，"
                    "不受 FILE 权限、secure_file_priv 与 0660 权限限制）。")
            return JSONResponse({"ok": True, "source": source or "none", "rows": [],
                                 "path": path, "segment": ("startup" if want_startup else "err"),
                                 "note": note})
        return JSONResponse({"ok": True, "source": source, "rows": rows,
                             "segment": ("startup" if want_startup else "err")})
    finally:
        conn.close()


# 终端「当前库」按节点持久（跨请求）：每条 SQL 新建连接，use 不持久，故按节点记录当前库，
# 非 USE 语句执行前先在同连接上重发 USE <当前库>，使多请求 `use db` 后 `show tables` 生效。
_TERM_DB = {}
_TERM_DB_LOCK = threading.Lock()


@app.post("/api/term")
def api_term(cluster_id: int = Form(...), host: str = Form(...), port: int = Form(...),
             sql: str = Form(...), user=Depends(auth_required)):
    """SQL 终端：优先用节点 root 账号执行（可读写，支持全部 SQL）；
    若节点未配置 root 密码，则回退到 ha_monitor 只读账号。"""
    s = (sql or "").strip()
    if not s:
        return JSONResponse({"ok": False, "error": "请输入 SQL"})
    try:
        port = int(port)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "端口无效"})
    # 跨请求保持「当前库」：每条请求新建连接，use 不持久，故按节点记录当前库，
    # 非 USE 语句执行前先在同连接上重发 USE <当前库>，使多请求 `use db` 后 `show tables` 生效。
    tkey = (int(cluster_id), str(host), int(port))
    _m = re.match(r"\s*(\w+)", s)
    first_kw = _m.group(1).upper() if _m else ""
    is_use = first_kw == "USE"
    apply_db = None
    record_db = None
    if is_use:
        _dbm = re.match(r"\s*USE\s+([^\s;]+)", s, re.IGNORECASE)
        if _dbm:
            record_db = _dbm.group(1).strip().strip("`").strip("'").strip('"')
    else:
        with _TERM_DB_LOCK:
            apply_db = _TERM_DB.get(tkey)
    # 先取节点 root 凭证（可读写）
    node = query("SELECT root_user, root_pass FROM nodes "
                 "WHERE cluster_id=%s AND ip=%s AND port=%s", (cluster_id, host, port))
    ru = (node[0]["root_user"] if node else "root") or "root"
    rp = node[0]["root_pass"] if node else ""
    account = "root"
    try:
        conn = connect(host, port, ru, rp, timeout=5)
    except Exception as e:
        # 回退到 ha_monitor 只读账号
        creds = get_cluster_creds(cluster_id) or {}
        hu = creds.get("ha_user")
        hp = creds.get("ha_pass")
        if not hu:
            return JSONResponse({"ok": False,
                                  "error": "无法连接节点(无 root 密码且缺 ha_monitor 凭证): " + str(e)})
        conn = connect(host, port, hu, hp, timeout=5)
        account = "ha_monitor(只读)"
    # 权限类错误码：账号能连上但实际没有权限（如 root@'<manager ip>' 仅 USAGE，
    # 执行 SHOW REPLICA STATUS 会报 1227）。此时回退 ha_monitor 重试一次。
    _PRIV_ERRNOS = frozenset({1227, 1142, 1044, 1045, 1141, 1370})

    def _errno_of(exc):
        args = getattr(exc, "args", None)
        if isinstance(args, (list, tuple)) and len(args) >= 1 and isinstance(args[0], int):
            return args[0]
        return None

    def _err_resp(exc, acct, dur):
        return JSONResponse({"ok": False, "account": acct, "error": str(exc),
                             "error_code": _errno_of(exc),
                             "sqlstate": getattr(exc, "sqlstate", None) or "HY000",
                             "duration": dur})

    def _run(c, acct, apply_db=None, record_db=None):
        t0 = _time.time()
        cur = c.cursor()
        if apply_db:
            try:
                cur.execute("USE `{}`".format(apply_db.replace("`", "")))
            except Exception:
                pass
        cur.execute(sql)
        dur = round(_time.time() - t0, 3)
        if cur.description:
            cols = [d[0] for d in cur.description]
            rows = [[_jsonable(v) for v in r] for r in cur.fetchall()]
            if record_db is not None:
                with _TERM_DB_LOCK:
                    _TERM_DB[tkey] = record_db
            return JSONResponse({"ok": True, "account": acct, "cols": cols,
                                 "rows": rows, "count": len(rows), "duration": dur})
        c.commit()
        if record_db is not None:
            with _TERM_DB_LOCK:
                _TERM_DB[tkey] = record_db
        affected = cur.rowcount if cur.rowcount and cur.rowcount >= 0 else 0
        return JSONResponse({"ok": True, "account": acct, "cols": [], "rows": [],
                             "count": affected, "info": "Query OK", "duration": dur})

    t_start = _time.time()
    try:
        return _run(conn, account, apply_db=apply_db, record_db=record_db)
    except Exception as e:
        if account == "root" and _errno_of(e) in _PRIV_ERRNOS:
            creds = get_cluster_creds(cluster_id) or {}
            hu, hp = creds.get("ha_user"), creds.get("ha_pass")
            if hu:
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = connect(host, port, hu, hp, timeout=5)
                    account = "ha_monitor(只读)"
                except Exception as e2:
                    return _err_resp(e2, "ha_monitor(只读)",
                                     round(_time.time() - t_start, 3))
                try:
                    return _run(conn, account, apply_db=apply_db, record_db=record_db)
                except Exception as e3:
                    return _err_resp(e3, account, round(_time.time() - t_start, 3))
        return _err_resp(e, account, round(_time.time() - t_start, 3))
    finally:
        try:
            conn.close()
        except Exception:
            pass


SQL_KEYWORDS = [
    "SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE", "CALL", "DO", "HANDLER",
    "SET", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "USE", "CREATE", "ALTER", "DROP", "TRUNCATE",
    "RENAME", "GRANT", "REVOKE", "FLUSH", "PURGE", "RESET", "START", "STOP", "RESTART",
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "LOCK", "UNLOCK", "ANALYZE", "OPTIMIZE",
    "REPAIR", "CHECK", "BACKUP", "RESTORE", "LOAD", "INSTALL", "UNINSTALL", "CACHE",
    "CHANGE", "KILL", "BINLOG", "TABLE", "DATABASE", "SCHEMA", "VIEW", "INDEX", "TRIGGER",
    "PROCEDURE", "FUNCTION", "EVENT", "USER", "ROLE", "PRIVILEGE", "FROM", "WHERE", "GROUP",
    "ORDER", "BY", "HAVING", "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
    "ON", "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "AS",
    "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "INTO", "VALUES", "DUAL", "WITH",
]


@app.post("/api/term_complete")
def api_term_complete(cluster_id: int = Form(...), host: str = Form(...), port: int = Form(...),
                      user=Depends(auth_required)):
    """Tab 补全候选：关键字 + 库 + 表 + 列。"""
    try:
        port = int(port)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "端口无效"})
    node = query("SELECT root_user, root_pass FROM nodes "
                 "WHERE cluster_id=%s AND ip=%s AND port=%s", (cluster_id, host, port))
    ru = (node[0]["root_user"] if node else "root") or "root"
    rp = node[0]["root_pass"] if node else ""
    account = "root"
    try:
        conn = connect(host, port, ru, rp, timeout=5)
    except Exception:
        creds = get_cluster_creds(cluster_id) or {}
        hu = creds.get("ha_user")
        hp = creds.get("ha_pass")
        if not hu:
            return JSONResponse({"ok": True, "completions": list(SQL_KEYWORDS)})
        conn = connect(host, port, hu, hp, timeout=5)
        account = "ha_monitor"
    cands = list(SQL_KEYWORDS)
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT schema_name FROM information_schema.schemata")
            cands += [r[0] for r in cur.fetchall()]
        except Exception:
            pass
        try:
            cur.execute("SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema NOT IN ('information_schema','performance_schema','mysql','sys','test') "
                        "LIMIT 400")
            cands += [r[0] for r in cur.fetchall()]
        except Exception:
            pass
        try:
            cur.execute("SELECT DISTINCT column_name FROM information_schema.columns "
                        "WHERE table_schema NOT IN ('information_schema','performance_schema','mysql','sys','test') "
                        "LIMIT 600")
            cands += [r[0] for r in cur.fetchall()]
        except Exception:
            pass
    finally:
        conn.close()
    # 去重保序
    seen, uniq = set(), []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            uniq.append(c)
    return JSONResponse({"ok": True, "account": account, "completions": uniq})


@app.get("/alerts")
def alerts_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    """告警设置：SMTP 状态、告警规则、收件人、当前告警集中管理。"""
    # 用 smtp_view()：与发信口径(_cfg)一致，DB 优先 + 环境变量回退。
    # 原写法只读 DB，导致「用环境变量配 SMTP」时邮件能发、页面却显示未配置。
    cfg = smtp_view()
    alert_emails = _load_alert_emails()
    clusters = [{"id": c["id"], "name": c.get("name") or f"集群{c['id']}"} for c in list_clusters()]
    rules = {
        "cooldown": get_setting("ALERT_COOLDOWN_SEC", "300"),
        "merge": get_setting("ALERT_MERGE", "1"),
        "lag_threshold": get_setting("ALERT_LAG_THRESHOLD", "30"),
        "receivers": get_setting("ALERT_RECEIVERS", ""),
        "grace": get_setting("ALERT_GRACE_SEC", "30"),
    }
    # 邮件通道故障常驻提示：投递真失败过就在页面顶部显示，避免「告警静默丢失」无人知晓。
    try:
        from .alerts import channel_error as _channel_error
        chan_err = _channel_error()
    except Exception:
        chan_err = None
    return render("alerts.html", request, user=user, cfg=cfg, alert_emails=alert_emails,
                  clusters=clusters, rules=rules, msg=msg, error=error, chan_err=chan_err)


@app.get("/api/active_alerts")
def api_active_alerts(user=Depends(auth_required)):
    """聚合当前未恢复告警：探活失败(hosts.err)、配置漂移(config_drift)、复制延迟超阈值(metric_history)。"""
    cname = {c["id"]: (c.get("name") or f"集群{c['id']}") for c in list_clusters()}

    def enrich(a):
        a["cluster_name"] = cname.get(a["cluster_id"], f"集群{a['cluster_id']}")
        # 自愈状态覆盖层：若该节点近期有成功的自愈记录，标记为「已自愈」，否则「未恢复」
        try:
            hr = query("SELECT status, message FROM alerts WHERE cluster_id=%s AND host=%s AND port=%s "
                       "ORDER BY id DESC LIMIT 1", (a["cluster_id"], a["host"], int(a["port"])))
            if hr and hr[0]["status"] == "healed":
                a["status"] = "healed"
                a["heal_msg"] = hr[0]["message"]
            else:
                a["status"] = "open"
        except Exception:
            a["status"] = "open"
        return a

    alerts = []
    for h in query("SELECT cluster_id, host, port, err FROM hosts WHERE err IS NOT NULL AND err <> ''"):
        alerts.append(enrich({"cluster_id": h["cluster_id"], "host": h["host"], "port": h["port"],
                       "type": "探活失败", "level": "error", "msg": h["err"], "heal": True}))
    for d in query(
        "SELECT d.cluster_id, d.host, d.port, d.var_name, d.expected, d.actual FROM config_drift d "
        "INNER JOIN (SELECT cluster_id, host, var_name, MAX(id) AS mx FROM config_drift WHERE status='drift' "
        "GROUP BY cluster_id, host, var_name) t ON t.mx=d.id WHERE d.status='drift'"):
        alerts.append(enrich({"cluster_id": d["cluster_id"], "host": d["host"], "port": d["port"],
                       "type": "配置漂移", "level": "warn",
                       "msg": f"{d['var_name']} 期望 {d['expected']} 实际 {d['actual']}", "heal": False}))
    lag = int(get_setting("ALERT_LAG_THRESHOLD", "30") or 30)
    try:
        for r in query(
            "SELECT m.cluster_id, m.host, m.port, m.seconds_behind FROM metric_history m "
            "INNER JOIN (SELECT cluster_id, host, port, MAX(id) AS mx FROM metric_history "
            "GROUP BY cluster_id, host, port) t ON t.cluster_id=m.cluster_id AND t.host=m.host "
            "AND t.port=m.port AND t.mx=m.id WHERE m.seconds_behind > %s", (lag,)):
            alerts.append(enrich({"cluster_id": r["cluster_id"], "host": r["host"], "port": r["port"],
                           "type": "复制延迟", "level": "warn",
                           "msg": f"延迟 {r['seconds_behind']}s > 阈值 {lag}s", "heal": True}))
    except Exception:
        pass
    # 严重(error)优先，其次按集群号
    alerts.sort(key=lambda a: (0 if a["level"] == "error" else 1, a["cluster_id"]))
    return JSONResponse({"ok": True, "alerts": alerts})


@app.get("/api/users")
def api_users(user=Depends(auth_required)):
    """用户列表（含角色）。"""
    return _json_ok(list_users())


@app.post("/api/users/add")
def api_user_add(username: str = Form(...), password: str = Form(...),
                 role: str = Form(ROLE_ADMIN), user=Depends(auth_required)):
    """新增用户。role=viewer 为只读账号（任何写操作会被 auth_required 拦截）。"""
    username = (username or "").strip()
    if not username or not password:
        return _json_ok({"ok": False, "error": "用户名和密码均不能为空"})
    role = ROLE_VIEWER if (role or "").strip().lower() == ROLE_VIEWER else ROLE_ADMIN
    if get_user(username):
        return _json_ok({"ok": False, "error": f"用户 {username} 已存在"})
    try:
        create_user(username, hash_pwd(password), role)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"新增失败：{e}"})
    return _json_ok({"msg": f"已新增用户 {username}（{'只读' if role == ROLE_VIEWER else '管理员'}）"})


@app.post("/api/users/role")
def api_user_role(username: str = Form(...), role: str = Form(...),
                  user=Depends(auth_required)):
    """修改用户角色。禁止把最后一个管理员降级为只读。"""
    username = (username or "").strip()
    role = ROLE_VIEWER if (role or "").strip().lower() == ROLE_VIEWER else ROLE_ADMIN
    u = get_user(username)
    if not u:
        return _json_ok({"ok": False, "error": f"用户 {username} 不存在"})
    if role == ROLE_VIEWER and (u.get("role") or ROLE_ADMIN) == ROLE_ADMIN \
            and count_admins() <= 1:
        return _json_ok({"ok": False, "error": "至少需保留一个管理员，无法将该账号降级为只读"})
    try:
        set_user_role(username, role)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"修改失败：{e}"})
    return _json_ok({"msg": f"{username} 已设为{'只读' if role == ROLE_VIEWER else '管理员'}"})


@app.post("/api/users/delete")
def api_user_delete(username: str = Form(...), user=Depends(auth_required)):
    """删除用户。禁止删除自己、禁止删光最后一个管理员。"""
    username = (username or "").strip()
    if username == user:
        return _json_ok({"ok": False, "error": "不能删除当前登录账号"})
    u = get_user(username)
    if not u:
        return _json_ok({"ok": False, "error": f"用户 {username} 不存在"})
    if (u.get("role") or ROLE_ADMIN) == ROLE_ADMIN and count_admins() <= 1:
        return _json_ok({"ok": False, "error": "至少需保留一个管理员"})
    try:
        delete_user(username)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"删除失败：{e}"})
    return _json_ok({"msg": f"已删除用户 {username}"})


@app.post("/api/alert_rules")
def api_alert_rules(cooldown: str = Form(""), merge: str = Form(""),
                    lag_threshold: str = Form(""), receivers: str = Form(""),
                    grace: str = Form(""), user=Depends(auth_required)):
    """保存告警规则（冷却/合并/延迟阈值/额外收件人/自愈观察期）。"""
    upsert_setting("ALERT_COOLDOWN_SEC", cooldown)
    upsert_setting("ALERT_MERGE", merge)
    upsert_setting("ALERT_LAG_THRESHOLD", lag_threshold)
    upsert_setting("ALERT_RECEIVERS", receivers)
    if grace.strip():
        upsert_setting("ALERT_GRACE_SEC", grace.strip())
    return JSONResponse({"ok": True, "msg": "✓ 已保存告警规则"})


@app.post("/api/node_detail")
def api_node_detail(cluster_id: int = Form(...), host: str = Form(...), port: int = Form(...),
                    user=Depends(auth_required)):
    """单节点详情：实时连节点取运行时变量（server_id/datadir/gtid/read_only）
    + 账号清单（节点级 root 本地密码 + 集群级 ha/repl/app）。供集群页「详情」抽屉使用。"""
    try:
        port = int(port)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "端口无效"})
    node = query(
        "SELECT root_user, root_pass FROM nodes WHERE cluster_id=%s AND ip=%s AND port=%s",
        (cluster_id, host, port))
    root_user = (node[0]["root_user"] if node else "root") or "root"
    root_pass = node[0]["root_pass"] if node else ""
    _cp = query("SELECT compose_path FROM nodes WHERE cluster_id=%s AND ip=%s AND port=%s",
                (cluster_id, host, port))
    compose_path = (_cp[0]["compose_path"] if _cp and _cp[0].get("compose_path") else "")
    creds = {}
    try:
        creds = get_cluster_creds(cluster_id) or {}
    except Exception:
        creds = {}
    out = {
        "ok": True, "host": host, "port": port, "cluster_id": cluster_id,
        "root_user": root_user, "root_pass": root_pass, "compose_path": compose_path,
        "ha_user": creds.get("ha_user"), "ha_pass": creds.get("ha_pass"),
        "repl_user": creds.get("repl_user"), "repl_pass": creds.get("repl_pass"),
        "app_user": creds.get("app_user"), "app_pass": creds.get("app_pass"),
        "server_id": None, "datadir": None, "gtid_mode": None,
        "read_only": None, "enforce_gtid_consistency": None,
        "gtid_strict_mode": None, "gtid_domain_id": None,
    }
    try:
        # 优先 ha_monitor、回退 root：此前只拿 root 连，节点 root 密码一旦漂移（典型
        # root@'<管理器IP>' 不存在/密码不符 → 1045）就整体连不上，抽屉显示「无法连接节点」，
        # 且下面 GTID 因取不到值被显示成 OFF，而实际主从是正常的 —— 完全是误报。
        conn = connect_robust(host, port,
                              creds.get("ha_user") or "", creds.get("ha_pass") or "",
                              root_user, root_pass, timeout=5)
        if conn is None:
            # 两条路都失败时，单独用 root 再连一次，把真实错误（如 1045）抛出来给用户看
            conn = connect(host, port, root_user, root_pass, timeout=5)
        try:
            with conn.cursor() as cur:
                # MariaDB 没有 gtid_mode 变量（GTID 天然开启，靠 gtid_domain_id 区分域）。
                # 不加区分地只查 gtid_mode，节点页会把 MariaDB 的 GTID 显示成"—/OFF"，
                # 让人误以为 GTID 没开。这里按 flavour 查 MariaDB 的真实语义变量。
                _is_maria = False
                try:
                    _is_maria = (detect_flavour(cur) == F_MARIADB)
                except Exception:
                    pass
                # v2.3.0：采集真实版本并回写（mysql_version 此前靠手填，常年空/过期，
                # 导致预检与权限分派拿不到版本）。同时给出支持矩阵判定供前端展示。
                try:
                    cur.execute("SELECT VERSION()")
                    _vr = cur.fetchone()
                    _ver = (_vr[0] if _vr else "") or ""
                    out["version"] = _ver
                    try:
                        from . import mariadb_matrix as _mm
                        out["version_support"] = _mm.support_check(_ver)
                    except Exception:
                        pass
                    if _ver:
                        execute("UPDATE nodes SET mysql_version=%s WHERE ip=%s AND port=%s "
                                "AND (mysql_version IS NULL OR mysql_version='' "
                                "OR mysql_version<>%s)",
                                (_ver[:48], host, port, _ver[:48]))
                except Exception:
                    pass
                _vars = ("('server_id','datadir','read_only','enforce_gtid_consistency'"
                         + (",'gtid_strict_mode','gtid_domain_id')" if _is_maria else ",'gtid_mode')"))
                cur.execute("SHOW VARIABLES WHERE Variable_name IN " + _vars)
                for name, val in cur.fetchall():
                    if name == "server_id":
                        out["server_id"] = val
                    elif name == "datadir":
                        out["datadir"] = val
                    elif name == "gtid_mode":
                        out["gtid_mode"] = val
                    elif name == "read_only":
                        out["read_only"] = val
                    elif name == "enforce_gtid_consistency":
                        out["enforce_gtid_consistency"] = val
                    elif name == "gtid_strict_mode":
                        out["gtid_strict_mode"] = val
                    elif name == "gtid_domain_id":
                        out["gtid_domain_id"] = val
                if _is_maria:
                    # MariaDB 的 GTID 是否可用 = gtid_domain_id 已配（非 0）
                    out["gtid_mode"] = ("ON" if str(out.get("gtid_domain_id") or "0") != "0"
                                        else "OFF")
        finally:
            conn.close()
    except Exception as e:
        out["conn_error"] = f"{type(e).__name__}: {e}"
    return JSONResponse(out)


@app.get("/api/maintenance")
def api_maintenance(cluster_id: int = 0, user=Depends(auth_required)):
    """列出生效中的维护窗口（v2.3.3）。cluster_id=0 表示全部集群。"""
    try:
        from .maintenance import active_windows
        rows = active_windows(int(cluster_id) if cluster_id else None)
        for r in rows:
            for k in ("started_at", "end_at", "created_at", "ended_at"):
                if r.get(k) is not None:
                    r[k] = str(r[k])
        return JSONResponse({"ok": True, "items": rows})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"}, status_code=500)


@app.post("/api/maintenance/begin")
def api_maintenance_begin(cluster_id: int = Form(...), scope: str = Form("cluster"),
                          target: str = Form(""), reason: str = Form(""),
                          minutes: int = Form(0), user=Depends(auth_required)):
    """开启维护窗口：期间抑制自动切换与状态类告警（v2.3.3）。

    minutes>0 表示到期自动结束；0 表示不限时，必须手动结束。
    """
    try:
        from .maintenance import begin
        _who = user.get("username") if isinstance(user, dict) else str(user)
        begin(int(cluster_id), scope, target, reason, minutes, created_by=_who)
        try:
            audit.add_audit(_who, "maintenance_begin", f"cluster={cluster_id}",
                            f"scope={scope} target={target or '-'} minutes={minutes} "
                            f"reason={reason}", cluster_id=int(cluster_id))
        except Exception:
            pass
        tip = "（%s 分钟后自动结束）" % minutes if minutes > 0 else "（不限时，需手动结束）"
        return JSONResponse({"ok": True, "msg": f"✓ 已进入维护模式{tip}"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"})


@app.post("/api/maintenance/end")
def api_maintenance_end(cluster_id: int = Form(...), scope: str = Form(""),
                        target: str = Form(""), user=Depends(auth_required)):
    """结束维护窗口。scope 为空时结束该集群的全部窗口（v2.3.3）。"""
    try:
        from .maintenance import end as _mend
        _who = user.get("username") if isinstance(user, dict) else str(user)
        _mend(int(cluster_id), scope=(scope or None), target=target)
        try:
            audit.add_audit(_who, "maintenance_end", f"cluster={cluster_id}",
                            f"scope={scope or 'all'}", cluster_id=int(cluster_id))
        except Exception:
            pass
        return JSONResponse({"ok": True, "msg": "✓ 已结束维护模式"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"})


# ==================================================================
# 数据迁移向导（既有 MariaDB 集群 → Custos 托管集群）
# 专注 MariaDB：probe/recommend/seed/verify（2.4.0 重做：纯逻辑导出导入，已移除桥接/切换/收尾步骤）。
# 步骤驱动：前端逐步调用，天然支持重试(重复调用)/暂停(不调下一步)。
# ==================================================================
from . import migration as _migration


@app.get("/migration")
def migration_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    """数据迁移向导页。"""
    return render("migration.html", request, user=user, msg=msg, error=error,
                  size_threshold_gb=_migration.SIZE_THRESHOLD_GB)


@app.get("/migration/history")
def migration_history_page(request: Request, user=Depends(auth_required)):
    """历史迁移记录独立页面。"""
    return render("migration_history.html", request, user=user)


async def _json_body(request: Request):
    try:
        return await request.json()
    except Exception:
        return {}


@app.post("/api/migration/probe")
async def api_migration_probe(request: Request, user=Depends(auth_required)):
    b = await _json_body(request)
    try:
        r = _migration.probe_source({
            "src_host": b.get("src_host", ""),
            "src_port": int(b.get("src_port", 3306)),
            "src_user": b.get("src_user", "root"),
            "src_pass": b.get("src_pass", ""),
        })
        return _json_ok(r)
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/migration/recommend")
async def api_migration_recommend(request: Request, user=Depends(auth_required)):
    b = await _json_body(request)
    probe = {"size_mb": float(b.get("size_mb", 0)), "flavor": b.get("flavor") or "mariadb"}
    return _json_ok(_migration.recommend_channel(probe, b.get("channel_pref") or None))


@app.post("/api/migration/probe_target")
async def api_migration_probe_target(request: Request, user=Depends(auth_required)):
    """探测目标 MariaDB：版本 + 账号分层权限（用于"非 root 能用到哪一步"的提示）。"""
    b = await _json_body(request)
    cfg = {
        "tgt_host": b.get("tgt_host", ""),
        "tgt_port": int(b.get("tgt_port", 3306)),
        "tgt_user": b.get("tgt_user") or "root",
        "tgt_root_pass": b.get("tgt_root_pass", ""),
    }
    try:
        conn = connect(cfg["tgt_host"], cfg["tgt_port"],
                       cfg["tgt_user"], cfg["tgt_root_pass"], timeout=8)
        try:
            cur = conn.cursor()
            cur.execute("SELECT VERSION()")
            ver = cur.fetchone()[0]
        finally:
            conn.close()
        try:
            priv = _migration.audit_target_privileges(cfg)
        except Exception:
            priv = {"ok": False}
        return _json_ok({"ok": True, "version": ver, "privileges": priv})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/migration/target_hint")
async def api_migration_target_hint(request: Request, user=Depends(auth_required)):
    """按目标 IP 自动给出端口/形态，省去手填（用户要求：自动化，不要手动点）。

    优先取 nodes 表里该 IP 登记的端口与形态；未登记则回退系统默认 NODE_PORT（3307）。
    """
    b = await _json_body(request)
    ip = (b.get("ip") or "").strip()
    default_port = int(get_setting("NODE_PORT") or 3307)
    if not ip:
        return _json_ok({"ok": True, "known": False, "port": default_port, "node_type": ""})
    try:
        rows = query("SELECT port, node_type FROM nodes WHERE ip=%s", (ip,))
        if rows:
            r = rows[0]
            return _json_ok({"ok": True, "known": True,
                             "port": int(r.get("port") or default_port),
                             "node_type": r.get("node_type") or ""})
    except Exception:
        pass
    return _json_ok({"ok": True, "known": False, "port": default_port, "node_type": ""})


@app.post("/api/migration/create")
async def api_migration_create(request: Request, user=Depends(auth_required)):
    b = await _json_body(request)
    try:
        # BUG-003：节点形态必须显式选择，禁止静默回退为容器默认值
        node_type = b.get("node_type") or ""
        if node_type not in ("mariadb", "mariadb-docker", "docker"):
            return _json_ok({"ok": False,
                             "error": "请选择有效的目标节点形态（容器化 MariaDB / 裸机 MariaDB / 容器化 MySQL）"})
        databases = b.get("databases") or []
        if isinstance(databases, str):
            databases = [d.strip() for d in databases.split(",") if d.strip()]
        repl_user = "replication"
        repl_pass = rand_secret(16)
        cfg = {
            "src_host": b.get("src_host", ""),
            "src_port": int(b.get("src_port", 3306)),
            "src_user": b.get("src_user", "root"),
            "src_pass": b.get("src_pass", ""),
            "src_gtid": bool(b.get("src_gtid", True)),
            "tgt_host": b.get("tgt_host", ""),
            "tgt_port": int(b.get("tgt_port", 3306)),
            "tgt_root_pass": b.get("tgt_root_pass", ""),
            # 目标库账号：默认 root。填非 root 时仅能完成「数据导入」，
            # 桥接/注册节点/切换/收尾需要更高权限（见 audit_target_privileges 分层提示）。
            "tgt_user": b.get("tgt_user") or "root",
            "tgt_version": b.get("tgt_version", ""),
            "node_type": node_type,
            "databases": databases,
            "repl_user": repl_user,
            "repl_pass": repl_pass,
            # 产品线信息：决定 dump 工具与 GTID 参数（两者互斥，混用必失败）
            "src_flavor": b.get("src_flavor") or "mariadb",
            "src_major": int(b.get("src_major") or 0),
            "tgt_flavor": b.get("tgt_flavor") or "mariadb",
            # 数据量：逻辑通道用它做真实百分比进度的分母
            "size_mb": float(b.get("size_mb") or 0),
        }
        task = {
            "name": b.get("name") or "未命名迁移",
            "channel": "logical",  # 2.4.1 起仅通用模式；高级模式（物理）已取消
            "status": "draft",
            "config": cfg,
            "steps": {},
            "log": "",
        }
        _migration.save_task(task)
        return _json_ok({"ok": True, "id": task["id"]})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/migration/{task_id}/step")
async def api_migration_step(task_id: int, request: Request, user=Depends(auth_required)):
    """异步启动一步：立即返回，实际工作在后台线程，前端轮询 /status 看进度。"""
    b = await _json_body(request)
    step = b.get("step", "")
    opts = {"deep": True} if b.get("deep") else None
    return _json_ok(_migration.start_step(task_id, step, opts))


@app.post("/api/migration/{task_id}/cancel")
def api_migration_cancel(task_id: int, user=Depends(auth_required)):
    """中止执行中的迁移任务（置标记，由步骤循环安全退出并释放并发门禁）。

    用途：任务卡死（如进度长时间 0%）时给运维一个出口，避免并发门禁被永久占用。
    """
    try:
        task = _migration.get_task(task_id)
        if not task:
            return _json_ok({"ok": False, "error": "任务不存在"})
        return _json_ok(_migration.request_cancel(task_id))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/migration/{task_id}/precheck")
async def api_migration_precheck(task_id: int, user=Depends(auth_required)):
    """迁移前兼容性预检（dry-run，不搬数据）。"""
    try:
        return _json_ok(_migration.precheck(task_id))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/migration/{task_id}/heal")
def api_migration_heal(task_id: int, user=Depends(auth_required)):
    """迁移目标的复制自愈：桥接阶段目标(从库)的 SQL/IO 线程因复制错误(如 1133/冲突)停止时，
    用目标 root 账号调用 heal_replication 跳过冲突事务 / 补建缺失账号 / 重启线程，使其恢复追平。

    与集群「修复复制」(POST /api/heal) 不同：迁移目标此刻尚未登记进 Custos 集群，
    无法走集群自愈；这里直接用迁移任务记录里的目标地址与 root 口令连接目标节点。
    """
    try:
        task = _migration.get_task(task_id)
        if not task:
            return _json_ok({"ok": False, "error": "任务不存在"})
        cfg = task["config"]
        host = cfg["tgt_host"]; port = int(cfg["tgt_port"])
        root_pass = cfg.get("tgt_root_pass") or ""
        if not root_pass:
            return _json_ok({"ok": False,
                             "error": "任务未记录目标 root 口令，无法连接目标自愈；"
                                      "请到目标节点手动 START REPLICA / 处理复制错误。"})
        ok, msg = heal_replication(host, port, "root", root_pass,
                                  root_user="root", root_pass=root_pass)
        return _json_ok({"ok": ok, "msg": msg})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.post("/api/migration/privileges")
async def api_migration_privileges(request: Request, user=Depends(auth_required)):
    """源库账号权限体检：列出迁移各步骤依赖的权限是否具备、缺了影响哪一步。"""
    try:
        b = await _json_body(request)
        return _json_ok(_migration.audit_source_privileges(b))
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/migration/{task_id}/status")
def api_migration_status(task_id: int, user=Depends(auth_required)):
    task = _migration.get_task(task_id)
    if not task:
        return _json_ok({"ok": False, "error": "任务不存在"})
    return _json_ok({"ok": True, **task})


@app.get("/api/migration/list")
def api_migration_list(user=Depends(auth_required)):
    return _json_ok({"ok": True, "tasks": _migration.list_tasks()})


@app.delete("/api/migration/{task_id}")
def api_migration_delete(task_id: int, user=Depends(auth_required)):
    """删除迁移任务（执行中的任务仅移除记录，不停止后台步骤）。"""
    return _json_ok(_migration.delete_task(task_id))


@app.post("/api/migration/cleanup-stale")
def api_migration_cleanup_stale(user=Depends(auth_required)):
    """清理卡死/陈旧的执行中任务（无后台线程在跑的 running 记录）。"""
    return _json_ok(_migration.cleanup_stale_running_tasks())


@app.post("/api/migration/unlock")
async def api_migration_unlock(request: Request, user=Depends(auth_required)):
    """强制解除某节点的迁移锁（及可选清理指向它的 running 任务）。

    用于节点被重装/重建后，管理后台仍残留「迁移中」状态的场景。等价于对管理库
    执行 DELETE FROM migration_locks / migration_tasks，但无需手敲 SQL。默认同时
    清理指向该节点的 running 任务（重装后这些任务已无意义）。

    body: {"host": "192.0.2.203", "port": 3307, "clear_tasks": true}
    """
    b = await _json_body(request)
    host = (b.get("host") or "").strip()
    port = int(b.get("port") or 0)
    clear_tasks = b.get("clear_tasks", True)
    if not host or not port:
        return _json_ok({"ok": False, "error": "host 与 port 必填"})
    try:
        info = _migration.force_clear_node(host, port, clear_tasks=bool(clear_tasks))
        return _json_ok({"ok": True, **info})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


# ==================================================================
@app.get("/api/alerts")
def api_alerts(
    level: str = "", status: str = "", cluster_id: int = 0,
    hours: int = 0, q: str = "", page: int = 1, page_size: int = 20,
    user=Depends(auth_required),
):
    """告警中心：按级别/确认状态/集群/时间/关键字筛选，分页返回。"""
    where, params = [], []
    if cluster_id:
        where.append("a.cluster_id=%s"); params.append(cluster_id)
    if status == "unread":
        where.append("a.acked=0")
    elif status == "acked":
        where.append("a.acked=1")
    if hours and hours > 0:
        where.append("a.created_at>=DATE_SUB(NOW(), INTERVAL %s HOUR)"); params.append(hours)
    if q:
        where.append("(a.message LIKE %s OR a.host LIKE %s OR a.alert_type LIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    level_case = (
        "CASE WHEN a.alert_type IN ('down','replication','ha_fail') THEN 'error'"
        " WHEN a.alert_type='lag' THEN 'warn'"
        " WHEN a.alert_type='recover' THEN 'info'"
        " WHEN a.alert_type='selfheal' AND a.status='unresolved' THEN 'warn'"
        " WHEN a.alert_type='selfheal' THEN 'info'"
        " ELSE 'warn' END"
    )
    if level in ("error", "warn", "info"):
        where.append(f"{level_case}=%s"); params.append(level)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    total = query(f"SELECT COUNT(*) AS n FROM alerts a{where_sql}", params)[0]["n"]
    off = max(0, (page - 1) * page_size)
    rows = query(
        f"SELECT a.id, a.cluster_id, a.host, a.port, a.alert_type, a.status, a.message, "
        f"a.acked, a.created_at, {level_case} AS level "
        f"FROM alerts a{where_sql} ORDER BY a.created_at DESC LIMIT %s OFFSET %s",
        params + [page_size, off],
    )
    cname = {c["id"]: (c.get("name") or f"集群{c['id']}") for c in list_clusters()}
    for r in rows:
        r["cluster_name"] = cname.get(r["cluster_id"], f"集群{r['cluster_id']}")
    return _json_ok({"ok": True, "alerts": rows, "total": total,
                     "page": page, "page_size": page_size})


@app.post("/api/alert_ack")
async def api_alert_ack(request: Request, user=Depends(auth_required)):
    """确认告警：单条(id)、批量(ids)或全部(all)。"""
    try:
        data = await request.json()
    except Exception:
        data = {}
    # 注意：写操作必须用 execute()（内部 commit），query() 不提交事务
    if bool(data.get("all")):
        execute("UPDATE alerts SET acked=1 WHERE acked=0")
    elif data.get("ids"):
        id_list = [int(x) for x in data["ids"] if str(x).isdigit()]
        if id_list:
            ph = ",".join(["%s"] * len(id_list))
            execute(f"UPDATE alerts SET acked=1 WHERE id IN ({ph})", id_list)
    elif data.get("id"):
        try:
            execute("UPDATE alerts SET acked=1 WHERE id=%s", (int(data["id"]),))
        except Exception as e:
            print(f"[alert_ack] 确认失败: {e}")
    return _json_ok({"ok": True})


@app.get("/api/switch_history")
def api_switch_history(
    cluster_id: int = 0, hours: int = 0, page: int = 1, page_size: int = 20,
    user=Depends(auth_required),
):
    """切换历史：按集群/时间筛选，分页返回。"""
    where, params = [], []
    if cluster_id:
        where.append("h.cluster_id=%s"); params.append(cluster_id)
    if hours and hours > 0:
        where.append("h.created_at>=DATE_SUB(NOW(), INTERVAL %s HOUR)"); params.append(hours)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    total = query(f"SELECT COUNT(*) AS n FROM switch_history h{where_sql}", params)[0]["n"]
    off = max(0, (page - 1) * page_size)
    rows = query(
        "SELECT h.id, h.cluster_id, h.from_host, h.from_port, h.to_host, h.to_port, "
        f"h.reason, h.created_at FROM switch_history h{where_sql} "
        "ORDER BY h.id DESC LIMIT %s OFFSET %s", params + [page_size, off])
    cname = {c["id"]: (c.get("name") or f"集群{c['id']}") for c in list_clusters()}
    for r in rows:
        r["cluster_name"] = cname.get(r["cluster_id"], f"集群{r['cluster_id']}")
    return _json_ok({"ok": True, "history": rows, "total": total,
                     "page": page, "page_size": page_size})


@app.post("/clusters")
def clusters_create(
    request: Request, name: str = Form(...),
    vip: str = Form(""), vip_prefix: str = Form("24"),
    interface: str = Form(""), vrid: str = Form(""),
    vip_port: str = Form(""),
    node_type: str = Form("docker"), mysql_version: str = Form(""),
    ha_user: str = Form(""), ha_pass: str = Form(""),
    repl_user: str = Form(""), repl_pass: str = Form(""),
    app_user: str = Form(""), app_pass: str = Form(""),
    user=Depends(auth_required),
):
    name = name.strip()
    if not name:
        return RedirectResponse("/clusters?error=集群名称不能为空", status_code=303)
    # 解析 VIP 端口
    vip_port_val = None
    if vip_port.strip():
        try:
            vip_port_val = int(vip_port.strip())
            if not (1 <= vip_port_val <= 65535):
                raise ValueError()
        except ValueError:
            return RedirectResponse(
                "/clusters?error=vip端口必须是 1-65535 之间的整数", status_code=303)
    node_type = node_type.strip()
    if node_type not in ("docker", "mariadb", "mariadb-docker"):
        return RedirectResponse(
            "/clusters?error=节点类型(node_type)非法或为空，请从 docker / mariadb / mariadb-docker 中选择",
            status_code=303)
    try:
        new_id = create_cluster(
            name,
            ha_user=ha_user.strip() or None,
            ha_pass=ha_pass.strip() or None,
            repl_user=repl_user.strip() or None,
            repl_pass=repl_pass.strip() or None,
            app_user=app_user.strip() or None,
            app_pass=app_pass.strip() or None,
            vip=vip.strip() or None,
            vip_prefix=vip_prefix.strip() or None,
            interface=interface.strip() or None,
            vrid=vrid.strip() or None,
            vip_port=vip_port_val,
            node_type=node_type,
            mysql_version=mysql_version.strip() or None,
        )
    except Exception as e:
        return RedirectResponse(f"/clusters?error={e}", status_code=303)
    audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                    "create_cluster", f"cluster={new_id}", name, new_id)
    # 创建成功后直达该集群详情页（新路由结构：总览 → 单集群详情）。
    # 自动生成的账号密码可在详情页「⚙ 修改配置/密码」中查看，或展开节点详情查看。
    return RedirectResponse(
        f"/clusters/{new_id}?msg={quote('已创建集群「' + name + '」；自动生成的账号密码可在「⚙ 修改配置/密码」中查看，或展开节点详情查看')}",
        status_code=303)


@app.post("/clusters/{cid}")
def cluster_update(
    cid: int, request: Request,
    name: str = Form(""), vip: str = Form(""), vip_prefix: str = Form(""),
    interface: str = Form(""), vrid: str = Form(""),
    vip_port: str = Form(""),
    node_type: str = Form(""), mysql_version: str = Form(""),
    ha_user: str = Form(""), ha_pass: str = Form(""),
    repl_user: str = Form(""), repl_pass: str = Form(""),
    app_user: str = Form(""), app_pass: str = Form(""),
    ha_regen: bool = Form(False), repl_regen: bool = Form(False), app_regen: bool = Form(False),
    user=Depends(auth_required),
):
    from .config import rand_secret
    from .clusters import get_cluster_creds
    fields = {}
    if name.strip():
        fields["name"] = name.strip()
    if vip.strip():
        fields["vip"] = vip.strip()
    if vip_prefix.strip():
        fields["vip_prefix"] = vip_prefix.strip()
    if interface.strip():
        fields["interface"] = interface.strip()
    if vrid.strip():
        fields["vrid"] = vrid.strip()
    if ha_user.strip():
        fields["ha_user"] = ha_user.strip()
    if ha_regen:
        fields["ha_pass"] = rand_secret(16)
    elif ha_pass.strip():
        fields["ha_pass"] = ha_pass.strip()
    if repl_user.strip():
        fields["repl_user"] = repl_user.strip()
    if repl_regen:
        fields["repl_pass"] = rand_secret(16)
    elif repl_pass.strip():
        fields["repl_pass"] = repl_pass.strip()
    if app_user.strip():
        fields["app_user"] = app_user.strip()
    if app_regen:
        fields["app_pass"] = rand_secret(16)
    elif app_pass.strip():
        fields["app_pass"] = app_pass.strip()
    if node_type.strip():
        fields["node_type"] = node_type.strip()
    if mysql_version.strip():
        fields["mysql_version"] = mysql_version.strip()
    # VIP 端口：合法则纳入更新（节点 compose 需随之重渲染）
    if vip_port.strip():
        try:
            vp = int(vip_port.strip())
            if not (1 <= vp <= 65535):
                raise ValueError()
            fields["vip_port"] = vp
        except ValueError:
            return RedirectResponse(
                f"/clusters/{cid}?error={quote('vip端口必须是 1-65535 之间的整数')}",
                status_code=303)
    old_creds = get_cluster_creds(cid)
    try:
        update_cluster(cid, **fields)
    except Exception as e:
        return RedirectResponse(
            f"/clusters/{cid}?error={quote('保存失败: ' + str(e))}", status_code=303)
    synced = 0
    # 账号【用户名】变更同样必须同步到节点：此前只判断密码字段，导致「只改用户名」时
    # 管理库改了、节点上仍是旧账号 —— 表现为新用户名登不上、页面显示与节点实际不一致。
    if any(k in fields for k in ("ha_pass", "repl_pass", "app_pass",
                                 "ha_user", "repl_user", "app_user")):
        try:
            synced = sync_node_creds(cid,
                                     old_ha_user=old_creds.get("ha_user"),
                                     old_repl_user=old_creds.get("repl_user"),
                                     old_repl_pass=old_creds.get("repl_pass"),
                                     old_app_user=old_creds.get("app_user"))
        except Exception:
            synced = 0
    msg = "已保存集群配置"
    if synced:
        msg += f"；已把新密码同步到 {synced} 个在线节点（到各节点执行 docker compose restart keepalived 使 ha_check.conf 生效）"
    # 重新生成该集群各节点的部署文件：补拷缺失的 notify 脚本、刷新 docker-compose
    #（含 VIP 端口映射）、并重渲染 keepalived.conf（同步最新 VIP/网卡/VRID/unicast_peer）。
    # 修复此前改 VIP 只刷 compose、漏刷 keepalived.conf，导致节点配置 virtual_ipaddress 为空、
    # keepalived 报 “No VIP specified”，VIP 永远不上浮。
    try:
        from .node_gen import regenerate_cluster_compose
        regenerate_cluster_compose(cid)
        msg += ("；已刷新各节点部署文件（keepalived.conf + notify 脚本 + docker-compose），"
                "请把各节点目录（默认 /home/docker/custos-node）下 keepalived/keepalived.conf "
                "同步到目标机并重载 keepalived 生效"
                "（如：scp keepalived/keepalived.conf 目标机:/home/docker/custos-node/keepalived/ 后 "
                "docker compose restart keepalived）")
    except Exception:
        pass
    return RedirectResponse(f"/clusters/{cid}?msg={quote(msg)}", status_code=303)


@app.post("/clusters/{cid}/delete")
def cluster_delete(cid: int, request: Request, user=Depends(auth_required)):
    try:
        delete_cluster(cid)
        return JSONResponse({"ok": True, "msg": "✓ 集群已删除"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 删除失败：{e}"})


@app.post("/clusters/{cid}/sync")
def cluster_sync(cid: int, user=Depends(auth_required)):
    """把集群最新密码同步到所有在线节点。

    注意：sync_node_creds 返回的是「已同步节点数(int)」，不是 (ok, detail) 元组，
    直接解包会抛 ValueError 导致 500，此处按 int 处理。
    """
    try:
        synced = sync_node_creds(cid)
        if synced:
            return JSONResponse({
                "ok": True,
                "msg": f"✓ 已将密码同步到 {synced} 个在线节点（请到各节点执行 "
                       f"docker compose restart keepalived 使 ha_check.conf 生效）"})
        return JSONResponse(
            {"ok": False,
             "msg": "✗ 无在线节点可同步（确认节点已启动且管理端可连通）"},
            status_code=200)
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 同步失败：{e}"}, status_code=200)


# ---------------- 节点一键部署 ----------------
# Web 展示时剥离的「宿主机部署目录前缀」，按历史命名顺序兼容：
#   mysql-ha-manager/（v2.1 及更早） → custos-ha/（当前） → custos/
# 「custos-ha/」必须排在「custos/」之前，否则会被截断成 "-ha/generated_nodes/<ip>"。
_DIR_PREFIXES = ("mysql-ha-manager/", "custos-ha/", "custos/")

# 节点形态 → 中文标签（与 nodes.html 节点行徽标口径保持一致）
_NODE_TYPE_LABEL = {
    "docker": "Docker",
    "mariadb": "MariaDB 裸机",
    "mariadb-docker": "MariaDB 容器",
}


def _node_type_summary(nodes):
    """按集群内【实际节点类型】汇总展示文案（如 "MariaDB 裸机×1 · MariaDB 容器×1"）。

    混部集群（裸机 MariaDB + 容器 MariaDB，同 MariaDB 引擎族允许混部）下，
    集群级 node_type 只是建集群时的默认值 / 加节点时的族校验基准，并非各节点真实形态。
    组头若直接展示集群级 node_type，会让用户误以为集群内所有节点都是该形态。
    """
    from collections import Counter
    cnt = Counter((n.get("node_type") or "mariadb-docker") for n in nodes)
    return " · ".join("%s×%d" % (_NODE_TYPE_LABEL.get(t, t), c) for t, c in cnt.most_common())


@app.get("/nodes")
def nodes_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = "",
               cluster_id: int = 0):
    nodes = list_nodes(int(cluster_id) if cluster_id else None)
    clusters = list_clusters()
    ac = int(cluster_id) if cluster_id else None
    creds = load_creds(ac)
    cfg = dict(
        app_user=creds.get("app_user") or get_setting("APP_USER") or "app",
        app_pass=creds.get("app_pass") or "",
        vip=creds.get("vip") or "",
        port=get_setting("NODE_PORT") or "3307",
    )
    # 附加集群名称与「去掉部署目录前缀」的展示（历史/当前前缀清单见 _DIR_PREFIXES）
    name_by_id = {c["id"]: c.get("name") or f"集群{c['id']}" for c in clusters}
    for n in nodes:
        n["cluster_name"] = name_by_id.get(n.get("cluster_id")) or "—"
        cp = n.get("compose_path") or ""
        dp = cp
        for _p in _DIR_PREFIXES:
            dp = dp.split(_p, 1)[-1]
        n["dir_display"] = dp or cp

    # 徽标显示**真实待办条数**（已剔除 notice 说明项）：让运维一眼看出「还剩几项、
    # 做完就没了」，而不是看到一个数字永远不动的红标，误以为系统卡死/功能 bug。
    try:
        from .node_gen import is_optional_action as _is_optional
    except Exception:
        _is_optional = None
    for n in nodes:
        try:
            _acts = json.loads(n.get("pending_actions") or "[]")
        except Exception:
            _acts = []
        if not isinstance(_acts, list):
            _acts = []
        if _is_optional is not None:
            _acts = [a for a in _acts if not _is_optional(a)]
        n["pending_count"] = len(_acts)

    # 按集群分组：节点管理页按「一个集群一张卡片」渲染，避免多集群混在同一张表里
    by_cid = {}
    for n in nodes:
        by_cid.setdefault(n.get("cluster_id"), []).append(n)
    groups = []
    for c in clusters:
        cid = c["id"]
        if ac and cid != ac:      # 指定了集群筛选时只显示该集群
            continue
        members = by_cid.get(cid, [])
        groups.append({
            "id": cid,
            "name": c.get("name") or f"集群{cid}",
            "node_type": c.get("node_type") or "mariadb-docker",
            "type_summary": _node_type_summary(members),
            "vip": c.get("vip") or "",
            "nodes": members,
        })
    # 尚未归属任何集群的节点（历史数据）单独归到「未分配」
    orphans = by_cid.get(None, [])
    if orphans and not ac:
        groups.append({"id": 0, "name": "未分配集群", "node_type": "",
                       "type_summary": _node_type_summary(orphans), "vip": "", "nodes": orphans})

    # 表单里「建议的下一个 server_id」提示值（真正分配在提交时按库实时计算并校验）
    try:
        _next_sid = get_next_server_id()
    except Exception:
        _next_sid = ""
    return render("nodes.html", request, user=user, nodes=nodes, clusters=clusters,
                  groups=groups, creds=creds, cfg=cfg, active_cluster=ac, msg=msg, error=error,
                  next_server_id=_next_sid)


@app.get("/audit")
def audit_page(request: Request, user=Depends(auth_required), limit: int = 200):
    rows = audit.list_audit(int(limit), 0, None)
    return render("audit.html", request, user=user, rows=rows, limit=int(limit))


@app.post("/nodes/add")
def nodes_add(
    request: Request,
    ip: str = Form(...),
    port: str = Form("3307"),
    root_user: str = Form("root"),
    root_pass: str = Form(...),
    initial_role: str = Form("master"),
    cluster_id: int = Form(0),
    node_type: str = Form("docker"),
    mysql_version: str = Form(""),
    server_id: str = Form(""),
    install_dir: str = Form(""),
    interface: str = Form(""),
    user=Depends(auth_required),
):
    initial_role = "master" if initial_role == "master" else "slave"
    ip = (ip or "").strip()
    # 节点安装目录：仅接受绝对路径，否则忽略回退默认（与引导命令 install_dir 规则一致）
    install_dir = (install_dir or "").strip()
    if install_dir and not install_dir.startswith("/"):
        install_dir = ""
    # 防护：管理端后台 MySQL（MGMT_MYSQL_HOST）仅管理器自身存储使用，绝不能作为集群节点；
    # 否则会把「管理库地址」误当节点 IP 写入集群配置（违反「1.88 的 MySQL 只负责服务端管理后台，
    # 不参与集群」）。仅当管理库为外部真实 IP 时才拦截，回环/auto 不拦截（便于单机开发）。
    # 比对按「host:port」精确匹配——同机不同端口的节点（如 1.88:3308）仍允许添加，
    # 只有「管理库本体」（如 1.88:3306）才被拦截。
    try:
        _mgmt = get_mgmt_host()
        _mgmt_port = str(MGMT.get("port", 3306))
        if (_mgmt and _mgmt not in ("127.0.0.1", "localhost", "auto")
                and ip == _mgmt and str(port) == _mgmt_port):
            return RedirectResponse(
                "/nodes?error=该地址是管理后台数据库（MGMT_MYSQL_HOST），不参与集群，不能作为节点添加",
                status_code=303)
    except Exception:
        pass
    # 节点必须归属已有集群（新建集群仅在「集群管理」页面完成）
    cid = int(cluster_id) if cluster_id else None
    if not cid:
        return RedirectResponse("/nodes?error=请先在「集群管理」创建集群，再选择所属集群", status_code=303)
    # 节点类型必须显式合法，禁止静默回退默认（BUG-003：曾静默回退 mariadb-docker）
    node_type = node_type.strip()
    if node_type not in ("docker", "mariadb", "mariadb-docker"):
        return RedirectResponse(
            "/nodes?error=节点类型(node_type)非法或为空，请从 docker / mariadb / mariadb-docker 中选择",
            status_code=303)
    # 版本跟随：页面未填版本时按集群登记版本补齐（旧主 10.6/11.4 → 新从库同版本），
    # 否则「不传版本」会把校验绕过去、生成 12.3.3 的跨大版本节点。
    if not (mysql_version or "").strip():
        try:
            from .clusters import get_cluster as _get_cluster
            mysql_version = ((_get_cluster(int(cid)) or {}).get("mysql_version") or "").strip()
        except Exception:
            pass
    # 同集群「同构」校验：禁止 docker/mariadb 混部、主版本须一致
    try:
        validate_node_version(cid, node_type, mysql_version.strip() or None)
    except Exception as e:
        return RedirectResponse(f"/nodes?error={e}", status_code=303)
    try:
        info = generate_node(
            ip, port, root_user, root_pass, initial_role, cid,
            node_type=node_type, mysql_version=mysql_version.strip() or None,
            server_id=server_id.strip() or None,
            install_dir=install_dir or None,
        )
        # 节点级 interface（v2.5.0）：页面未指定时，缺省沿用集群登记的网卡；
        # 多网卡主机混部的集群，每个节点网卡可能不同，需逐节点设置才能正确上浮 VIP。
        _iface = (interface or "").strip()
        if not _iface:
            try:
                from .clusters import get_cluster as _gc
                _iface = ((_gc(int(cid)) or {}).get("interface") or "").strip()
            except Exception:
                _iface = ""
        if _iface:
            try:
                execute("UPDATE nodes SET interface=%s WHERE ip=%s AND port=%s",
                        (_iface, ip, int(port)))
            except Exception:
                pass
        audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                        "add_node", f"{ip}:{port}",
                        f"type={node_type} ver={mysql_version}", cluster_id=cid)
    except Exception as e:
        # 只把 str(e) 回给用户，服务端必须同时留下完整堆栈：否则像
        # "unsupported operand type(s) for /: 'str' and 'str'" 这种报错（2026-10-02 实测：
        # 新建集群后首次添加节点偶发，节点其实已建成、页面却显示失败）根本无从定位。
        try:
            logger = logging.getLogger("custos.api")
            logger.exception("nodes_add 失败(ip=%s port=%s type=%s ver=%s): %s",
                             ip, port, node_type, mysql_version, e)
        except Exception:
            pass
        return RedirectResponse(f"/nodes?error={e}", status_code=303)
    role_tip = "（已设为初始主库）" if initial_role == "master" else ""
    # 安装说明精简为两种通用方式（curl 引导安装 / 手动上传节点包），不再按类型堆砌长文。
    msg = (f"已生成节点 {info['node_name']}{role_tip}。"
           "安装方式二选一：① 点该节点「引导安装」复制一行命令，到目标机以 root 执行即可自动完成部署；"
           "② 或把生成的节点目录传到目标机，按类型手动部署"
           "（裸机跑 Install 脚本 / 容器跑 docker compose up -d）。")
    return RedirectResponse(
        f"/nodes?msg={msg}",
        status_code=303,
    )


@app.post("/nodes/delete")
def nodes_delete(ip: str = Form(...), user=Depends(auth_required)):
    try:
        delete_node(ip)
        # 同步删除项目目录下已生成的同名部署单元（generated_nodes/<ip>）
        deleted = delete_node_dir(ip)
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"删除失败：{e}"})
    extra = f"，并清理项目目录 {deleted} 个文件" if deleted else ""
    return JSONResponse({"ok": True, "msg": "已删除节点" + extra})


@app.post("/nodes/{ip}/regen")
def nodes_regen(ip: str, user=Depends(auth_required)):
    """用节点已保存的参数重新生成配置文件（覆盖原文件），并返回生成文件的 md5 便于核对。"""
    try:
        row = query(
            "SELECT n.ip, n.port, n.root_user, n.root_pass, n.cluster_id, h.initial_role, "
            "n.node_type, n.mysql_version, n.compose_path "
            "FROM nodes n LEFT JOIN hosts h ON h.host=n.ip AND h.cluster_id=n.cluster_id "
            "WHERE n.ip=%s", (ip,)
        )
        if not row:
            return JSONResponse({"ok": False, "msg": f"未找到节点 {ip} 的生成记录"})
        n = row[0]
        # 节点可能尚未探活登记 initial_role，缺省为 slave
        initial_role = n.get("initial_role") or "slave"
        # 必须沿用节点已保存的 node_type / mysql_version，否则会按默认 docker 重建，
        # 把裸机节点错误地重生成成「MySQL 容器 + keepalived」合一部署（覆盖掉裸机专属文件）。
        # 同理必须沿用【自定义安装目录】：此前 regen 不传 install_dir，generate_node 走
        # node_install_dest(None) 退回默认 /home/docker/custos-node 并写回 nodes.compose_path，
        # 把运维填的 /opt/custos-node 之类自定义目录静默重置（节点包内嵌路径一并错位）。
        # node_install_dest 幂等：传入已是 .../custos-node 的完整路径会原样返回，
        # 不会拼成 .../custos-node/custos-node。
        _saved_dir = (n.get("compose_path") or "").strip()
        info = generate_node(
            n["ip"], n["port"], n["root_user"], n["root_pass"],
            initial_role, int(n["cluster_id"] or 0),
            node_type=n.get("node_type") or "mariadb-docker",
            mysql_version=n.get("mysql_version"),
            install_dir=_saved_dir or None,
        )
        # 计算生成目录内各文件的 md5，便于核对
        import hashlib
        from .config import NODES_DIR as _NODES_DIR
        base = Path(_NODES_DIR) / ip
        md5s = []
        if base.exists():
            for f in sorted(base.rglob("*")):
                if f.is_file():
                    h = hashlib.md5(f.read_bytes()).hexdigest()
                    md5s.append({"file": str(f.relative_to(base)), "md5": h})
        return JSONResponse({
            "ok": True,
            "msg": f"已重新生成节点 {info['node_name']}",
            "run_cmd": info.get("run_cmd"),
            "md5s": md5s,
            # v2.3.0：变更生效动作清单（recreate / restart / exec / manual）
            "actions": info.get("actions") or [],
            "changed_files": info.get("changed_files") or [],
        })
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"重新生成失败: {e}"})


def _render_node_fix_sql(n, creds, manager_ip, manager_subnet=None):
    """按节点真实风味（MySQL / MariaDB）生成 fix-node-creds.sql。

    管理后台需同时支持 MySQL 与 MariaDB 集群，而两者账号/权限语法**不兼容**：
      - MariaDB：用 `VIA ... USING PASSWORD()` 形式（native 插件 + 明文再哈希），
        特权为 REPLICATION SLAVE ADMIN / SLAVE MONITOR / READ_ONLY ADMIN
      - MySQL 8 ：用 `WITH <plugin> BY` 形式，
        特权为 REPLICATION_SLAVE_ADMIN / SYSTEM_VARIABLES_ADMIN
    用错会直接 1064（语法错）或 1227（特权名不存在），故必须按 flavour 选渲染器。

    优先连库用 `SELECT VERSION()` 实时探测；节点不可达（恰恰是最需要本脚本的场景）
    时回退到库里记录的 node_type。
    """
    ha_user = creds.get("ha_user") or "ha_monitor"
    ha_pass = creds.get("ha_pass") or ""
    repl_user = creds.get("repl_user") or "replication"
    repl_pass = creds.get("repl_pass") or ""
    root_user = n.get("root_user") or "root"
    root_pass = n.get("root_pass") or ""
    host = n.get("ip") or n.get("host") or ""
    try:
        port = int(n.get("port") or 0) or 3306
    except (TypeError, ValueError):
        port = 3306

    flavour = None
    if host and ha_pass:
        try:
            conn = connect(host, port, ha_user, ha_pass, timeout=3)
            try:
                flavour = detect_flavour(conn.cursor())
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            flavour = None
    if flavour is None:
        flavour = flavour_from_node_type(n.get("node_type") or "mariadb-docker")

    if flavour == F_MARIADB:
        return render_fix_node_creds_sql_mariadb(
            ha_user, ha_pass, repl_user, repl_pass, root_user, root_pass,
            manager_ip, port, manager_subnet)
    return render_fix_node_creds_sql(
        ha_user, ha_pass, repl_user, repl_pass, root_user, root_pass,
        manager_ip, manager_subnet,
        auth_plugin=_auth_plugin_for(n.get("mysql_version"), n.get("node_type")))


@app.get("/api/nodes/{ip}/pending")
def api_node_pending(ip: str, user=Depends(auth_required)):
    """查询节点当前的「配置待生效」动作清单（无待办返回空数组）。

    v2.3.0：regen / 改配置后，页面据此提示运维「去节点上做什么」，
    并区分「重启即可 / 必须重建容器 / 要执行 SQL」。
    """
    try:
        from .node_gen import node_pending_actions
        return JSONResponse({"ok": True, "actions": node_pending_actions(ip)})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"}, status_code=500)


@app.get("/api/nodes/{ip}/pending/done")
def api_node_pending_done(ip: str, token: str = "", key: str = ""):
    """节点执行完动作后回传「已执行」——免登录，凭一次性 token 精确销账**这一条**。

    供节点侧 curl 调用（命令由待办清单的 ack_cmd 现签）：脚本跑完粘一条，该条待办
    即从「配置待生效」消失，不必回到页面点「已执行」，也不会让徽标长期挂死。
    token 绑定 ip+key，换机器/换条目都验签失败，泄露也只能清这一条。
    """
    try:
        from .node_gen import verify_pending_ack_token, ack_pending_action
        if not key or not verify_pending_ack_token(ip, key, token):
            return JSONResponse({"ok": False, "msg": "token 无效或已过期，请在管理端重新获取回传命令"},
                                status_code=403)
        r = ack_pending_action(ip, key) or {}
        if not r.get("found"):
            return JSONResponse({"ok": True, "msg": "该节点已无此待办（可能已销账）"})
        return JSONResponse({"ok": True, "cleared": r.get("cleared") or [],
                             "msg": "已销账：%s" % "、".join(r.get("cleared") or [])})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"}, status_code=500)


@app.post("/nodes/{ip}/ack_deploy")
def nodes_ack_deploy(ip: str, force: bool = False, user=Depends(auth_required)):
    """运维已在节点上执行完动作后点「已执行」。

    v2.3.8：不再是「一键盲清」。只清除两类动作——无法远程核验的 manual 类、
    以及自动核验已确认落实的；对 db_uptime / proc:* 这类可远程核验但**尚未落实**的
    动作一律保留并回传原因，杜绝「点一下徽标没了、实际配置没生效、问题过会儿又回来」。
    确需绕过核验（如数据库在 regen 前就重启过、节点无 agent）时用 ?force=1 人工兜底。
    """
    try:
        from .node_gen import ack_node_pending
        r = ack_node_pending(ip, force=bool(force)) or {}
        kept = r.get("kept") or []
        if kept:
            detail = "；".join(
                "%s（%s）" % ((k or {}).get("title") or "", (k or {}).get("reason") or "")
                for k in kept)
            return JSONResponse({
                "ok": True, "cleared": r.get("cleared") or [], "kept": kept,
                "msg": "以下动作经远程核验确认【尚未生效】，已保留：%s。"
                       "请在节点执行对应命令，生效后会自动消失；"
                       "若你确认已生效只是核验不到，可用「仍然清除」。" % detail,
            })
        return JSONResponse({"ok": True, "cleared": r.get("cleared") or [], "kept": [],
                             "msg": "已清除待生效提示"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"标记失败：{type(e).__name__}: {e}"})


@app.get("/nodes/{ip}/creds_sql")
def node_creds_sql(ip: str, user=Depends(auth_required)):
    """下载该节点的「账号/权限修复脚本」fix-node-creds.sql。

    该文件含真实凭证（ha_monitor / replication / root 及管理器服务端后台 IP），
    用于节点本机以真实 root 执行自愈：修正 ha_monitor 仅 GRANT USAGE、root@'<管理器IP>'
    密码漂移、复制账号缺失等问题。业务通信仅发生在「管理器服务端后台 IP」与节点之间。
    """
    try:
        row = query(
            "SELECT n.ip, n.port, n.node_type, n.root_user, n.root_pass, n.cluster_id "
            "FROM nodes n WHERE n.ip=%s", (ip,))
        if not row:
            return JSONResponse({"ok": False, "msg": f"未找到节点 {ip}"}, status_code=404)
        n = row[0]
        cid = int(n.get("cluster_id") or 0)
        creds = get_cluster_creds(cid)
        manager_ip = get_manager_public_ip()
        manager_subnet = get_manager_docker_subnet_wildcard()
        sql = _render_node_fix_sql(n, creds, manager_ip, manager_subnet)
        headers = {"Content-Disposition": f'attachment; filename="fix-node-creds-{ip}.sql"'}
        return Response(content=sql, media_type="application/sql", headers=headers)
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"}, status_code=500)


@app.get("/api/node_creds_sql")
def api_node_creds_sql(cluster_id: int = 0, host: str = "", port: int = 0,
                       download: int = 0, user=Depends(auth_required)):
    """按 cluster_id+host+port 返回节点的 fix-node-creds.sql（查看或下载）。

    复用与 /nodes/<ip>/creds_sql 相同的渲染逻辑，供集群节点详情抽屉中
    「查看/下载自愈 SQL」按钮调用（抽屉只有 cluster_id+host+port，无独立 IP 入口）。
    """
    try:
        row = query(
            "SELECT n.ip, n.port, n.node_type, n.root_user, n.root_pass, n.cluster_id "
            "FROM nodes n "
            "WHERE n.cluster_id=%s AND n.ip=%s AND n.port=%s", (int(cluster_id), host, int(port)))
        if not row:
            return _json_ok({"ok": False, "error": f"未找到节点 {host}:{port}"})
        n = row[0]
        cid = int(n.get("cluster_id") or 0)
        creds = get_cluster_creds(cid)
        manager_ip = get_manager_public_ip()
        manager_subnet = get_manager_docker_subnet_wildcard()
        sql = _render_node_fix_sql(n, creds, manager_ip, manager_subnet)
        if download:
            headers = {"Content-Disposition": f'attachment; filename="fix-node-creds-{host}.sql"'}
            return Response(content=sql, media_type="application/sql", headers=headers)
        return _json_ok({"ok": True, "sql": sql})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


def _tail_file(path, offset=0, max_first=200000):
    """按字节偏移读取文件尾部，用于实时增量轮询。offset=0 时取最后 max_first 字节。"""
    try:
        size = os.path.getsize(path)
    except OSError:
        return "", 0, 0
    if offset < 0 or offset > size:
        offset = max(0, size - max_first)
    try:
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read()
        new_offset = offset + len(data)
        return data.decode("utf-8", "replace"), new_offset, size
    except Exception:
        return "", offset, size


@app.get("/api/srv_log")
def api_srv_log(svc: str = "ha_manager", since: int = 86400, offset: int = 0,
                user=Depends(auth_required)):
    """读取管理端容器日志。

    - ha_manager：优先经宿主 docker.sock 读取**本容器自身**的 stdout/stderr
      （entrypoint / uvicorn / print 输出都在这里，等价于 `docker logs`），
      这是唯一完整的数据源；docker.sock 不可用时回退到本地控制台日志文件。
    - 其它容器：按容器名/ID 读取宿主上的兄弟容器日志（经 docker.sock）。

    增量轮询（since < 3600）无新内容时返回空串，前端据此跳过追加，
    避免把「暂无日志」占位文本反复追加进日志区。
    """
    try:
        since = max(1, int(since))
    except (TypeError, ValueError):
        since = 86400
    try:
        off = int(offset)
    except (TypeError, ValueError):
        off = 0
    incremental = since < 3600          # 轮询增量 vs 首次全量
    empty_hint = "" if incremental else "（最近 %d 秒内暂无日志）" % since

    # ---- 管理器自身 ----
    if svc in ("ha_manager", "custos", "__self__"):
        # 1) 自身容器 stdout/stderr（最完整）
        try:
            cid = docker_helper.own_container_id()
        except Exception:
            cid = None
        if cid:
            try:
                log = docker_helper.container_logs_by_id(cid, since)
            except Exception:
                log = None
            if log is not None:
                return JSONResponse({
                    "ok": True, "svc": svc, "source": "docker-self",
                    "log": log if log.strip() else empty_hint, "offset": 0,
                })
        # 2) 回退：本地落盘日志（stdout Tee 写入的控制台日志优先）
        for path in (_MANAGER_CONSOLE_LOG, _MANAGER_LOG):
            if path and os.path.exists(path):
                text, new_off, size = _tail_file(path, off)
                if text.strip():
                    return JSONResponse({"ok": True, "svc": svc, "source": "file",
                                         "log": text, "offset": new_off, "size": size})
        return JSONResponse({"ok": True, "svc": svc, "source": "file",
                             "log": "" if incremental else
                             "（暂无日志：docker.sock 不可用，且本地日志文件为空）",
                             "offset": 0})

    # ---- 兄弟容器 ----
    TOKEN = {
        "keepalived": "keepalived",
        "monitor": "custos-monitor",
        "ha_manager-mgmt": "custos-ha-mgmt",
    }
    token = TOKEN.get(svc, svc)
    if not docker_helper.docker_available():
        return JSONResponse({"ok": False, "svc": svc,
                             "error": "未挂载 /var/run/docker.sock，无法读取兄弟容器日志。"})
    log = docker_helper.container_logs(token, since, file_fallback=(since >= 3600))
    if log is None:
        try:
            units = [u["name"] for u in docker_helper.list_units()]
        except Exception:
            units = []
        return JSONResponse({
            "ok": False, "svc": svc,
            "error": (f"本机未找到容器「{token}」。注意 keepalived / monitor 通常运行在"
                      f"**节点主机**上，不在管理端本机。"
                      f"当前宿主容器：{('、'.join(units) if units else '无')}"),
            "units": units,
        })
    return JSONResponse({"ok": True, "svc": svc, "source": "docker",
                         "log": log if log.strip() else empty_hint, "offset": 0})


@app.get("/api/srv_units")
def api_srv_units(user=Depends(auth_required)):
    """列出宿主上可读取日志的容器，供「容器日志」下拉动态选择。

    动态列举可避免下拉里出现本机根本不存在的容器（如运行在节点主机上的
    keepalived / monitor），从源头消除「点了但读不到」。
    """
    try:
        units = docker_helper.list_units()
    except Exception:
        units = []
    items = [{"value": "ha_manager", "label": "Custos（管理器自身）"}]
    seen = {"ha_manager"}
    for u in units:
        name = u.get("name") or ""
        if not name or name in seen:
            continue
        seen.add(name)
        items.append({"value": name,
                      "label": f"{name}（{u.get('state') or '未知'}）"})
    return _json_ok({"ok": True, "units": items,
                     "docker": docker_helper.docker_available()})
