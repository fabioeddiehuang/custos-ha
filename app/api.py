"""FastAPI Web 管理后台：登录鉴权、拓扑展示、手动切换、配置管理、历史、节点一键部署、改密码。"""
import hashlib
import json
import subprocess
import socket
import threading
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, date, time
from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from itsdangerous import URLSafeTimedSerializer, BadSignature

from .config import SECRET_KEY, WEB_USER, rand_secret, get_manager_public_ip, get_manager_docker_subnet_wildcard, get_mgmt_host, MGMT
from .db import query, execute, get_conn, get_setting, upsert_setting
from .state import get_current_master
from .failover import do_failover, do_failover_to, load_creds, ensure_topology, node_root
from .node_gen import (generate_node, list_nodes, delete_node, delete_node_dir,
                       sync_node_creds, render_fix_node_creds_sql)
from .clusters import list_clusters, validate_node_version, get_cluster, create_cluster, update_cluster, delete_cluster, get_cluster_creds
from .alerts import send_alert, last_error
from .ha_client import (heal_replication, heal_io_gtid_gap, get_status, make_slave_of,
                       fix_node_mysql_params, reset_replication_creds_on_master,
                       restart_slave_io, connect_robust)
from .monitor import run_once, start_in_background, _probe_all, trigger_act_once_async
from . import precheck, audit, metrics, slow_log, config_drift

app = FastAPI()
TEMPLATES = Environment(
    loader=FileSystemLoader(str(Path(__file__).parent / "templates")),
    autoescape=select_autoescape(["html"]),
)
serializer = URLSafeTimedSerializer(SECRET_KEY)
COOKIE = "ha_session"
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")

# 随 Web 进程内常驻后台监控线程：uvicorn 加载本模块即自动拉起，无需再单独 `python -m app.monitor`。
# monitor 负责探活、选主、建复制通道、推送 current_master（keepalived 据此漂 VIP），是 HA 的核心驱动。
start_in_background()


def hash_pwd(p):
    return hashlib.sha256((p + SECRET_KEY).encode()).hexdigest()


def auth_required(request: Request):
    token = request.cookies.get(COOKIE)
    if not token:
        raise HTTPException(status_code=307, headers={"Location": "/login"})
    try:
        user = serializer.loads(token, max_age=86400 * 7)
    except BadSignature:
        raise HTTPException(status_code=307, headers={"Location": "/login"})
    return user


def render(name, request, **ctx):
    ctx["request"] = request
    return HTMLResponse(TEMPLATES.get_template(name).render(ctx))


def _json_default(o):
    """json.dumps 兜底：datetime 等类型序列化为字符串，杜绝裸 500。"""
    if isinstance(o, (datetime, date, time)):
        return o.isoformat()
    return str(o)


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


def _build_status_payload():
    """构造与仪表盘同源的状态载荷（含 hosts.err / display_role / is_cm）。

    直接返回普通 dict，由 _json_ok 负责序列化，避免 FastAPI 在框架层
    jsonable_encoder 抛 500（函数内 try 捕获不到序列化阶段异常）。"""
    hosts = query(
        "SELECT h.host, h.port, h.cluster_id, h.role, h.status, h.is_current_master, "
        "h.initial_role, h.repl_delay, h.slave_io, h.slave_sql, h.last_seen, h.err "
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
        if not row or row["password_hash"] != hash_pwd(password):
            return render("login.html", request, error="用户名或密码错误")
    except Exception as e:
        return render("login.html", request, error=f"管理库错误: {e}")
    resp = RedirectResponse("/", status_code=303)
    resp.set_cookie(COOKIE, serializer.dumps(username), httponly=True)
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
                "h.repl_delay, h.slave_io, h.slave_sql, h.last_seen, h.err "
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
    # 读取版本号（若有）
    try:
        version = (Path(__file__).parent / "VERSION").read_text().strip()
    except Exception:
        version = ""
    return render("dashboard.html", request, user=user, clusters=dash,
                  history=history, error=err, version=version)


@app.get("/api/status")
def api_status(user=Depends(auth_required)):
    try:
        return _json_ok(_build_status_payload())
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


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
                    "node_type": nr.get("node_type") or "docker",
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


@app.get("/api/slow_log")
def api_slow_log(cluster_id: int = 0, host: str = "", port: int = 0, limit: int = 200,
                 min_ms: int = 0, user=Depends(auth_required)):
    """慢日志明细（按耗时倒序）。"""
    try:
        rows = slow_log.get_slow_log(int(cluster_id), host or None, port or None, int(limit),
                                     int(min_ms) if min_ms else None)
        return _json_ok({"ok": True, "rows": rows})
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
        # 自愈失败：记录「未恢复」状态
        try:
            execute("INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,created_at) "
                    "VALUES(%s,%s,%s,'selfheal','unresolved',%s,NOW())",
                    (cluster_id, host, int(port), tail))
        except Exception:
            pass
        return JSONResponse({"ok": False,
                              "msg": "复制仍未恢复（IO=%s, SQL=%s）。%s [%s]" % (
                                  io2, sql2, tail, "；".join(steps))})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"{type(e).__name__}: {e}"})


@app.post("/api/failover")
def api_failover(cluster_id: int = Form(...), user=Depends(auth_required)):
    try:
        run_once()  # 先按现状刷新，确保切换基于最新拓扑
    except Exception:
        pass
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
    try:
        ok = do_failover_to(cid, host, int(port), "manual: 指定主库")
        msg = ("已将 %s:%s 设为主库" % (host, port)) if ok else "设置失败(确认节点在线且已配置账号)"
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
    emails = query("SELECT id, email, is_default FROM alert_emails ORDER BY id")
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
    cfg = dict(
        smtp_host=get_setting("SMTP_HOST") or "",
        smtp_port=get_setting("SMTP_PORT") or "",
        smtp_user=get_setting("SMTP_USER") or "",
    )
    alert_emails = _load_alert_emails()
    clusters = [{"id": c["id"], "name": c.get("name") or f"集群{c['id']}"}
                for c in list_clusters()]
    return render("settings.html", request, user=user, cfg=cfg, msg=msg, error=error,
                  alert_emails=alert_emails, clusters=clusters,
                  check_interval=get_setting("CHECK_INTERVAL", "2"),
                  manager_ip=get_setting("MANAGER_PUBLIC_IP", ""))


@app.post("/settings")
def settings_save(
    request: Request,
    smtp_host: str = Form(""),
    smtp_port: str = Form(""),
    smtp_user: str = Form(""),
    smtp_pass: str = Form(""),
    check_interval: str = Form(""),
    manager_ip: str = Form(""),
    user=Depends(auth_required),
):
    upsert_setting("SMTP_HOST", smtp_host.strip())
    upsert_setting("SMTP_PORT", smtp_port.strip())
    upsert_setting("SMTP_USER", smtp_user.strip())
    if smtp_pass.strip():
        upsert_setting("SMTP_PASS", smtp_pass.strip())
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
        ok = send_alert("邮件告警测试", "测试邮件，告警通道正常！", recipients=emails)
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
        return JSONResponse({"ok": False, "msg": f"✗ 添加失败（可能已存在）：{e}"}, status_code=200)


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
            if not row or row["password_hash"] != hash_pwd(current_password):
                return render("change_password.html", request, user=user,
                              msg="", error="当前密码错误")
            c.execute("UPDATE users SET password_hash=%s WHERE username=%s",
                      (hash_pwd(new_password), user))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse("/settings?msg=密码已修改", status_code=303)


# ---------------- 集群管理（多集群：每集群独立账号/复制账号/密码/VIP） ----------------
@app.get("/clusters")
def clusters_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = "",
                  cid: int = 0):
    raw = list_clusters()
    counts = {r["cluster_id"]: r["n"] for r in
              query("SELECT cluster_id, COUNT(*) AS n FROM hosts GROUP BY cluster_id")}
    cms = {r["cluster_id"]: f"{r['host']}:{r['port']}"
           for r in query("SELECT cluster_id, host, port FROM current_master")}
    hosts_all = query(
        "SELECT cluster_id, host, port, status, role, slave_io, slave_sql, err "
        "FROM hosts")
    by_cluster = {}
    for h in hosts_all:
        by_cluster.setdefault(h["cluster_id"], []).append(h)
    clusters = []
    for c in raw:
        cid = c["id"]
        cm = cms.get(cid, "")
        hs = by_cluster.get(cid, [])
        online = sum(1 for h in hs if h.get("status") == "up")
        host_rows = []
        slaves = []
        for h in hs:
            hk = f"{h['host']}:{h['port']}"
            is_cm = (hk == cm)
            if not is_cm:
                slaves.append(hk)
            repl_bad = (not is_cm) and (
                h.get("slave_io") not in ("Yes", None) or
                h.get("slave_sql") not in ("Yes", None))
            down = h.get("status") != "up"
            host_rows.append({
                "host": h["host"], "port": h["port"],
                "role": "主库" if is_cm else "从库",
                "status": h.get("status"),
                "slave_io": h.get("slave_io") or "—",
                "slave_sql": h.get("slave_sql") or "—",
                "err": (h.get("err") or "").strip(),
                "bad": bool(repl_bad or down),
            })
        has_issue = (online != counts.get(cid, 0)) or any(r["bad"] for r in host_rows)
        cc = dict(
            c, node_count=counts.get(cid, 0),
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
        clusters.append(cc)
    active = None
    for c in clusters:
        if c["id"] == cid:
            active = c
    if not active and clusters:
        active = clusters[0]
    creds = active.get("creds") if active else None
    return render("clusters.html", request, user=user, clusters=clusters,
                  active=active, creds=creds, msg=msg, error=error,
                  auto_failover=get_setting("AUTO_FAILOVER", "1") == "1",
                  auto_repair=get_setting("AUTO_REPAIR", "1") == "1")


@app.post("/api/setting_set")
def api_setting_set(key: str = Form(...), value: str = Form(...),
                    user=Depends(auth_required)):
    """保存一个开关/配置项（如 AUTO_FAILOVER / AUTO_REPAIR）。"""
    allowed = {"AUTO_FAILOVER", "AUTO_REPAIR", "ALERT_COOLDOWN_SEC",
               "ALERT_MERGE", "ALERT_LAG_THRESHOLD", "ALERT_RECEIVERS"}
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


@app.post("/api/node_error_log")
def api_node_error_log(cluster_id: int = Form(...), host: str = Form(...), port: int = Form(...),
                       limit: int = Form(500), user=Depends(auth_required)):
    """读取节点 MySQL 错误日志（mysql.error_log）。尽力而为：用节点 root 连入，
    查 @@log_error 路径，尝试 LOAD_FILE 读取最近 limit 行；无 FILE 权限则降级返回路径提示。"""
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
        cur.execute("SELECT @@log_error")
        path = (cur.fetchone() or ("",))[0] or ""
        lines = []
        if path:
            try:
                cur.execute("SELECT LOAD_FILE(%s)", (path,))
                blob = (cur.fetchone() or ("",))[0] or ""
                if blob:
                    lines = [l for l in blob.split("\n") if l.strip()]
                    lines = lines[-int(limit):]
            except Exception as e2:
                return JSONResponse({"ok": False, "path": path,
                                      "error": "读取错误日志失败(可能无 FILE 权限): " + str(e2)})
        return JSONResponse({"ok": True, "path": path, "lines": lines})
    finally:
        conn.close()


@app.post("/api/term")
def api_term(cluster_id: int = Form(...), host: str = Form(...), port: int = Form(...),
             sql: str = Form(...), user=Depends(auth_required)):
    """只读 SQL 终端：复用 ha_monitor 账号，仅允许 SELECT/SHOW/EXPLAIN/DESCRIBE/DESC/WITH/USE/HELP。"""
    if not _READONLY_RE.match(sql or ""):
        return JSONResponse({"ok": False,
                              "error": "仅允许只读语句 (SELECT/SHOW/EXPLAIN/DESCRIBE/DESC/WITH/USE/HELP)"})
    try:
        port = int(port)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "端口无效"})
    creds = get_cluster_creds(cluster_id) or {}
    ha_user = creds.get("ha_user")
    ha_pass = creds.get("ha_pass")
    if not ha_user:
        return JSONResponse({"ok": False, "error": "集群缺少 ha_monitor 凭证"})
    try:
        conn = connect(host, port, ha_user, ha_pass, timeout=5)
    except Exception as e:
        return JSONResponse({"ok": False, "error": "无法连接节点: " + str(e)})
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description:
            cols = [d[0] for d in cur.description]
            rows = [[_jsonable(v) for v in r] for r in cur.fetchall()]
            return JSONResponse({"ok": True, "cols": cols, "rows": rows, "count": len(rows)})
        conn.commit()
        return JSONResponse({"ok": True, "cols": [], "rows": [], "count": 0, "info": "执行成功（无结果集）"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})
    finally:
        conn.close()


@app.get("/alerts")
def alerts_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    """告警设置：SMTP 状态、告警规则、收件人、当前告警集中管理。"""
    cfg = dict(
        smtp_host=get_setting("SMTP_HOST") or "",
        smtp_port=get_setting("SMTP_PORT") or "",
        smtp_user=get_setting("SMTP_USER") or "",
    )
    alert_emails = _load_alert_emails()
    clusters = [{"id": c["id"], "name": c.get("name") or f"集群{c['id']}"} for c in list_clusters()]
    rules = {
        "cooldown": get_setting("ALERT_COOLDOWN_SEC", "300"),
        "merge": get_setting("ALERT_MERGE", "1"),
        "lag_threshold": get_setting("ALERT_LAG_THRESHOLD", "30"),
        "receivers": get_setting("ALERT_RECEIVERS", ""),
    }
    return render("alerts.html", request, user=user, cfg=cfg, alert_emails=alert_emails,
                  clusters=clusters, rules=rules, msg=msg, error=error)


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


@app.post("/api/alert_rules")
def api_alert_rules(cooldown: str = Form(""), merge: str = Form(""),
                    lag_threshold: str = Form(""), receivers: str = Form(""),
                    user=Depends(auth_required)):
    """保存告警规则（冷却/合并/延迟阈值/额外收件人）。"""
    upsert_setting("ALERT_COOLDOWN_SEC", cooldown)
    upsert_setting("ALERT_MERGE", merge)
    upsert_setting("ALERT_LAG_THRESHOLD", lag_threshold)
    upsert_setting("ALERT_RECEIVERS", receivers)
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
    creds = {}
    try:
        creds = get_cluster_creds(cluster_id) or {}
    except Exception:
        creds = {}
    out = {
        "ok": True, "host": host, "port": port, "cluster_id": cluster_id,
        "root_user": root_user, "root_pass": root_pass,
        "ha_user": creds.get("ha_user"), "ha_pass": creds.get("ha_pass"),
        "repl_user": creds.get("repl_user"), "repl_pass": creds.get("repl_pass"),
        "app_user": creds.get("app_user"), "app_pass": creds.get("app_pass"),
        "server_id": None, "datadir": None, "gtid_mode": None,
        "read_only": None, "enforce_gtid_consistency": None,
    }
    try:
        conn = connect(host, port, root_user, root_pass, timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SHOW VARIABLES WHERE Variable_name IN "
                    "('server_id','datadir','gtid_mode','read_only','enforce_gtid_consistency')")
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
        finally:
            conn.close()
    except Exception as e:
        out["conn_error"] = f"{type(e).__name__}: {e}"
    return JSONResponse(out)


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
            node_type=node_type.strip() or "docker",
            mysql_version=mysql_version.strip() or None,
        )
    except Exception as e:
        return RedirectResponse(f"/clusters?error={e}", status_code=303)
    audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                    "create_cluster", f"cluster={new_id}", name, new_id)
    # 直接渲染新集群，便于用户立即查看自动生成的账号密码（眼睛图标可显隐）
    return render(
        "clusters.html", request, user=user, clusters=list_clusters(),
        active=get_cluster(new_id), creds=get_cluster_creds(new_id),
        msg=f"已创建集群「{name}」，请妥善保存自动生成的账号密码（点击眼睛图标可查看/隐藏）",
    )


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
            return render(
                "clusters.html", request, user=user, clusters=list_clusters(),
                active=get_cluster(cid), creds=get_cluster_creds(cid),
                error="vip端口必须是 1-65535 之间的整数")
    try:
        update_cluster(cid, **fields)
    except Exception as e:
        return render(
            "clusters.html", request, user=user, clusters=list_clusters(),
            active=get_cluster(cid), creds=get_cluster_creds(cid),
            error=f"保存失败: {e}",
        )
    synced = 0
    if any(k in fields for k in ("ha_pass", "repl_pass", "app_pass")):
        try:
            synced = sync_node_creds(cid)
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
                "请把各节点目录下 keepalived/keepalived.conf 同步到目标机并重载 keepalived 生效"
                "（如：scp keepalived/keepalived.conf 目标机:/home/docker/<ip>/keepalived/ 后 "
                "docker compose restart keepalived）")
    except Exception:
        pass
    return render(
        "clusters.html", request, user=user, clusters=list_clusters(),
        active=get_cluster(cid), creds=get_cluster_creds(cid), msg=msg,
    )


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
    # 附加集群名称与「去掉 mysql-ha-manager/ 前缀」的部署目录显示
    name_by_id = {c["id"]: c.get("name") or f"集群{c['id']}" for c in clusters}
    for n in nodes:
        n["cluster_name"] = name_by_id.get(n.get("cluster_id")) or "—"
        cp = n.get("compose_path") or ""
        n["dir_display"] = cp.split("mysql-ha-manager/", 1)[-1] or cp
    return render("nodes.html", request, user=user, nodes=nodes, clusters=clusters,
                  creds=creds, cfg=cfg, active_cluster=ac, msg=msg, error=error)


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
    user=Depends(auth_required),
):
    initial_role = "master" if initial_role == "master" else "slave"
    ip = (ip or "").strip()
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
    # 同集群「同构」校验：禁止 docker/mariadb 混部、主版本须一致
    try:
        validate_node_version(cid, node_type.strip() or "docker", mysql_version.strip() or None)
    except Exception as e:
        return RedirectResponse(f"/nodes?error={e}", status_code=303)
    try:
        info = generate_node(
            ip, port, root_user, root_pass, initial_role, cid,
            node_type=node_type.strip() or "docker", mysql_version=mysql_version.strip() or None,
        )
        audit.add_audit(user.get("username") if isinstance(user, dict) else str(user),
                        "add_node", f"{ip}:{port}",
                        f"type={node_type} ver={mysql_version}", cluster_id=cid)
    except Exception as e:
        return RedirectResponse(f"/nodes?error={e}", status_code=303)
    role_tip = "（已设为初始主库，VIP 将优先在此节点绑定）" if initial_role == "master" else ""
    # 节点类型特定提示
    extra_tip = ""
    nt = node_type.strip()
    if nt == "baremetal":
        extra_tip = ("（裸机 MySQL 节点已废弃，请改用 MariaDB 节点类型）")
    elif nt == "mariadb":
        extra_tip = ("（MariaDB 节点：把目录与 MariaDB 安装包 mariadb-*-linux-*.tar.gz 放同一目录，"
                     "执行 sudo bash Install-ha-mariadb-Node.sh 全量安装；"
                     "各节点就绪后管理器自动建复制/选主/VIP 漂移；"
                     "卸载用 sudo bash Uninstall-mariadb-Node.sh，重装/改密直接重跑 Install 即可）")
    return RedirectResponse(
        f"/nodes?msg=已生成节点 {info['node_name']}{role_tip}{extra_tip}，到目标主机执行：{info['run_cmd']}",
        status_code=303,
    )


@app.post("/nodes/delete")
def nodes_delete(request: Request, ip: str = Form(...), user=Depends(auth_required)):
    try:
        delete_node(ip)
        # 同步删除项目目录下已生成的同名部署单元（generated_nodes/<ip>）
        deleted = delete_node_dir(ip)
    except Exception as e:
        return RedirectResponse(f"/nodes?error={e}", status_code=303)
    extra = f"，并清理项目目录 {deleted} 个文件" if deleted else ""
    return RedirectResponse(f"/nodes?msg=已删除节点{extra}", status_code=303)


@app.post("/nodes/{ip}/regen")
def nodes_regen(ip: str, user=Depends(auth_required)):
    """用节点已保存的参数重新生成配置文件（覆盖原文件），并返回生成文件的 md5 便于核对。"""
    try:
        row = query(
            "SELECT n.ip, n.port, n.root_user, n.root_pass, n.cluster_id, h.initial_role, "
            "n.node_type, n.mysql_version "
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
        info = generate_node(
            n["ip"], n["port"], n["root_user"], n["root_pass"],
            initial_role, int(n["cluster_id"] or 0),
            node_type=n.get("node_type") or "docker",
            mysql_version=n.get("mysql_version"),
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
        })
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"重新生成失败: {e}"})


@app.get("/nodes/{ip}/creds_sql")
def node_creds_sql(ip: str, user=Depends(auth_required)):
    """下载该节点的「账号/权限修复脚本」fix-node-creds.sql。

    该文件含真实凭证（ha_monitor / replication / root 及管理器服务端后台 IP），
    用于节点本机以真实 root 执行自愈：修正 ha_monitor 仅 GRANT USAGE、root@'<管理器IP>'
    密码漂移、复制账号缺失等问题。业务通信仅发生在「管理器服务端后台 IP」与节点之间。
    """
    try:
        row = query(
            "SELECT n.root_user, n.root_pass, n.cluster_id FROM nodes n WHERE n.ip=%s", (ip,))
        if not row:
            return JSONResponse({"ok": False, "msg": f"未找到节点 {ip}"}, status_code=404)
        n = row[0]
        cid = int(n.get("cluster_id") or 0)
        creds = get_cluster_creds(cid)
        manager_ip = get_manager_public_ip()
        manager_subnet = get_manager_docker_subnet_wildcard()
        sql = render_fix_node_creds_sql(
            creds.get("ha_user") or "ha_monitor", creds.get("ha_pass") or "",
            creds.get("repl_user") or "replication", creds.get("repl_pass") or "",
            n.get("root_user") or "root", n.get("root_pass") or "",
            manager_ip,
            manager_subnet,
        )
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
            "SELECT n.root_user, n.root_pass, n.cluster_id FROM nodes n "
            "WHERE n.cluster_id=%s AND n.ip=%s AND n.port=%s", (int(cluster_id), host, int(port)))
        if not row:
            return _json_ok({"ok": False, "error": f"未找到节点 {host}:{port}"})
        n = row[0]
        cid = int(n.get("cluster_id") or 0)
        creds = get_cluster_creds(cid)
        manager_ip = get_manager_public_ip()
        manager_subnet = get_manager_docker_subnet_wildcard()
        sql = render_fix_node_creds_sql(
            creds.get("ha_user") or "ha_monitor", creds.get("ha_pass") or "",
            creds.get("repl_user") or "replication", creds.get("repl_pass") or "",
            n.get("root_user") or "root", n.get("root_pass") or "",
            manager_ip,
            manager_subnet,
        )
        if download:
            headers = {"Content-Disposition": f'attachment; filename="fix-node-creds-{host}.sql"'}
            return Response(content=sql, media_type="application/sql", headers=headers)
        return _json_ok({"ok": True, "sql": sql})
    except Exception as e:
        return _json_ok({"ok": False, "error": f"{type(e).__name__}: {e}"})


@app.get("/api/srv_log")
def api_srv_log(svc: str = "ha_manager", user=Depends(auth_required)):
    """读取服务端容器日志（默认最近 15 行），供前端「服务端容器日志」按需拉取，降低常驻压力。"""
    try:
        r = subprocess.run(["docker", "logs", "--tail", "15", svc],
                           capture_output=True, text=True, timeout=8)
        log = (r.stdout + r.stderr).strip()
        return JSONResponse({"ok": True, "log": log or "(无输出)"})
    except FileNotFoundError:
        return JSONResponse({"ok": False, "msg": "未找到 docker 命令（请在管理器宿主机运行）"})
    except Exception as e:
        return JSONResponse({"ok": False, "msg": str(e)})
