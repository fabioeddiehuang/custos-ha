"""FastAPI Web 管理后台：登录鉴权、拓扑展示、手动切换、配置管理、历史、节点一键部署、改密码。"""
import hashlib
import json
from pathlib import Path
from datetime import datetime, date, time
from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from itsdangerous import URLSafeTimedSerializer, BadSignature

from .config import SECRET_KEY, WEB_USER, rand_secret
from .db import query, execute, get_conn, get_setting, upsert_setting
from .state import get_current_master
from .failover import do_failover, do_failover_to, load_creds, ensure_topology, node_root
from .node_gen import generate_node, list_nodes, delete_node, delete_node_dir, sync_node_creds
from .clusters import list_clusters, get_cluster, create_cluster, update_cluster, delete_cluster, get_cluster_creds
from .alerts import send_alert, last_error
from .ha_client import heal_replication, heal_io_gtid_gap, get_status, make_slave_of
from .monitor import run_once, start_in_background, _probe_all, trigger_act_once_async

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

        # 重新读取，给出最终状态与真实错误（而非掩盖为「复制正常」）
        st2 = live()
        io2, sql2 = st2.get("slave_io"), st2.get("slave_sql")
        if io2 == "Yes" and sql2 == "Yes":
            return JSONResponse({"ok": True, "msg": "复制已修复。" + ("；".join(steps) if steps else "")})
        io_err = (st2.get("slave_io_err") or "").strip()
        sql_err = (st2.get("slave_sql_err") or "").strip()
        detail = "；".join([x for x in [io_err, sql_err] if x])
        if detail:
            tail = "真实错误: " + detail
        else:
            tail = "真实错误未知（建议检查 %s → %s 的网络连通、复制账号(%s)权限与 GTID 集合）" % (
                host, cur_h, creds.get("repl_user"))
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
    ok = do_failover(cluster_id, "manual")
    try:
        run_once()  # 切换后立即重建拓扑并回写状态，使仪表盘即时刷新
    except Exception:
        pass
    return JSONResponse({"ok": ok, "msg": "已触发手动切换" if ok else "切换未执行(检查配置/日志)"})


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
    ok = do_failover_to(cid, host, int(port), "manual: 指定主库")
    try:
        run_once()  # 设主后立即重建拓扑并回写状态，使仪表盘即时刷新
    except Exception:
        pass
    return JSONResponse({"ok": ok,
                         "msg": ("已将 %s:%s 设为主库" % (host, port))
                         if ok else "设置失败(确认节点在线且已配置账号)"})


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
                  alert_emails=alert_emails, clusters=clusters)


@app.post("/settings")
def settings_save(
    request: Request,
    smtp_host: str = Form(""),
    smtp_port: str = Form(""),
    smtp_user: str = Form(""),
    smtp_pass: str = Form(""),
    user=Depends(auth_required),
):
    upsert_setting("SMTP_HOST", smtp_host.strip())
    upsert_setting("SMTP_PORT", smtp_port.strip())
    upsert_setting("SMTP_USER", smtp_user.strip())
    if smtp_pass.strip():
        upsert_setting("SMTP_PASS", smtp_pass.strip())
    return JSONResponse({"ok": True, "msg": "✓ 已保存 SMTP 配置"})


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
                  active=active, creds=creds, msg=msg, error=error)


@app.post("/clusters")
def clusters_create(
    request: Request, name: str = Form(...),
    vip: str = Form(""), vip_prefix: str = Form("24"),
    interface: str = Form(""), vrid: str = Form(""),
    vip_port: str = Form(""),
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
        )
    except Exception as e:
        return RedirectResponse(f"/clusters?error={e}", status_code=303)
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


@app.post("/nodes/add")
def nodes_add(
    request: Request,
    ip: str = Form(...),
    port: str = Form("3307"),
    root_user: str = Form("root"),
    root_pass: str = Form(...),
    initial_role: str = Form("master"),
    cluster_id: int = Form(0),
    user=Depends(auth_required),
):
    initial_role = "master" if initial_role == "master" else "slave"
    # 节点必须归属已有集群（新建集群仅在「集群管理」页面完成）
    cid = int(cluster_id) if cluster_id else None
    if not cid:
        return RedirectResponse("/nodes?error=请先在「集群管理」创建集群，再选择所属集群", status_code=303)
    try:
        info = generate_node(ip, port, root_user, root_pass, initial_role, cid)
    except Exception as e:
        return RedirectResponse(f"/nodes?error={e}", status_code=303)
    role_tip = "（已设为初始主库，VIP 将优先在此节点绑定）" if initial_role == "master" else ""
    return RedirectResponse(
        f"/nodes?msg=已生成节点 {info['node_name']}{role_tip}，到目标主机执行：{info['run_cmd']}",
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
            "SELECT n.ip, n.port, n.root_user, n.root_pass, n.cluster_id, h.initial_role "
            "FROM nodes n LEFT JOIN hosts h ON h.host=n.ip AND h.cluster_id=n.cluster_id "
            "WHERE n.ip=%s", (ip,)
        )
        if not row:
            return JSONResponse({"ok": False, "msg": f"未找到节点 {ip} 的生成记录"})
        n = row[0]
        # 节点可能尚未探活登记 initial_role，缺省为 slave
        initial_role = n.get("initial_role") or "slave"
        info = generate_node(
            n["ip"], n["port"], n["root_user"], n["root_pass"],
            initial_role, int(n["cluster_id"] or 0),
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
