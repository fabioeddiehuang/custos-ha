"""FastAPI Web 管理后台：登录鉴权、拓扑展示、手动切换、配置管理、历史、节点一键部署、改密码。"""
import hashlib
from pathlib import Path
from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from itsdangerous import URLSafeTimedSerializer, BadSignature

from .config import SECRET_KEY, WEB_USER, rand_secret
from .db import query, execute, get_conn, get_setting, upsert_setting
from .state import get_current_master
from .failover import do_failover, do_failover_to, load_creds, ensure_topology
from .node_gen import generate_node, list_nodes, delete_node, delete_node_dir, sync_node_creds
from .clusters import list_clusters, get_cluster, create_cluster, update_cluster, delete_cluster, get_cluster_creds
from .alerts import send_alert, last_error

app = FastAPI()
TEMPLATES = Environment(
    loader=FileSystemLoader(str(Path(__file__).parent / "templates")),
    autoescape=select_autoescape(["html"]),
)
serializer = URLSafeTimedSerializer(SECRET_KEY)
COOKIE = "ha_session"
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


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
            dash.append(dict(
                cluster_id=cid, name=cl.get("name") or f"集群{cid}",
                vip=cl.get("vip") or "—",
                total_nodes=len(hosts),
                online=sum(1 for h in hosts if h["status"] == "up"),
                offline=len(hosts) - sum(1 for h in hosts if h["status"] == "up"),
                masters=sum(1 for h in hosts if h["role"] == "master"),
                slaves=sum(1 for h in hosts if h["role"] == "slave"),
                current_master=current_master,
                hosts=hosts,
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
        hosts = query(
            "SELECT h.host, h.port, h.cluster_id, h.role, h.status, h.is_current_master, "
            "h.initial_role, h.repl_delay, h.slave_io, h.slave_sql, h.last_seen "
            "FROM hosts h ORDER BY h.id"
        )
        cms = {}
        for r in query("SELECT cluster_id, host, port FROM current_master"):
            cms[r["cluster_id"]] = f"{r['host']}:{r['port']}"
        clusters = [{"id": c["id"], "name": c.get("name") or f"集群{c['id']}"}
                    for c in list_clusters()]
        switch_history = query(
            "SELECT h.id, h.from_host, h.from_port, h.to_host, h.to_port, h.reason, "
            "h.cluster_id, c.name AS cluster_name, h.created_at "
            "FROM switch_history h LEFT JOIN clusters c ON c.id=h.cluster_id "
            "ORDER BY h.id DESC LIMIT 100"
        )
        return JSONResponse({
            "ok": True,
            "current_masters": cms,
            "hosts": hosts,
            "clusters": clusters,
            "switch_history": switch_history,
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.post("/api/failover")
def api_failover(cluster_id: int = Form(...), user=Depends(auth_required)):
    ok = do_failover(cluster_id, "manual")
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
    ok = do_failover_to(cid, host, int(port), "manual: 指定主库")
    return JSONResponse({"ok": ok,
                         "msg": ("已将 %s:%s 设为主库" % (host, port))
                         if ok else "设置失败(确认节点在线且已配置账号)"})


@app.get("/settings")
def settings_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    """服务端配置：邮件告警等全局配置。各集群的管理/复制账号与密码请在「集群管理」中设置。"""
    cfg = dict(
        smtp_host=get_setting("SMTP_HOST") or "",
        smtp_port=get_setting("SMTP_PORT") or "",
        smtp_user=get_setting("SMTP_USER") or "",
        alert_email=get_setting("ALERT_EMAIL") or "",
    )
    n_clusters = len(list_clusters())
    return render("settings.html", request, user=user, cfg=cfg, msg=msg, error=error,
                  n_clusters=n_clusters)


@app.post("/settings")
def settings_save(
    request: Request,
    smtp_host: str = Form(""),
    smtp_port: str = Form(""),
    smtp_user: str = Form(""),
    smtp_pass: str = Form(""),
    alert_email: str = Form(""),
    user=Depends(auth_required),
):
    upsert_setting("SMTP_HOST", smtp_host.strip())
    upsert_setting("SMTP_PORT", smtp_port.strip())
    upsert_setting("SMTP_USER", smtp_user.strip())
    if smtp_pass.strip():
        upsert_setting("SMTP_PASS", smtp_pass.strip())
    upsert_setting("ALERT_EMAIL", alert_email.strip())
    return JSONResponse({"ok": True, "msg": "✓ 已保存邮件配置"})


@app.post("/settings/test_email")
def test_email(user=Depends(auth_required)):
    """发送一封测试邮件，验证 SMTP 配置与收件地址是否正确。"""
    try:
        ok = send_alert(
            "邮件告警测试",
            "这是一封来自 MySQL HA 管理器的测试邮件。\n"
            "若您收到此邮件，说明邮件告警配置正确，后续以下情况将自动收到告警：\n"
            "· 主库不可达 / 自动故障切换\n"
            "· 节点离线 / 恢复在线\n"
            "· 复制中断 / 复制延迟过高\n"
            "· 手动切换主库",
        )
        if ok:
            return JSONResponse({"ok": True, "msg": "✓ 发送成功：测试邮件已发出，请检查收件箱"})
        detail = last_error or "SMTP 配置不正确或收件地址无效"
        return JSONResponse({"ok": False, "msg": f"✗ 发送失败：{detail}"}, status_code=200)
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"✗ 发送失败：{e}"}, status_code=200)


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
    clusters = []
    for c in raw:
        cc = dict(
            c, node_count=counts.get(c["id"], 0),
            current_master=cms.get(c["id"], "未选举"),
        )
        try:
            cc["creds"] = get_cluster_creds(c["id"])
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
    ha_user: str = Form(""), ha_pass: str = Form(""),
    repl_user: str = Form(""), repl_pass: str = Form(""),
    app_user: str = Form(""), app_pass: str = Form(""),
    user=Depends(auth_required),
):
    name = name.strip()
    if not name:
        return RedirectResponse("/clusters?error=集群名称不能为空", status_code=303)
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
    return render(
        "clusters.html", request, user=user, clusters=list_clusters(),
        active=get_cluster(cid), creds=get_cluster_creds(cid), msg=msg,
    )


@app.post("/clusters/{cid}/delete")
def cluster_delete(cid: int, request: Request, user=Depends(auth_required)):
    try:
        delete_cluster(cid)
    except Exception as e:
        return RedirectResponse(f"/clusters?error={e}", status_code=303)
    return RedirectResponse("/clusters?msg=已删除集群及其节点", status_code=303)


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
