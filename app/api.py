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
from .failover import do_failover, do_failover_to, load_creds
from .node_gen import generate_node, list_nodes, delete_node, sync_node_creds

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
    cur = get_current_master()
    try:
        hosts = query(
            "SELECT h.host, h.port, h.role, h.status, h.is_current_master, h.initial_role, "
            "h.repl_delay, h.slave_io, h.slave_sql, h.last_seen, h.err "
            "FROM hosts h ORDER BY h.id"
        )
        history = query(
            "SELECT * FROM switch_history ORDER BY id DESC LIMIT 20"
        )
    except Exception as e:
        hosts, history = [], []
        err = str(e)
    else:
        err = None
    return render("dashboard.html", request, user=user, hosts=hosts,
                  current=cur, history=history, error=err)


@app.get("/api/status")
def api_status(user=Depends(auth_required)):
    try:
        hosts = query("SELECT host, port, role, status, is_current_master, initial_role, "
                      "repl_delay, slave_io, slave_sql, last_seen FROM hosts ORDER BY id")
        cur = get_current_master()
        return JSONResponse({"ok": True, "current_master": cur, "hosts": hosts})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.post("/api/failover")
def api_failover(user=Depends(auth_required)):
    ok = do_failover("manual")
    return JSONResponse({"ok": ok, "msg": "已触发手动切换" if ok else "切换未执行(检查配置/日志)"})


@app.post("/api/failover/to")
def api_failover_to(host: str = Form(...), port: int = Form(...), user=Depends(auth_required)):
    ok = do_failover_to(host, int(port), "manual: 指定主库")
    return JSONResponse({"ok": ok,
                         "msg": ("已将 %s:%s 设为主库" % (host, port))
                         if ok else "设置失败(确认节点在线且已配置账号)"})


@app.get("/settings")
def settings_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    creds = load_creds()
    cfg = dict(
        ha_user=creds.get("ha_user") or "ha_monitor",
        ha_pass=get_setting("HA_PASS") or "",
        repl_user=creds.get("repl_user") or "repl",
        repl_pass=get_setting("REPL_PASS") or "",
        app_user=get_setting("APP_USER") or "app",
        app_pass=get_setting("APP_PASS") or "",
        vip=get_setting("VIP") or "",
        interface=get_setting("INTERFACE") or "eth0",
        vrid=get_setting("VRID") or "51",
        smtp_host=get_setting("SMTP_HOST") or "",
        smtp_port=get_setting("SMTP_PORT") or "",
        smtp_user=get_setting("SMTP_USER") or "",
        alert_email=get_setting("ALERT_EMAIL") or "",
    )
    return render("settings.html", request, user=user, cfg=cfg, msg=msg, error=error)


@app.post("/settings")
def settings_save(
    request: Request,
    ha_user: str = Form(""),
    ha_pass: str = Form(""),
    repl_user: str = Form(""),
    repl_pass: str = Form(""),
    vip: str = Form(""),
    interface: str = Form(""),
    vrid: str = Form(""),
    smtp_host: str = Form(""),
    smtp_port: str = Form(""),
    smtp_user: str = Form(""),
    smtp_pass: str = Form(""),
    alert_email: str = Form(""),
    user=Depends(auth_required),
):
    generated = []

    # 第一步：管理账号（ha_monitor）。密码留空则沿用已保存值；若从未设置则自动生成。
    ha_user = ha_user.strip() or "ha_monitor"
    if ha_pass.strip():
        ha_pass = ha_pass.strip()
    else:
        ha_pass = get_setting("HA_PASS")
        if not ha_pass:
            ha_pass = rand_secret(16)
            generated.append(("ha_monitor 密码", ha_pass))

    # 第二步：复制账号（replication）。同上逻辑。
    repl_user = repl_user.strip() or "repl"
    if repl_pass.strip():
        repl_pass = repl_pass.strip()
    else:
        repl_pass = get_setting("REPL_PASS")
        if not repl_pass:
            repl_pass = rand_secret(16)
            generated.append(("复制(replication)账号密码", repl_pass))

    # 业务最小权限账号（app）：供测试/应用通过 VIP 连接，密码仅首次自动生成，之后沿用。
    app_user = get_setting("APP_USER") or "app"
    app_pass = get_setting("APP_PASS")
    if not app_pass:
        app_pass = rand_secret(16)
        upsert_setting("APP_PASS", app_pass)
        generated.append(("业务(app)账号密码", app_pass))

    # 第三步：VIP 必填（系统无法自动探测节点网段空闲 IP）；网卡/VRID 留空用默认值。
    vip = vip.strip()
    if not vip:
        return render(
            "settings.html", request, user=user,
            cfg=dict(
                ha_user=ha_user, ha_pass=get_setting("HA_PASS") or "",
                repl_user=repl_user, repl_pass=get_setting("REPL_PASS") or "",
                vip="", interface=interface.strip() or "eth0", vrid=vrid.strip() or "51",
                smtp_host=smtp_host, smtp_port=smtp_port,
                smtp_user=smtp_user, alert_email=alert_email,
            ),
            error="VIP 为必填项：需为节点所在网段的空闲 IP，系统无法自动探测，请在「第三步」填写。",
        )
    interface = interface.strip() or "eth0"
    vrid = vrid.strip() or "51"

    # 记录旧密码，用于判断 HA/复制密码是否变化（变化则需同步到节点）
    old_ha_pass = get_setting("HA_PASS")
    old_repl_pass = get_setting("REPL_PASS")

    # 保存（服务端配置，与节点无关）
    upsert_setting("HA_USER", ha_user)
    upsert_setting("HA_PASS", ha_pass)
    upsert_setting("REPL_USER", repl_user)
    upsert_setting("REPL_PASS", repl_pass)
    upsert_setting("APP_USER", app_user)
    upsert_setting("APP_PASS", app_pass)
    upsert_setting("VIP", vip)
    upsert_setting("INTERFACE", interface)
    upsert_setting("VRID", vrid)
    upsert_setting("SMTP_HOST", smtp_host)
    upsert_setting("SMTP_PORT", smtp_port)
    upsert_setting("SMTP_USER", smtp_user)
    if smtp_pass.strip():
        upsert_setting("SMTP_PASS", smtp_pass.strip())
    upsert_setting("ALERT_EMAIL", alert_email)

    # HA/复制密码变更：同步到在线节点（ALTER USER）并重生成 ha_check.conf，
    # 避免节点 MySQL 账号或 keepalived 检查脚本密码与管理端脱节导致探活失败 / VIP 不漂移。
    creds_changed = (ha_pass != old_ha_pass) or (repl_pass != old_repl_pass)
    synced = 0
    if creds_changed:
        if old_ha_pass:
            upsert_setting("HA_PASS_PREV", old_ha_pass)  # 供 sync 时回连旧密码节点
        try:
            synced = sync_node_creds()
        except Exception as e:
            generated.append(("凭证同步到节点失败", str(e)))

    cfg = dict(
        ha_user=ha_user, ha_pass=ha_pass, repl_user=repl_user, repl_pass=repl_pass,
        app_user=app_user, app_pass=app_pass,
        vip=vip, interface=interface, vrid=vrid,
        smtp_host=smtp_host, smtp_port=smtp_port,
        smtp_user=smtp_user, alert_email=alert_email,
    )
    sync_tip = ""
    if creds_changed:
        sync_tip = (f"；已把新凭证同步到 {synced} 个在线节点（请到各节点宿主机执行 "
                    f"`docker compose restart keepalived` 使 ha_check.conf 生效）")
    return render(
        "settings.html", request, user=user, cfg=cfg,
        msg="已保存「服务端配置」"
            + ("（本次自动生成了部分密码，请查看下方提示并妥善保存）" if generated else "")
            + sync_tip,
        generated=generated,
    )


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
                return render("settings.html", request, user=user,
                              msg="", error="当前密码错误")
            c.execute("UPDATE users SET password_hash=%s WHERE username=%s",
                      (hash_pwd(new_password), user))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse("/settings?msg=密码已修改", status_code=303)


# ---------------- 节点一键部署 ----------------
@app.get("/nodes")
def nodes_page(request: Request, user=Depends(auth_required), msg: str = "", error: str = ""):
    nodes = list_nodes()
    return render("nodes.html", request, user=user, nodes=nodes, msg=msg, error=error)


@app.post("/nodes/add")
def nodes_add(
    request: Request,
    ip: str = Form(...),
    port: str = Form("3307"),
    root_user: str = Form("root"),
    root_pass: str = Form(...),
    initial_role: str = Form("slave"),
    user=Depends(auth_required),
):
    initial_role = "master" if initial_role == "master" else "slave"
    try:
        info = generate_node(ip, port, root_user, root_pass, initial_role)
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
    except Exception as e:
        return RedirectResponse(f"/nodes?error={e}", status_code=303)
    return RedirectResponse("/nodes?msg=已删除", status_code=303)
