"""邮件告警。优先读取管理库 settings 中的 SMTP_* 配置，环境变量作为回退。

多收件人模型（alert_emails / alert_email_map）：
- alert_emails.is_default=1 的邮箱为「默认接收者」，会收到所有集群的告警。
- alert_email_map 将某个邮箱绑定到指定集群，只有该集群的告警才发送给它。
- 解析收件人 = 默认邮箱 ∪ （若传入 cluster_id）该集群被显式绑定的邮箱。
- 若没有任何邮箱，则回退到旧的 ALERT_EMAIL 配置（兼容单邮箱历史用法）。
"""
import smtplib
import time
import threading
from email.mime.text import MIMEText
from .config import env
from .db import get_setting, query, execute


def _normalize_smtp(raw_host, raw_port=""):
    """规范化 SMTP 主机 / 端口：容忍把端口或协议前缀写进「SMTP 主机」字段。

    典型误填：`smtp.exmail.qq.com:465`、`smtp://smtp.qq.com:587`。
    smtplib 会把整串当主机名交给 getaddrinfo，必然报
    `[Errno -2] Name or service not known`；页面只显示「DNS 解析失败」，
    用户完全看不出是自己多写了端口。这里拆出来即可自愈。
    """
    h = str(raw_host or "").strip()
    p_raw = str(raw_port or "").strip()
    port = None
    low = h.lower()
    for pref in ("smtp://", "smtps://", "ssl://", "tls://"):
        if low.startswith(pref):
            h = h[len(pref):]
            break
    h = h.strip().strip("/").strip()
    # 仅当「恰好一个冒号 + 冒号后是纯数字」时才拆端口，避免误伤 IPv6(::1)
    if h.count(":") == 1:
        _h, _p = h.rsplit(":", 1)
        if _p.isdigit() and _h.strip():
            h, port = _h.strip(), int(_p)
    if p_raw:
        try:
            port = int(p_raw)
        except ValueError:
            pass
    return h, (port or 25)


def _cfg():
    host, port = _normalize_smtp(
        get_setting("SMTP_HOST") or env("SMTP_HOST"),
        str(get_setting("SMTP_PORT") or env("SMTP_PORT") or "").strip(),
    )
    return dict(
        host=host,
        port=port,
        user=get_setting("SMTP_USER") or env("SMTP_USER"),
        passwd=get_setting("SMTP_PASS") or env("SMTP_PASS"),
        # 独立发件人地址（可选）：部分中继（如 API-Key 登录的邮件推送服务）
        # 登录名与发件人并不相同，必须能单独指定；留空则回退到 SMTP_USER。
        frm=get_setting("SMTP_FROM") or env("SMTP_FROM"),
    )


def smtp_view():
    """供页面展示的「生效 SMTP 配置」：口径必须与 _cfg() 一致（DB 优先、env 回退）。

    此前 /settings 与 /alerts 两个页面直接用 get_setting(...) 组装 cfg，**没有 env 回退**，
    而真正发信的 _cfg() 是 `get_setting(...) or env(...)`。于是在【用环境变量配置 SMTP】
    的部署上会出现：邮件其实能正常发出，但页面徽章恒显示「未配置 SMTP」——即
    「展示口径」与「运行口径」不一致导致的假告警。统一走本函数即可消除该歧义。
    """
    host, port = _normalize_smtp(
        get_setting("SMTP_HOST") or env("SMTP_HOST"),
        str(get_setting("SMTP_PORT") or env("SMTP_PORT") or "").strip(),
    )
    raw_port = str(get_setting("SMTP_PORT") or env("SMTP_PORT") or "").strip()
    return dict(
        smtp_host=host,
        # 端口展示与运行口径一致：未单独配置时，展示从主机里拆出的那个端口
        smtp_port=raw_port or (str(port) if host and port != 25 else ""),
        smtp_user=(get_setting("SMTP_USER") or env("SMTP_USER") or "").strip(),
        smtp_from=(get_setting("SMTP_FROM") or env("SMTP_FROM") or "").strip(),
    )


# ---------------------------------------------------------------------------
# 同类告警发送节流（避免监控每个周期重复刷邮件）
# - 同一 (主题, 集群) 在 ALERT_COOLDOWN_SEC 内只真正发送一封；
# - 冷却期内同类的后续告警被抑制，并在冷却结束后的下一封邮件中以
#   「已抑制 N 次」汇总（ALERT_MERGE=1 时），既不刷屏又保留次数信息。
# - 复用 settings 中已有的 ALERT_COOLDOWN_SEC / ALERT_MERGE 配置（前端可改）。
# ---------------------------------------------------------------------------
_alert_lock = threading.Lock()
_last_sent = {}      # key -> 上次真正发送的时间戳
_suppressed = {}     # key -> 冷却期内被抑制的次数


def _alert_cooldown_sec():
    try:
        v = int(get_setting("ALERT_COOLDOWN_SEC", "300") or 0)
    except Exception:
        v = 300
    return v if v > 0 else 0


def _alert_merge_enabled():
    v = (get_setting("ALERT_MERGE", "1") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def _alert_grace_sec():
    """自愈观察期（秒）：状态类告警先挂起这么久，期间若故障自动恢复则【只记录、不发信】。

    背景：监控每 CHECK_INTERVAL(默认 2s) 探活一次，复制中断/节点抖动往往在 1~2 个周期
    内被自愈逻辑（重挂复制、注入空事务、重新选主）修好。若一冒头就发信，运维会收到
    大量「故障 + 已恢复」的无效邮件对。默认 30s：足够覆盖自愈耗时，又不会过度延迟
    真实故障的通知（自动切换类邮件由 failover 立即发送，不受此观察期影响）。
    设为 0 表示关闭观察期，立即发送（等同旧行为）。
    """
    try:
        v = int(get_setting("ALERT_GRACE_SEC", "30") or 0)
    except Exception:
        v = 30
    return v if v > 0 else 0


# ---- 观察期待发告警：key -> {timer, subject, at} ----
_pending_lock = threading.Lock()
_pending = {}


def _defer_alert(cancel_key, delay, subject, body, cluster_id, recipients, scope=""):
    """挂起一封告警：delay秒后若未被 cancel_alert 取消，才真正投递。

    ⚠️ 本函数是【覆盖式重挂】：同一 cancel_key 再次进入时会 cancel 掉上一个 timer
    并把 deadline 往后推 delay。因此调用方**必须**只在状态跃迁时调用
    （monitor._alert_on_transition 的各分支都有 prev 门控）；若像历史上的 repl_fork
    分支那样「每周期都调」，且探活周期 < 观察期，则 deadline 被无限后推、
    告警永远不会发出（2026-10-12 深挖确认的真 bug）。
    """
    with _pending_lock:
        old = _pending.pop(cancel_key, None)
        if old:
            try:
                old["timer"].cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _flush_alert,
                            args=(cancel_key, subject, body, cluster_id, recipients, scope))
        t.daemon = True
        _pending[cancel_key] = {"timer": t, "subject": subject, "scope": scope,
                                "at": time.time() + delay}
        t.start()
    print(f"[alert:pending] 「{subject}」进入 {delay}s 自愈观察期，期内恢复则只记录不发送")


def _flush_alert(cancel_key, subject, body, cluster_id, recipients, scope=""):
    """观察期结束仍未取消 -> 故障是真实的，投递邮件。

    scope 必须原样带回，否则到点后的冷却 key 与挂起时算的不一致
    （2026-10-12：冷却 key 此前不含节点维度，同集群不同节点会互相吞告警）。
    """
    with _pending_lock:
        _pending.pop(cancel_key, None)
    _deliver(subject, body, cluster_id, recipients, scope=scope)


def cancel_alert(cancel_key, note=""):
    """观察期内故障已自愈 -> 撤销待发邮件。

    返回 True 表示确实撤销了一封待发告警（调用方应改走「只记录」分支）；
    False 表示没有待发告警（说明故障持续超过了观察期，邮件早已发出）。
    """
    with _pending_lock:
        p = _pending.pop(cancel_key, None)
        if not p:
            return False
        try:
            p["timer"].cancel()
        except Exception:
            pass
    print(f"[alert:cancel] 「{p.get('subject')}」观察期内已自愈 -> 不发送邮件（仅记录）"
          + (f"：{note}" if note else ""))
    return True


def record_alert(cluster_id, host, port, alert_type, message, status="healed"):
    """只记录、不发信：写入 alerts 表（告警中心可见，级别 info）。"""
    try:
        execute(
            "INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,created_at) "
            "VALUES(%s,%s,%s,%s,%s,%s,NOW())",
            (cluster_id, host, port, alert_type, status, message),
        )
    except Exception as e:
        print(f"[alert:record] 写入告警记录失败(可忽略): {e}")


def resolve_recipients(cluster_id=None):
    """返回应当接收告警的邮箱列表（去重）。"""
    emails = []
    seen = set()
    try:
        for r in query("SELECT email FROM alert_emails WHERE is_default=1 AND enabled=1"):
            e = (r.get("email") or "").strip()
            if e and e not in seen:
                seen.add(e)
                emails.append(e)
        if cluster_id is not None:
            for r in query(
                "SELECT e.email AS email FROM alert_emails e "
                "JOIN alert_email_map m ON m.email_id=e.id "
                "WHERE m.cluster_id=%s AND e.enabled=1",
                (int(cluster_id),),
            ):
                e = (r.get("email") or "").strip()
                if e and e not in seen:
                    seen.add(e)
                    emails.append(e)
    except Exception as e:
        print(f"[alert:resolve] 读取收件人失败: {e}")
    # 未绑定任何集群的邮箱 = 接收【全部集群】告警。
    # 不加这条，用户「新建邮箱」后（is_default 默认 0、又还没绑集群）将永远收不到
    # 任何告警——所有告警都带 cluster_id，两个分支都命中不了它，页面却显示「已添加」，
    # 表现为「明明加了邮箱却收不到报警邮件」（2026-10-02 实测确认）。
    try:
        for r in query(
            "SELECT email FROM alert_emails e WHERE e.enabled=1 "
            "AND NOT EXISTS (SELECT 1 FROM alert_email_map m WHERE m.email_id=e.id)"
        ):
            e = (r.get("email") or "").strip()
            if e and e not in seen:
                seen.add(e)
                emails.append(e)
    except Exception as e:
        print(f"[alert:resolve] 读取未绑定邮箱失败(可忽略): {e}")
    # 回退：未配置任何邮箱时，使用旧的单邮箱设置
    if not emails:
        legacy = (get_setting("ALERT_EMAIL") or env("ALERT_EMAIL") or "").strip()
        if legacy:
            emails = [x.strip() for x in legacy.split(",") if x.strip()]
    return emails


# 记录最近一次发送失败的原因，便于前端展示
last_error = ""

# 最近一次「邮件通道故障」（投递阶段真发送失败）：与 last_error 不同，它只在
# 真正尝试投递并失败时写入，用于设置页常驻提示「告警通道不可用」。
_CHANNEL_ERROR = {}


def channel_error():
    """返回最近一次邮件通道故障 {ts, error, subject}；无故障返回 None。"""
    return _CHANNEL_ERROR or None


# v2.3.3：维护窗口内**仍然要发出去**的告警关键字。
# 维护期出事（切换失败、数据冲突、备份失败）反而更要看得到，绝不能一并静默。
_MAINT_PASS_KEYWORDS = ("切换", "数据冲突", "备份失败", "重建")


def _subject_pass_maintenance(subject):
    """判断该告警在维护窗口内是否仍需发送（切换类 / 需人工处理类放行）。"""
    s = str(subject or "")
    return any(k in s for k in _MAINT_PASS_KEYWORDS)


def send_alert(subject, body, cluster_id=None, recipients=None, force=False,
               grace=None, cancel_key=None, scope=""):
    """发送告警邮件。

    grace/cancel_key：启用「自愈观察期」。传入 cancel_key 后，告警不会立刻投递，
    而是挂起 grace 秒（缺省取 ALERT_GRACE_SEC）；期间若调用方用同一 cancel_key
    调用 cancel_alert()（表示故障已自愈），邮件被撤销，调用方改走「只记录」分支。
    force=True 时忽略观察期（测试邮件等）。

    scope（2026-10-12 新增）：**节点维度**，形如 "192.168.1.188:3307"。参与同类冷却的
    key 计算。旧实现冷却 key 只用 (主题, 集群)，而主题是字面常量、节点信息只在 body 里
    ⇒ 同集群 A 节点的「复制中断」会把 B 节点 5 分钟内的同类故障一起静默吞掉，
    且跃迁触发不重试 ⇒ 该告警既无邮件也无落库。传 scope 后按节点独立冷却。
    不传则退化为旧行为（仅按主题+集群），故对既有调用方零破坏。
    """
    global last_error
    last_error = ""
    c = _cfg()
    if recipients is None:
        recipients = resolve_recipients(cluster_id)
    if not c["host"] or not recipients:
        last_error = "未配置 SMTP 服务器或告警接收邮箱"
        print(f"[alert:skip] {subject} | {body}")
        return False

    # ---- 自愈观察期：状态类告警先挂起，期间自愈则只记录不发信 ----
    if not force and cancel_key:
        g = _alert_grace_sec() if grace is None else int(grace or 0)
        if g > 0:
            _defer_alert(cancel_key, g, subject, body, cluster_id, recipients, scope)
            return False

    return _deliver(subject, body, cluster_id, recipients, force=force, scope=scope)


def _deliver(subject, body, cluster_id=None, recipients=None, force=False, scope=""):
    """真正投递（含同类冷却节流）。由 send_alert 或观察期到点后调用。"""
    global last_error, _CHANNEL_ERROR
    last_error = ""
    # 冷却节流：仅在**发送成功后**才落时间戳（见下方 try 内 [alert:sent] 处）；
    # 发送失败则不落，避免 SMTP 故障期间同类告警被冷却静默吞掉。
    _throttle_key, _throttle_now = None, 0.0
    # v2.3.3：维护窗口内抑制「状态类」告警（离线/复制/延迟/半同步），避免维护期刷屏。
    # 放在投递层而不是 send_alert：自愈观察期挂起的告警是到点后直接走 _deliver 的，
    # 只在 send_alert 判断会漏掉这批，维护期照样收到一堆邮件。
    if cluster_id and not force and not _subject_pass_maintenance(subject):
        try:
            from .maintenance import is_in_maintenance
            if is_in_maintenance(int(cluster_id)):
                print(f"[alert:skip] 维护窗口内，已抑制状态类告警：{subject}")
                return False
        except Exception:
            pass
    c = _cfg()
    if recipients is None:
        recipients = resolve_recipients(cluster_id)
    if not c["host"] or not recipients:
        last_error = "未配置 SMTP 服务器或告警接收邮箱"
        # 通道已停用（未配置 SMTP / 无收件人）时不再挂「投递失败」故障横幅
        _CHANNEL_ERROR = {}
        print(f"[alert:skip] {subject} | {body}")
        return False

    # ---- 同类告警节流：同一 (主题, 集群, 节点) 在冷却期内只发一封 ----
    # scope（节点维度）必须参与 key：主题在monitor 中是字面常量、节点信息只在 body，
    # 旧 key 不含节点 ⇒ 同集群多节点的**独立**故障互相静默吞掉（2026-10-12 深挖）。
    if not force:
        cooldown = _alert_cooldown_sec()
        if cooldown > 0:
            key = "%s||%s||%s" % (subject.strip(),
                                   cluster_id if cluster_id is not None else '_global',
                                   scope or '_all')
            # ⚠️ 必须用**单调钟**（2026-10-12）：本模块其余限频（monitor._HEAL_NEXT_AT 等）
            # 用的都是 time.monotonic()。墙钟回拨（NTP 校正 / 人工改时）会让
            # now - last 变成负值 ⇒ `now - last < cooldown` 恒成立 ⇒ 该 key 被
            # **无限期抑制**（回拨 1 小时 = 该类告警静默 1 小时以上）。
            now = time.monotonic()
            with _alert_lock:
                last = _last_sent.get(key, 0.0)
                if now - last < cooldown:
                    _suppressed[key] = _suppressed.get(key, 0) + 1
                    print(f"[alert:throttled] 同类冷却中，抑制重复发送：{subject} (cluster={cluster_id})")
                    return False
                # 冷却已过，允许发送。
                # ⚠ 不在这里落时间戳：若下面 SMTP 发送失败却已记时间戳，冷却期内后续
                #   同类告警会被全部抑制 → 真实故障的邮件被**静默吞掉**。
                #   改为「发送成功后才落」（见 try 内 [alert:sent] 处）。
                n_suppressed = _suppressed.get(key, 0)
                _suppressed[key] = 0
                _throttle_key, _throttle_now = key, now
            if n_suppressed > 0 and _alert_merge_enabled():
                mins = max(1, cooldown // 60)
                body = body + f"\n\n[注] 本类告警在过去的 {mins} 分钟内已被同类冷却抑制 {n_suppressed} 次。"

    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[MySQL-HA] {subject}"
        msg["From"] = c["frm"] or c["user"] or c["host"]
        msg["To"] = ", ".join(recipients)
        port = int(c["port"] or 25)
        if port == 465:
            # 显式 SSL（如企业邮件服务）
            with smtplib.SMTP_SSL(c["host"], port, timeout=10) as s:
                if c["user"] and c["passwd"]:
                    s.login(c["user"], c["passwd"])
                s.sendmail(msg["From"], recipients, msg.as_string())
        else:
            # 明文或 STARTTLS（端口 25 / 587 等）
            with smtplib.SMTP(c["host"], port, timeout=10) as s:
                s.ehlo()
                if port in (587, 465) or port == 25:
                    try:
                        s.starttls()
                        s.ehlo()
                    except Exception:
                        pass
                if c["user"] and c["passwd"]:
                    s.login(c["user"], c["passwd"])
                s.sendmail(msg["From"], recipients, msg.as_string())
        print(f"[alert:sent] {subject} -> {recipients}")
        # 发送**成功**才落冷却时间戳（失败则不落，避免同类告警在冷却期内被静默抑制）
        if _throttle_key is not None:
            with _alert_lock:
                _last_sent[_throttle_key] = _throttle_now
        # 通道恢复即消除「邮件通道故障」横幅：此前 _CHANNEL_ERROR 只在失败时写入、
        # 成功从不清除，导致一次投递失败（哪怕早已修好）会让横幅永久显示陈旧错误，
        # 只有重启管理器进程才消失。
        _CHANNEL_ERROR = {}
        return True
    except Exception as e:
        last_error = str(e)
        print(f"[alert:error] {e}")
        # 发信失败必须留痕（2026-10-02）：否则 SMTP 配错 / DNS 不通 / 端口被墙时，
        # 告警【静默丢失】——用户看到的是「没告警=一切正常」，实际告警通道早断了，
        # 只有翻容器日志才能发现。这里：① 落一条告警中心记录（页面可见）；
        # ② 记住通道错误，供设置页提示。
        try:
            record_alert(cluster_id, "-", 0, "alert_channel",
                         "告警邮件发送失败（本次告警未送达）：%s | %s"
                         % (subject, str(e)[:200]),
                         status="failed")
        except Exception:
            pass
        _CHANNEL_ERROR = {"ts": time.time(), "error": str(e)[:300], "subject": str(subject)[:120]}
        return False
