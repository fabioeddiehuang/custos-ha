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


def _cfg():
    return dict(
        host=get_setting("SMTP_HOST") or env("SMTP_HOST"),
        port=int(get_setting("SMTP_PORT") or env("SMTP_PORT") or 25),
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
    return dict(
        smtp_host=(get_setting("SMTP_HOST") or env("SMTP_HOST") or "").strip(),
        smtp_port=str(get_setting("SMTP_PORT") or env("SMTP_PORT") or "").strip(),
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


def _defer_alert(cancel_key, delay, subject, body, cluster_id, recipients):
    """挂起一封告警：delay 秒后若未被 cancel_alert 取消，才真正投递。"""
    with _pending_lock:
        old = _pending.pop(cancel_key, None)
        if old:
            try:
                old["timer"].cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _flush_alert,
                            args=(cancel_key, subject, body, cluster_id, recipients))
        t.daemon = True
        _pending[cancel_key] = {"timer": t, "subject": subject, "at": time.time() + delay}
        t.start()
    print(f"[alert:pending] 「{subject}」进入 {delay}s 自愈观察期，期内恢复则只记录不发送")


def _flush_alert(cancel_key, subject, body, cluster_id, recipients):
    """观察期结束仍未取消 -> 故障是真实的，投递邮件。"""
    with _pending_lock:
        _pending.pop(cancel_key, None)
    _deliver(subject, body, cluster_id, recipients)


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
               grace=None, cancel_key=None):
    """发送告警邮件。

    grace/cancel_key：启用「自愈观察期」。传入 cancel_key 后，告警不会立刻投递，
    而是挂起 grace 秒（缺省取 ALERT_GRACE_SEC）；期间若调用方用同一 cancel_key
    调用 cancel_alert()（表示故障已自愈），邮件被撤销，调用方改走「只记录」分支。
    force=True 时忽略观察期（测试邮件等）。
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
            _defer_alert(cancel_key, g, subject, body, cluster_id, recipients)
            return False

    return _deliver(subject, body, cluster_id, recipients, force=force)


def _deliver(subject, body, cluster_id=None, recipients=None, force=False):
    """真正投递（含同类冷却节流）。由 send_alert 或观察期到点后调用。"""
    global last_error
    last_error = ""
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
        print(f"[alert:skip] {subject} | {body}")
        return False

    # ---- 同类告警节流：同一 (主题, 集群) 在冷却期内只发一封 ----
    if not force:
        cooldown = _alert_cooldown_sec()
        if cooldown > 0:
            key = f"{subject.strip()}||{cluster_id if cluster_id is not None else '_global'}"
            now = time.time()
            with _alert_lock:
                last = _last_sent.get(key, 0.0)
                if now - last < cooldown:
                    _suppressed[key] = _suppressed.get(key, 0) + 1
                    print(f"[alert:throttled] 同类冷却中，抑制重复发送：{subject} (cluster={cluster_id})")
                    return False
                # 冷却已过，允许发送；记下来并在发送成功后落盘时间戳
                n_suppressed = _suppressed.get(key, 0)
                _suppressed[key] = 0
                _last_sent[key] = now
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
        global _CHANNEL_ERROR
        _CHANNEL_ERROR = {"ts": time.time(), "error": str(e)[:300], "subject": str(subject)[:120]}
        return False
