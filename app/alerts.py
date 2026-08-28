"""邮件告警。优先读取管理库 settings 中的 SMTP_* 配置，环境变量作为回退。

多收件人模型（alert_emails / alert_email_map）：
- alert_emails.is_default=1 的邮箱为「默认接收者」，会收到所有集群的告警。
- alert_email_map 将某个邮箱绑定到指定集群，只有该集群的告警才发送给它。
- 解析收件人 = 默认邮箱 ∪ （若传入 cluster_id）该集群被显式绑定的邮箱。
- 若没有任何邮箱，则回退到旧的 ALERT_EMAIL 配置（兼容单邮箱历史用法）。
"""
import smtplib
from email.mime.text import MIMEText
from .config import env
from .db import get_setting, query


def _cfg():
    return dict(
        host=get_setting("SMTP_HOST") or env("SMTP_HOST"),
        port=int(get_setting("SMTP_PORT") or env("SMTP_PORT") or 25),
        user=get_setting("SMTP_USER") or env("SMTP_USER"),
        passwd=get_setting("SMTP_PASS") or env("SMTP_PASS"),
    )


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
    # 回退：未配置任何邮箱时，使用旧的单邮箱设置
    if not emails:
        legacy = (get_setting("ALERT_EMAIL") or env("ALERT_EMAIL") or "").strip()
        if legacy:
            emails = [x.strip() for x in legacy.split(",") if x.strip()]
    return emails


# 记录最近一次发送失败的原因，便于前端展示
last_error = ""


def send_alert(subject, body, cluster_id=None, recipients=None):
    global last_error
    last_error = ""
    c = _cfg()
    if recipients is None:
        recipients = resolve_recipients(cluster_id)
    if not c["host"] or not recipients:
        last_error = "未配置 SMTP 服务器或告警接收邮箱"
        print(f"[alert:skip] {subject} | {body}")
        return False
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[MySQL-HA] {subject}"
        msg["From"] = c["user"] or c["host"]
        msg["To"] = ", ".join(recipients)
        port = int(c["port"] or 25)
        if port == 465:
            # 显式 SSL（如腾讯企业邮 / 阿里云邮）
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
        return False
