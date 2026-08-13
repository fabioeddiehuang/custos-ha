"""邮件告警。优先读取管理库 settings 中的 SMTP_* 配置，环境变量作为回退。"""
import smtplib
from email.mime.text import MIMEText
from .config import env
from .db import get_setting


def _cfg():
    return dict(
        host=get_setting("SMTP_HOST") or env("SMTP_HOST"),
        port=int(get_setting("SMTP_PORT") or env("SMTP_PORT") or 25),
        user=get_setting("SMTP_USER") or env("SMTP_USER"),
        passwd=get_setting("SMTP_PASS") or env("SMTP_PASS"),
        to=get_setting("ALERT_EMAIL") or env("ALERT_EMAIL"),
    )


def send_alert(subject, body):
    c = _cfg()
    if not c["host"] or not c["to"]:
        print(f"[alert:skip] {subject} | {body}")
        return False
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[MySQL-HA] {subject}"
        msg["From"] = c["user"] or c["host"]
        msg["To"] = c["to"]
        with smtplib.SMTP(c["host"], c["port"], timeout=10) as s:
            if c["user"] and c["passwd"]:
                s.login(c["user"], c["passwd"])
            s.sendmail(msg["From"], [c["to"]], msg.as_string())
        print(f"[alert:sent] {subject}")
        return True
    except Exception as e:
        print(f"[alert:error] {e}")
        return False
