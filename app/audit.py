"""audit.py — v2.1 操作审计日志。

记录两类来源：
- 手动操作（Web 用户在 api.py 中调用 add_audit，actor=Web 用户名）
- 自动操作（故障切换/自愈/重指向等，actor='system'）

仅写入与查询，无副作用，不影响主流程。
"""
from .db import query, execute


def add_audit(actor, action, target="", detail="", cluster_id=None):
    try:
        execute(
            "INSERT INTO audit_log (actor, action, target, detail, cluster_id) "
            "VALUES (%s,%s,%s,%s,%s)",
            (actor or "system", action, target or "", detail or "", cluster_id),
        )
    except Exception as e:
        # 审计失败绝不应阻断主流程
        print(f"[audit] 写入审计日志失败(可忽略): {e}")


def list_audit(limit=200, offset=0, cluster_id=None):
    try:
        if cluster_id:
            return query(
                "SELECT * FROM audit_log WHERE cluster_id=%s ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (cluster_id, int(limit), int(offset)),
            )
        return query(
            "SELECT * FROM audit_log ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (int(limit), int(offset)),
        )
    except Exception:
        return []
