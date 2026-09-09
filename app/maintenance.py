"""maintenance.py — 维护模式 begin / end（v2.3.3）。

为什么需要
----------
做维护、重建从库、升级版本时，monitor 仍可能触发**自动切换**，把正在操作的节点切走：
业务闪断、重建任务半途而废、告警刷屏，运维还以为出了真故障。
Orchestrator 等同类产品都提供 `begin/end-maintenance`；这也是后续
「MariaDB 版本升级编排」（v3.0）的必要前置。

语义
----
- **作用域**：`cluster`（整集群，抑制一切自动切换与状态告警）/
             `node`（单节点，不因该节点触发切换、不发该节点状态告警）。
- **时限**：`minutes>0` 时到期自动结束（由 monitor 周期 `purge_expired()` 兜底），
            `minutes=0` 表示不限时，必须手动结束。
- **抑制范围**：自动切换（主库失联 / 延迟压力）、状态类告警（离线 / 复制 / 延迟 / 半同步）。
- **不抑制**：切换类与「需人工处理」类告警（维护期出事反而更要看得到）、
              手动切换（仅前端二次确认）。

为什么告警抑制放在两处
----------------------
- `alerts.send_alert` 按 cluster_id 兜底（覆盖所有调用点，含以后新增的）；
- `monitor._alert_on_transition` 按 (cluster_id, host) 精确判断，
  用于覆盖**节点级**维护窗口（send_alert 没有 host 参数）。
"""


def _rows(sql, args=None):
    from .db import query
    return query(sql, args) or []


def _exec(sql, args=None):
    from .db import execute
    return execute(sql, args)


def begin(cluster_id, scope="cluster", target="", reason="", minutes=0, created_by=""):
    """开启维护窗口。minutes>0 表示到期自动结束；0 表示不限时（需手动结束）。

    同一 (cluster_id, scope, target) 只保留一个生效窗口：重复开启会先结束旧的，
    避免「开了两次、结束一次还残留」的幽灵窗口。
    """
    scope = "node" if str(scope).strip().lower() == "node" else "cluster"
    target = str(target or "").strip()
    if scope == "node" and not target:
        raise ValueError("节点级维护窗口必须指定节点 IP")
    try:
        minutes = int(minutes or 0)
    except (TypeError, ValueError):
        minutes = 0
    end(cluster_id, scope=scope, target=target)
    if minutes > 0:
        _exec(
            "INSERT INTO maintenance (cluster_id, scope, target, reason, started_at, "
            "end_at, created_by, created_at) VALUES (%s,%s,%s,%s,NOW(),"
            "DATE_ADD(NOW(), INTERVAL %s MINUTE),%s,NOW())",
            (cluster_id, scope, target, (reason or "").strip()[:255],
             minutes, (created_by or "").strip()[:64]),
        )
    else:
        _exec(
            "INSERT INTO maintenance (cluster_id, scope, target, reason, started_at, "
            "end_at, created_by, created_at) VALUES (%s,%s,%s,%s,NOW(),NULL,%s,NOW())",
            (cluster_id, scope, target, (reason or "").strip()[:255],
             (created_by or "").strip()[:64]),
        )
    return True


def end(cluster_id, scope=None, target=""):
    """结束维护窗口。scope 为 None 时结束该集群的全部窗口。"""
    if scope:
        _exec(
            "UPDATE maintenance SET active=0, ended_at=NOW() "
            "WHERE cluster_id=%s AND scope=%s AND target=%s AND active=1",
            (cluster_id, scope, str(target or "").strip()),
        )
    else:
        _exec(
            "UPDATE maintenance SET active=0, ended_at=NOW() "
            "WHERE cluster_id=%s AND active=1",
            (cluster_id,),
        )
    return True


def purge_expired():
    """结束已到期的窗口（monitor 周期调用，避免依赖定时器）。"""
    try:
        return _exec(
            "UPDATE maintenance SET active=0, ended_at=NOW() "
            "WHERE active=1 AND end_at IS NOT NULL AND end_at <= NOW()")
    except Exception:
        return 0


def active_windows(cluster_id=None):
    """返回生效中的窗口列表（已过滤到期项）。"""
    purge_expired()
    if cluster_id:
        return _rows(
            "SELECT id, cluster_id, scope, target, reason, started_at, end_at, "
            "created_by, created_at FROM maintenance "
            "WHERE active=1 AND cluster_id=%s ORDER BY started_at DESC", (cluster_id,))
    return _rows(
        "SELECT id, cluster_id, scope, target, reason, started_at, end_at, "
        "created_by, created_at FROM maintenance "
        "WHERE active=1 ORDER BY started_at DESC")


def is_in_maintenance(cluster_id, host=None):
    """该集群（或该节点）是否处于维护窗口内。

    host 非空时：集群级窗口 或 该节点自身的窗口，任一命中即算维护中。
    """
    try:
        cid = int(cluster_id)
    except (TypeError, ValueError):
        return False
    try:
        rows = active_windows(cid)
    except Exception:
        return False
    for r in rows:
        if str(r.get("scope") or "cluster") == "cluster":
            return True
        if host and str(r.get("target") or "").strip() == str(host).strip():
            return True
    return False


def summary(cluster_id):
    """给前端用的维护状态摘要（没有窗口时返回 None）。"""
    try:
        rows = active_windows(int(cluster_id))
    except Exception:
        return None
    if not rows:
        return None
    r = rows[0]
    return {
        "active": True,
        "scope": r.get("scope") or "cluster",
        "target": r.get("target") or "",
        "reason": r.get("reason") or "",
        "started_at": str(r.get("started_at") or ""),
        "end_at": str(r.get("end_at") or ""),
        "created_by": r.get("created_by") or "",
        "count": len(rows),
    }
