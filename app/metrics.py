"""metrics.py — v2.1 指标历史快照采集（供趋势图）。

monitor 周期性调用 collect_metrics()，把每个节点的复制状态快照写入 metric_history。
- seconds_behind / slave_io / slave_sql / ro / connections / repl_delay
- repl_delay 即复制延迟（秒），趋势图主指标；NULL 表示无复制通道（主库）。
保留期由 setting METRIC_RETENTION_DAYS 控制（默认 30 天）。
"""
from .db import query, execute, get_setting
from .ha_client import get_status, connect_robust


def _cluster_ids(cluster_id=None):
    if cluster_id:
        return [cluster_id]
    return [c["id"] for c in query("SELECT id FROM clusters")]


def collect_metrics(cluster_id=None):
    """采集全部（或指定）集群节点复制状态快照。返回采集节点数。

    连接数/线程数由 get_status() 内统一用 ha_monitor 账号采集（无需节点 root），
    故此处直接取 st 的结果，不再单独探活。
    """
    from .clusters import get_cluster_creds
    from .node_gen import list_nodes
    count = 0
    for cid in _cluster_ids(cluster_id):
        creds = get_cluster_creds(cid)
        if not creds:
            continue
        nodes = list_nodes(cid)
        for n in nodes:
            ip = n["ip"]
            port = int(n["port"])
            st = get_status(ip, port, creds["ha_user"], creds["ha_pass"])
            seconds_behind = st.get("seconds_behind")
            repl_delay = seconds_behind if (seconds_behind is not None) else None
            ro = st.get("ro")
            connections = st.get("connections")
            threads_running = st.get("threads_running")
            execute(
                "INSERT INTO metric_history "
                "(cluster_id, host, port, seconds_behind, slave_io, slave_sql, ro, connections, repl_delay) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (cid, ip, port, seconds_behind,
                 st.get("slave_io"), st.get("slave_sql"),
                 1 if ro else 0, connections, repl_delay),
            )
            count += 1
    _purge()
    return count


def _purge():
    try:
        days = int(get_setting("METRIC_RETENTION_DAYS") or 30)
        execute("DELETE FROM metric_history WHERE ts < DATE_SUB(NOW(), INTERVAL %s DAY)",
                (days,))
    except Exception as e:
        print(f"[metrics] 清理历史失败(可忽略): {e}")


def get_metric_history(cluster_id, host=None, port=None, hours=24):
    """取最近 hours 小时内的快照，供趋势图（按时间升序）。"""
    try:
        if host:
            return query(
                "SELECT ts, seconds_behind, slave_io, slave_sql, ro, connections, repl_delay "
                "FROM metric_history WHERE cluster_id=%s AND host=%s AND port=%s "
                "AND ts >= DATE_SUB(NOW(), INTERVAL %s HOUR) ORDER BY ts",
                (cluster_id, host, int(port), int(hours)),
            )
        return query(
            "SELECT ts, seconds_behind, slave_io, slave_sql, ro, connections, repl_delay "
            "FROM metric_history WHERE cluster_id=%s "
            "AND ts >= DATE_SUB(NOW(), INTERVAL %s HOUR) ORDER BY ts",
            (cluster_id, int(hours)),
        )
    except Exception:
        return []
