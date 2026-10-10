"""metrics.py — v2.1 指标历史快照采集（供趋势图）。

monitor 周期性调用 collect_metrics()，把每个节点的复制状态快照写入 metric_history。
- seconds_behind / slave_io / slave_sql / ro / connections / repl_delay
- repl_delay 即复制延迟（秒），趋势图主指标；NULL 表示无复制通道（主库）。
保留期由 setting METRIC_RETENTION_DAYS 控制（默认 30 天）。
"""
import threading

from .db import query, execute, get_setting
from .ha_client import get_status, connect_robust

# 每个节点上一次落库快照的签名，用于变更检测（一致则不 INSERT）
# 加锁：collect_metrics 由监控线程周期性调用，也可能被其它后台任务触发；
# 无保护的字典读写在并发下会让签名交错、变更检测失真（漏插或重复插快照）。
_LAST_SIG = {}
_SIG_LOCK = threading.Lock()


def _cluster_ids(cluster_id=None):
    if cluster_id:
        return [cluster_id]
    return [c["id"] for c in query("SELECT id FROM clusters")]


def collect_metrics(cluster_id=None):
    """采集全部（或指定）集群节点复制状态快照。返回**实际写入**的节点数。

    【空闲零写入】某节点本次快照与上一条完全一致（延迟 / IO / SQL / read_only / 连接数
    全相同）时**不 INSERT**。旧实现每 ~9s 对所有节点无条件插一行，是管理库空闲写入的
    主要来源之一；稳态下现在一次写都不做。

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
            # MariaDB 的 @@read_only 返回 'ON'/'OFF' 字符串（MySQL 为 1/0 整型）。
            # 原 `1 if ro else 0` 对 'OFF'（**可写**）得 1 —— 非空字符串为真，
            # 于是把**可写**节点记成只读，metric_history.ro 与变更检测签名整体反了。
            # 这里按两种形态统一解析成 0/1。
            if ro is None:
                ro_i = 0
            elif isinstance(ro, (int, float)):
                ro_i = 1 if int(ro) else 0
            else:
                ro_i = 1 if str(ro).strip().upper() in ("1", "ON", "TRUE") else 0
            connections = st.get("connections")
            threads_running = st.get("threads_running")
            # 变更检测：与上次落库快照一致则整条跳过（稳态零写入）
            sig = (
                "" if seconds_behind is None else str(seconds_behind),
                st.get("slave_io") or "",
                st.get("slave_sql") or "",
                str(ro_i),
                "" if connections is None else str(connections),
            )
            key = (cid, ip, port)
            with _SIG_LOCK:
                if _LAST_SIG.get(key) == sig:
                    continue
                _LAST_SIG[key] = sig
            execute(
                "INSERT INTO metric_history "
                "(cluster_id, host, port, seconds_behind, slave_io, slave_sql, ro, connections, repl_delay) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (cid, ip, port, seconds_behind,
                 st.get("slave_io"), st.get("slave_sql"),
                 ro_i, connections, repl_delay),
            )
            count += 1
    _purge()
    return count


# 保留期清理限频：DELETE ... WHERE ts < ... 若缺 ts 单列索引会全表扫描，
# 把整表页读进 buffer pool 造成脏页与后续 checkpoint 写盘。限频后影响可忽略
# （索引已在 db.py 的迁移里补建：metric_history.idx_ts）。
_LAST_PURGE = [0.0]


def _purge():
    import time
    if time.monotonic() - _LAST_PURGE[0] < 3600:
        return
    _LAST_PURGE[0] = time.monotonic()
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
