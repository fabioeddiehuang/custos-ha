"""slow_log.py — v2.1 慢日志采集（节点 mysql.slow_log -> 管理库 slow_log）。

采集流程（周期性，由 monitor 调用）：
1. 用节点 root 连入，读取 mysql.slow_log（需节点已开启 slow_query_log 且 log_output=TABLE）。
2. 写入管理库 slow_log 表（保留期由 SLOWLOG_RETENTION_DAYS 控制，默认 7 天）。
3. 采集后 TRUNCATE 节点侧 mysql.slow_log，保证节点侧日志表极小、不影响性能。

若节点未开启慢日志（表不存在/无权限），静默跳过（best-effort），不影响其他节点。
"""
from .db import query, execute, get_setting
from .ha_client import connect_robust
from .clusters import get_cluster_creds
from .node_gen import list_nodes
from .config import SLOWLOG_ENABLED


def slowlog_enabled():
    """运行时开关：优先取管理库 settings.SLOWLOG_ENABLED，缺省回退到环境变量。"""
    db = get_setting("SLOWLOG_ENABLED")
    if db is not None:
        return str(db).strip() in ("1", "true", "True", "on", "yes", "y", "Y")
    return SLOWLOG_ENABLED


def _to_ms(val):
    if val is None:
        return 0
    try:
        # pymysql 把 TIME 映射为 datetime.timedelta
        return int(getattr(val, "total_seconds", lambda: 0)() * 1000)
    except Exception:
        return 0


def collect_slow_log(cluster_id=None):
    """采集全部（或指定）集群节点的慢日志。返回采集条数。

    凭证：统一使用集群的 ha_monitor 账号（get_cluster_creds 返回，管理器确定持有）。
    旧实现取 nodes 表的 root_user/root_pass，但 list_nodes() 并不返回 root_pass，
    导致空密码连不上节点被静默跳过，慢日志永远是空。改用 ha_monitor 后至少能连上。
    若节点未开启慢日志（mysql.slow_log 表不存在 / 账号无权限），仍静默跳过（best-effort）。
    """
    if not slowlog_enabled():
        # 采集已暂停：避免 ha_monitor 无 DELETE 权限清空 mysql.slow_log 时
        # 每周期报 1556 并重复采集。需开启时设置 SLOWLOG_ENABLED=1 并授权 DELETE。
        return 0
    if cluster_id:
        clusters = [cluster_id]
    else:
        clusters = [c["id"] for c in query("SELECT id FROM clusters")]
    total = 0
    for cid in clusters:
        creds = get_cluster_creds(cid)
        if not creds:
            continue
        nodes = list_nodes(cid)
        for n in nodes:
            ip = n["ip"]
            port = int(n["port"])
            conn = connect_robust(ip, port, creds["ha_user"], creds["ha_pass"])
            if not conn:
                continue
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM mysql.slow_log")
                cur.fetchone()
                cur.execute(
                    "SELECT query_time, lock_time, rows_sent, rows_examined, db, sql_text "
                    "FROM mysql.slow_log"
                )
                for r in cur.fetchall():
                    execute(
                        "INSERT INTO slow_log "
                        "(cluster_id, host, port, query_time_ms, lock_time_ms, rows_sent, "
                        "rows_examined, db, sql_text) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (cid, ip, port, _to_ms(r[0]), _to_ms(r[1]),
                         int(r[2] or 0), int(r[3] or 0), r[4] or "", (r[5] or "")[:4000]),
                    )
                    total += 1
                # 清空节点侧慢日志表，保持节点侧极小
                # 用 DELETE 而非 TRUNCATE：TRUNCATE 对 CSV 日志表需 DROP 权限，
                # ha_monitor 仅有 DELETE（避免授予过宽的 DROP）；DELETE 需 DELETE 权限。
                # 已部署(v2.1 前)节点未重跑 init.sql、无 DELETE 授权时，回退用节点 root 清空，
                # 避免「节点表不清 → 每周期把相同行重复插入管理库 slow_log」。
                try:
                    cur.execute("DELETE FROM mysql.slow_log")
                    conn.commit()
                except Exception:
                    conn.rollback()
                    try:
                        rconn = connect_robust(
                            ip, port, n.get("root_user") or "root", n.get("root_pass") or ""
                        )
                        if rconn:
                            try:
                                rc = rconn.cursor()
                                rc.execute("DELETE FROM mysql.slow_log")
                                rconn.commit()
                            finally:
                                rconn.close()
                    except Exception as e2:
                        print(f"[slow_log] 节点 {ip}:{port} 清空慢日志表失败(将重复采集,建议授予 ha_monitor DELETE 权限): {e2}")
            except Exception as e:
                # 未开启慢日志（表不存在/无权限）时静默跳过
                print(f"[slow_log] 节点 {ip}:{port} 跳过(慢日志不可用,需节点开启 slow_query_log+log_output=TABLE 且账号有权限): {e}")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
    _purge()
    return total


def _purge():
    try:
        days = int(get_setting("SLOWLOG_RETENTION_DAYS") or 7)
        execute("DELETE FROM slow_log WHERE ts < DATE_SUB(NOW(), INTERVAL %s DAY)", (days,))
    except Exception as e:
        print(f"[slow_log] 清理失败(可忽略): {e}")


def get_slow_log(cluster_id, host=None, port=None, limit=200, min_ms=None):
    try:
        sql = ("SELECT ts, host, port, query_time_ms, lock_time_ms, rows_sent, rows_examined, db, sql_text "
               "FROM slow_log WHERE cluster_id=%s")
        args = [cluster_id]
        if host:
            sql += " AND host=%s AND port=%s"
            args += [host, int(port)]
        if min_ms:
            sql += " AND query_time_ms>=%s"
            args.append(int(min_ms))
        sql += " ORDER BY query_time_ms DESC LIMIT %s"
        args.append(int(limit))
        return query(sql, tuple(args))
    except Exception:
        return []
