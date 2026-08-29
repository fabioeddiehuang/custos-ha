"""slow_log.py — v2.2 慢日志采集（节点 mysql.slow_log -> 管理库 slow_log）。

采集流程（周期性，由 monitor 调用）：
1. 用集群 ha_monitor 连入节点，读取 mysql.slow_log（需节点已开启 slow_query_log
   且 log_output=TABLE）。
2. **增量采集**：slow_log_cursor 记录每个节点已采集到的最大 start_time，下次只取
   start_time > 游标 的新行。这样即使清空节点侧日志表失败（权限不足 / 1556 日志表
   不允许加锁 / 只读副本 1290），也不会把相同行重复写进管理库——旧实现每周期全量
   重采，一旦清空失败就会无限重复采集。
3. 清空节点侧 mysql.slow_log 降级为「尽力而为」：
   - 只读副本（read_only / super_read_only）直接跳过，不再报 1290；
   - 失败时按节点限频打印（默认 6 小时一次），不再每周期刷屏。

若节点未开启慢日志（表不存在/无权限），静默跳过（best-effort），不影响其他节点。
"""
import time

from .db import query, execute, get_setting
from .ha_client import connect_robust
from .clusters import get_cluster_creds
from .node_gen import list_nodes
from .config import SLOWLOG_ENABLED

# 清空失败的限频窗口（秒）：同一节点在该窗口内只提示一次
_CLEAR_FAIL_GAP = 6 * 3600
_clear_fail_at = {}


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


_SLOW_COLS = ("start_time, query_time, lock_time, rows_sent, rows_examined, db, sql_text")


def _ensure_cursor_table():
    """增量采集游标表（对已部署环境做幂等补建，主定义在 db.SCHEMA_SQL）。"""
    try:
        execute(
            "CREATE TABLE IF NOT EXISTS slow_log_cursor ("
            "  cluster_id INT NOT NULL,"
            "  host VARCHAR(64) NOT NULL,"
            "  port INT NOT NULL,"
            "  last_start_time DATETIME(6) NULL,"
            "  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            "  PRIMARY KEY (cluster_id, host, port)"
            ") ENGINE=InnoDB"
        )
    except Exception:
        pass


def _get_cursor(cid, ip, port):
    try:
        rows = query(
            "SELECT last_start_time FROM slow_log_cursor "
            "WHERE cluster_id=%s AND host=%s AND port=%s", (cid, ip, port))
        return rows[0]["last_start_time"] if rows else None
    except Exception:
        return None


def _set_cursor(cid, ip, port, ts):
    try:
        execute(
            "INSERT INTO slow_log_cursor (cluster_id, host, port, last_start_time) "
            "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE last_start_time=VALUES(last_start_time)",
            (cid, ip, port, ts))
    except Exception as e:
        print(f"[slow_log] 游标写入失败(可忽略): {e}")


def _is_writable(cur):
    """判断当前连接是否可写（副本 read_only 时清空日志表必然失败，直接跳过）。"""
    try:
        cur.execute("SELECT @@read_only, @@super_read_only")
        row = cur.fetchone() or (0, 0)
        ro = str(row[0] if row[0] is not None else 0)
        sro = str(row[1] if len(row) > 1 and row[1] is not None else 0)
        return ro in ("0", "b'0'", "") and sro in ("0", "b'0'", "")
    except Exception:
        return True


def _clear_node_table(cid, ip, port, cur, n):
    """尽力清空节点侧 mysql.slow_log。失败不影响采集（已有游标去重）。"""
    try:
        cur.execute("DELETE FROM mysql.slow_log")
        _clear_fail_at.pop((cid, ip, port), None)
        return
    except Exception as e1:
        try:
            conn = cur.connection
            conn.rollback()
        except Exception:
            pass
    # ha_monitor 权限不足时回退节点 root
    try:
        rconn = connect_robust(ip, port, n.get("root_user") or "root", n.get("root_pass") or "")
        if rconn:
            try:
                rc = rconn.cursor()
                rc.execute("DELETE FROM mysql.slow_log")
                _clear_fail_at.pop((cid, ip, port), None)
                return
            finally:
                rconn.close()
    except Exception as e2:
        e1 = e2
    _warn_clear_fail(cid, ip, port, e1)


def _warn_clear_fail(cid, ip, port, err):
    """清空失败按节点限频提示：避免每周期刷屏（游标已保证不会重复采集）。"""
    key = (cid, ip, port)
    now = time.time()
    if now - _clear_fail_at.get(key, 0) < _CLEAR_FAIL_GAP:
        return
    _clear_fail_at[key] = now
    print(f"[slow_log] 节点 {ip}:{port} 清空慢日志表失败({err})；"
          f"已启用游标增量采集，不会重复入库。建议授予 ha_monitor DELETE，"
          f"或让节点定期轮转 mysql.slow_log。该提示每 {_CLEAR_FAIL_GAP // 3600} 小时最多一次。")


def collect_slow_log(cluster_id=None):
    """采集全部（或指定）集群节点的慢日志（增量）。返回本次新增条数。

    凭证：统一使用集群的 ha_monitor 账号（get_cluster_creds 返回，管理器确定持有）。

    增量语义：以 slow_log_cursor.last_start_time 为水位，只采集 start_time 更大的行。
    因此「清空节点侧日志表」从「必须成功」降级为「尽力而为」，清空失败也不会再
    造成管理库 slow_log 重复膨胀。
    """
    if not slowlog_enabled():
        return 0
    _ensure_cursor_table()
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

                last = _get_cursor(cid, ip, port)
                if last:
                    cur.execute(
                        f"SELECT {_SLOW_COLS} FROM mysql.slow_log "
                        f"WHERE start_time > %s ORDER BY start_time", (last,))
                else:
                    cur.execute(f"SELECT {_SLOW_COLS} FROM mysql.slow_log ORDER BY start_time")

                rows = cur.fetchall() or []
                max_ts = last
                for r in rows:
                    st = r[0]
                    execute(
                        "INSERT INTO slow_log "
                        "(cluster_id, host, port, query_time_ms, lock_time_ms, rows_sent, "
                        "rows_examined, db, sql_text) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (cid, ip, port, _to_ms(r[1]), _to_ms(r[2]),
                         int(r[3] or 0), int(r[4] or 0), r[5] or "", (r[6] or "")[:4000]),
                    )
                    total += 1
                    if st is not None and (max_ts is None or st > max_ts):
                        max_ts = st

                # 推进水位：只在确实采到新行时更新，避免节点表被清空后水位反而后退
                if rows and max_ts is not None:
                    _set_cursor(cid, ip, port, max_ts)

                # 清空节点侧慢日志表（best-effort）：只读副本直接跳过
                if _is_writable(cur):
                    _clear_node_table(cid, ip, port, cur, n)
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
