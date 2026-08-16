"""mysql_compat.py — v2.1 复制 SQL 兼容层。

目标：让 v2.0 的 Docker(8.0.x) 集群在复制相关 SQL 上行为「字节级不变」，
同时新增对 8.0.22+/8.4 的 REPLICATION SOURCE/REPLICA 新语法的支持（裸机/新集群）。

判定规则：
- 存储的 mysql_version 为 NULL（老集群）或不传 → 一律按旧语法 (CHANGE MASTER / SHOW SLAVE)。
- 实际节点连接：用 detect_syntax() 按 SELECT VERSION() 判定，最可靠。
  >= 8.0.22（含 8.1/8.2/8.3/8.4）走新语法，< 8.0.22 走旧语法。

本模块只「生成 SQL 字符串」，执行交给调用方用 text() 包一层，避免重复依赖。
"""
import re

# 新复制术语 (CHANGE REPLICATION SOURCE / SHOW REPLICA) 在 8.0.22 引入
_NEW_SYNTAX_MIN = (8, 0, 22)


def parse_version(version_str):
    """'8.0.36-cluster' -> (8,0,36)；无法解析返回 (0,0,0)。"""
    if not version_str:
        return (0, 0, 0)
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)", version_str)
    if not m:
        return (0, 0, 0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def use_new_syntax(version_str=None):
    """按存储的 mysql_version 判定（None/空=旧语法，兼容 v2.0 老集群）。"""
    if not version_str:
        return False
    return parse_version(version_str) >= _NEW_SYNTAX_MIN


def detect_syntax(cur):
    """给定节点游标（pymysql cursor 或 sqlalchemy connection 均可用），
    按 SELECT VERSION() 判定语法；失败回退旧语法。

    注：传入的是「游标/连接」，函数会原地执行一条 SELECT VERSION() 并 fetch，
    调用方应在已有游标上调用，不影响后续语句。
    """
    try:
        cur.execute("SELECT VERSION()")
        row = cur.fetchone()
        v = parse_version(row[0]) if row else (0, 0, 0)
        return "new" if v >= _NEW_SYNTAX_MIN else "old"
    except Exception:
        return "old"


def _ch(channel):
    return f" FOR CHANNEL '{channel}'" if channel else ""


def change_replication_sql(syntax, host, port, user, password, channel=None):
    if syntax == "new":
        return (f"CHANGE REPLICATION SOURCE TO SOURCE_HOST='{host}', SOURCE_PORT={port}, "
                f"SOURCE_USER='{user}', SOURCE_PASSWORD='{password}', SOURCE_AUTO_POSITION=1{_ch(channel)}")
    return (f"CHANGE MASTER TO MASTER_HOST='{host}', MASTER_PORT={port}, "
            f"MASTER_USER='{user}', MASTER_PASSWORD='{password}', MASTER_AUTO_POSITION=1{_ch(channel)}")


def start_replication_sql(syntax, channel=None):
    return ("START REPLICA" if syntax == "new" else "START SLAVE") + _ch(channel)


def stop_replication_sql(syntax, channel=None):
    return ("STOP REPLICA" if syntax == "new" else "STOP SLAVE") + _ch(channel)


def reset_replica_sql(syntax, all_=True, channel=None):
    s = "RESET REPLICA" if syntax == "new" else "RESET SLAVE"
    if all_:
        s += " ALL"
    return s + _ch(channel)


def show_replica_status_sql(syntax, channel=None):
    return ("SHOW REPLICA STATUS" if syntax == "new" else "SHOW SLAVE STATUS") + _ch(channel)


def show_replicas_sql(syntax):
    return "SHOW REPLICAS" if syntax == "new" else "SHOW SLAVE HOSTS"


def change_replication_filter_sql(ignore_dbs, channel=None):
    """REPLICATE_IGNORE_DB 新旧语法通用。"""
    dbs = ",".join(f"({d})" for d in (ignore_dbs or []))
    return f"CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB={dbs}{_ch(channel)}"
