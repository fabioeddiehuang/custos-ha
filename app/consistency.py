"""consistency.py — 主从数据一致性校验（纯 SQL，不引入外部依赖）。

背景
----
复制报错（1062/1032）只是数据分歧的**结果**，等它出现时分歧往往已存在很久。
本模块主动比对主从，把分歧在酿成故障前暴露出来。

两种模式
--------
- rows（默认，轻量）：比对各业务表行数。可定期巡检，代价极小。
- checksum（较重）：CHECKSUM TABLE 逐表比对，能发现「行数相同但内容不同」。
  InnoDB 上 CHECKSUM TABLE 需逐行计算，大表代价高，故仅按需手动触发。

安全边界
--------
- 只读：全部为 SELECT / CHECKSUM，绝不写任何节点。
- 表数量与单表行数均有上限，避免大集群把管理库或节点拖垮。
"""
from .db import query
from .state import get_current_master
from .clusters import get_cluster_creds
from .ha_client import connect_robust

SYSTEM_DBS = ("mysql", "information_schema", "performance_schema", "sys")

# 保护阈值：超过则跳过该表的 checksum（行数比对不受此限，代价极低）
MAX_TABLES = 300
CHECKSUM_MAX_ROWS = 20_000_000


def _connect(host, port, user, pwd):
    return connect_robust(host, port, user, pwd)


def _fetch_tables(conn, max_rows=None):
    """列出业务表 [(schema, table, rows)]，按行数升序（小表先比对，快出结果）。"""
    sql = ("SELECT table_schema, table_name, COALESCE(table_rows, 0) AS n "
           "FROM information_schema.tables "
           "WHERE table_type='BASE TABLE' AND table_schema NOT IN (%s,%s,%s,%s)")
    if max_rows is not None:
        sql += " AND COALESCE(table_rows,0) <= %s"
        args = SYSTEM_DBS + (int(max_rows),)
    else:
        args = SYSTEM_DBS
    sql += " ORDER BY n"
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return [(r[0], r[1], int(r[2] or 0)) for r in cur.fetchall()]


def _count(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM `%s`.`%s`" % (schema.replace("`", "``"),
                                                        table.replace("`", "``")))
        r = cur.fetchone()
        return int(r[0]) if r else None


def _checksum(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("CHECKSUM TABLE `%s`.`%s`" % (schema.replace("`", "``"),
                                                  table.replace("`", "``")))
        r = cur.fetchone()
        # 结果形如 ('db.tbl', 123456789) 或 (..., NULL)
        return (r[1] if r and len(r) > 1 else None)


def _node_root(cid, host):
    rows = query("SELECT root_user, root_pass FROM nodes WHERE cluster_id=%s AND ip=%s",
                 (cid, host))
    if rows:
        return (rows[0].get("root_user") or "root"), (rows[0].get("root_pass") or "")
    return "root", ""


def check_consistency(cid, mode="rows"):
    """比对主库与各从库的数据一致性。

    返回 {"ok", "mode", "master", "diffs":[...], "checked": 表数, "slaves": 比对过的从库}
    diffs 每项：{"host","port","table","master","slave","kind":"rows"|"checksum"}
    """
    mode = "checksum" if str(mode).lower() == "checksum" else "rows"
    m_host, m_port = get_current_master(cid)
    m_port = int(m_port or 3306)
    if not m_host:
        return {"ok": False, "error": "集群当前主库未知"}

    m_user, m_pass = _node_root(cid, m_host)
    slaves = query("SELECT host, port FROM hosts WHERE cluster_id=%s "
                   "AND is_current_master=0", (cid,)) or []

    mconn = _connect(m_host, m_port, m_user, m_pass)
    if not mconn:
        return {"ok": False, "error": f"无法连接主库 {m_host}:{m_port}"}

    diffs, checked, done_slaves = [], 0, []
    try:
        max_rows = CHECKSUM_MAX_ROWS if mode == "checksum" else None
        tables = _fetch_tables(mconn, max_rows)[:MAX_TABLES]

        # 主库侧先算一遍，避免每个从库重复计算
        m_vals = {}
        for sc, tb, _n in tables:
            key = "%s.%s" % (sc, tb)
            try:
                m_vals[key] = (_checksum(mconn, sc, tb) if mode == "checksum"
                               else _count(mconn, sc, tb))
            except Exception:
                m_vals[key] = None
        checked = len(tables)

        for s in slaves:
            s_host, s_port = s["host"], int(s["port"])
            s_user, s_pass = _node_root(cid, s_host)
            sconn = _connect(s_host, s_port, s_user, s_pass)
            if not sconn:
                diffs.append({"host": s_host, "port": s_port, "table": "-",
                              "master": None, "slave": None, "kind": "connect",
                              "note": "无法连接从库"})
                continue
            try:
                for sc, tb, _n in tables:
                    key = "%s.%s" % (sc, tb)
                    mv = m_vals.get(key)
                    try:
                        sv = (_checksum(sconn, sc, tb) if mode == "checksum"
                              else _count(sconn, sc, tb))
                    except Exception as e:
                        diffs.append({"host": s_host, "port": s_port, "table": key,
                                      "master": mv, "slave": None, "kind": mode,
                                      "note": f"从库读取失败：{str(e)[:120]}"})
                        continue
                    if mv is None or sv is None:
                        continue
                    if str(mv) != str(sv):
                        diffs.append({"host": s_host, "port": s_port, "table": key,
                                      "master": mv, "slave": sv, "kind": mode,
                                      "note": ("行数不一致" if mode == "rows"
                                               else "校验和不一致（内容有差异）")})
            finally:
                try:
                    sconn.close()
                except Exception:
                    pass
            done_slaves.append(f"{s_host}:{s_port}")
    finally:
        try:
            mconn.close()
        except Exception:
            pass

    return {"ok": True, "mode": mode, "master": f"{m_host}:{m_port}",
            "diffs": diffs, "checked": checked, "slaves": done_slaves}
