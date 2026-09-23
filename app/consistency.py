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
from .db import query, execute
from .state import get_current_master
from .clusters import get_cluster_creds
from .ha_client import connect_robust

# 比较时排除的库：
# - 4 个标准系统库（永不参与业务一致性比较）
# - ha_local：节点本地状态库。切换主库时把 current_master 写入【各节点本机】
#   ha_local.state，且以 SET sql_log_bin=0 写入（不参与复制），从库侧还加了
#   REPLICATE_IGNORE_DB=ha_local；因此主从的 ha_local 本就不一致——纳入比对会
#   恒定产生伪差异（典型表现：主库比从库多 1 行）。属设计使然，必须排除。
# - custos：管理器自身元数据库，若落在集群节点上同样不具业务一致性意义，排除。
SYSTEM_DBS = ("mysql", "information_schema", "performance_schema", "sys",
              "ha_local", "custos")

# 保护阈值：超过则跳过该表的 checksum（行数比对不受此限，代价极低）
MAX_TABLES = 300
CHECKSUM_MAX_ROWS = 20_000_000


def _connect(host, port, user, pwd):
    return connect_robust(host, port, user, pwd)


def _fetch_tables(conn, max_rows=None):
    """列出业务表 [(schema, table, rows)]，按行数升序（小表先比对，快出结果）。"""
    # 占位符数量必须随 SYSTEM_DBS【动态生成】：此前硬编码 4 个 %s，而 SYSTEM_DBS
    # 后来扩展到 6 项（新增 ha_local / custos），参数个数与占位符个数不符 ->
    # pymysql 抛 "not all arguments converted during string formatting"，
    # 一致性校验直接失败。动态生成后，今后增删 SYSTEM_DBS 都不会再犯。
    ph = ", ".join(["%s"] * len(SYSTEM_DBS))
    sql = ("SELECT table_schema, table_name, COALESCE(table_rows, 0) AS n "
           "FROM information_schema.tables "
           f"WHERE table_type='BASE TABLE' AND table_schema NOT IN ({ph})")
    if max_rows is not None:
        sql += " AND COALESCE(table_rows,0) <= %s"
        args = tuple(SYSTEM_DBS) + (int(max_rows),)
    else:
        args = tuple(SYSTEM_DBS)
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
    slave_status, divergence_windows = {}, {}
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
                _st = _replica_status(sconn)
                if _st:
                    slave_status["%s:%s" % (s_host, s_port)] = _st
                _dw = _divergence_window(cid, s_host, s_port)
                if _dw.get("available"):
                    divergence_windows["%s:%s" % (s_host, s_port)] = _dw
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
                        _cat, _sev, _note = _classify(mv, sv, mode)
                        _d = {"host": s_host, "port": s_port, "table": key,
                              "master": mv, "slave": sv, "kind": mode,
                              "category": _cat, "severity": _sev, "note": _note}
                        if _cat == "orphan":
                            _dwx = divergence_windows.get("%s:%s" % (s_host, s_port))
                            if _dwx and _dwx.get("last_interrupted_ts"):
                                _d["window_hint"] = (
                                    "分裂窗口嫌疑：复制最近一次异常于 %s（基于监控快照），"
                                    "此期间从库独有行可能是真实业务写入，删除前请先导出回灌主库。"
                                    % _dwx["last_interrupted_ts"])
                        diffs.append(_d)
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
            "diffs": diffs, "checked": checked, "slaves": done_slaves,
            "slave_status": slave_status, "divergence_windows": divergence_windows}


# ===================== abc / d 新增（行级差异、分叉窗口、导出回灌）=====================
# 仅追加，不改动上方既有只读比对逻辑。

MAX_ROWDIFF_KEYS = 2000   # 行级差异最多比对的主键数（防止超大表拖垮管理器/节点）
MAX_ROWDIFF_ROWS = 500    # 实际展开返回的行数上限


def _classify(mv, sv, mode):
    """把一处表级差异分类（c）：orphan=从库独有孤儿行 / missing=从库缺失 / content=内容差异。"""
    if mode == "checksum":
        return "content", "high", "校验和不一致（行数相同但内容有差异）"
    try:
        mi, si = int(mv), int(sv)
    except Exception:
        return "unknown", "medium", "数值无法解析"
    if si > mi:
        return "orphan", "high", "从库行数多于主库，疑似分裂窗口内的孤儿写入（可能是真实业务数据）"
    if si < mi:
        return "missing", "medium", "从库行数少于主库，可能复制未追平或数据缺失"
    return "unknown", "low", "行数一致"


def _replica_status(conn):
    """读取从库复制状态（b 标注用）。兼容 MySQL / MariaDB 的 SHOW REPLICA/SLAVE STATUS。"""
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SHOW REPLICA STATUS")
            except Exception:
                cur.execute("SHOW SLAVE STATUS")
            rows = cur.fetchall()
        if not rows:
            return {}
        r = rows[0]

        def g(*ks):
            for k in ks:
                if k in r and r[k] is not None:
                    return r[k]
            return None

        return {
            "io_running": g("Slave_IO_Running", "Replica_IO_Running"),
            "sql_running": g("Slave_SQL_Running", "Replica_SQL_Running"),
            "last_error": g("Last_SQL_Error", "Last_IO_Error",
                            "Replica_Last_SQL_Error", "Replica_Last_IO_Error"),
            "seconds_behind": g("Seconds_Behind_Master", "Seconds_Behind_Replica"),
        }
    except Exception:
        return {}


def _divergence_window(cid, host, port):
    """基于监控快照（metric_history）估算复制最近一次异常时段（b 分叉窗口标注）。

    返回 {available, last_interrupted_ts}；last_interrupted_ts 为最近一次
    slave_sql 非 Yes 的采样时间——分裂/中断往往在此期间发生，从库独有行嫌疑最大。
    """
    try:
        rows = query(
            "SELECT ts, slave_sql FROM metric_history "
            "WHERE cluster_id=%s AND host=%s AND port=%s ORDER BY ts DESC LIMIT 200",
            (cid, host, int(port)))
        if not rows:
            return {"available": False}
        last_bad = None
        for r in rows:
            if r.get("slave_sql") not in ("Yes", None):
                last_bad = r.get("ts")
                break
        return {"available": True,
                "last_sample_ts": rows[0].get("ts"),
                "last_interrupted_ts": last_bad}
    except Exception:
        return {"available": False}


def _pk_columns(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT k.COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE k "
            "JOIN information_schema.TABLE_CONSTRAINTS t "
            "ON k.CONSTRAINT_NAME=t.CONSTRAINT_NAME AND k.TABLE_SCHEMA=t.TABLE_SCHEMA "
            "AND k.TABLE_NAME=t.TABLE_NAME "
            "WHERE t.CONSTRAINT_TYPE='PRIMARY KEY' AND k.TABLE_SCHEMA=%s AND k.TABLE_NAME=%s "
            "ORDER BY k.ORDINAL_POSITION", (schema, table))
        return [r["COLUMN_NAME"] for r in cur.fetchall()]


def _table_columns(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (schema, table))
        return [r["COLUMN_NAME"] for r in cur.fetchall()]


def row_diff(cid, host, port, schema, table):
    """（a）展开某张差异表的行级差异：主键集合比对 + 两侧完整行。

    返回 {ok, pk_cols, columns, only_in_master:[...], only_in_slave:[...],
          master_count, slave_count, truncated, error}
    only_in_slave 即从库独有行（分裂窗口内真实写入嫌疑），UI 据此提示「先导出回灌」。
    无主键的表拒绝展开（避免不安全/不可靠的行级比对）。
    """
    m_host, m_port = get_current_master(cid)
    m_port = int(m_port or 3306)
    if not m_host:
        return {"ok": False, "error": "集群当前主库未知"}
    m_user, m_pass = _node_root(cid, m_host)
    s_user, s_pass = _node_root(cid, host)
    mconn = _connect(m_host, m_port, m_user, m_pass)
    sconn = _connect(host, int(port), s_user, s_pass)
    if not mconn or not sconn:
        return {"ok": False, "error": "无法连接主库或从库"}
    try:
        pk = _pk_columns(mconn, schema, table)
        if not pk:
            return {"ok": False, "error": "该表无主键，无法安全展开行级差异（请人工按业务键比对）"}
        cols = _table_columns(mconn, schema, table)
        pk_sql = ", ".join("`%s`" % c.replace("`", "``") for c in pk)
        col_sql = ", ".join("`%s`" % c.replace("`", "``") for c in cols)

        def _keys(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT %s FROM `%s`.`%s`" % (
                    pk_sql, schema.replace("`", "``"), table.replace("`", "``")))
                return set(tuple(r) for r in cur.fetchall())

        m_keys, s_keys = _keys(mconn), _keys(sconn)
        only_master = m_keys - s_keys
        only_slave = s_keys - m_keys
        truncated = False
        if len(only_master) > MAX_ROWDIFF_KEYS or len(only_slave) > MAX_ROWDIFF_KEYS:
            truncated = True
            only_master = set(list(only_master)[:MAX_ROWDIFF_KEYS])
            only_slave = set(list(only_slave)[:MAX_ROWDIFF_KEYS])

        def _fetch(conn, keyset):
            out = []
            for k in list(keyset)[:MAX_ROWDIFF_ROWS]:
                where = " AND ".join(
                    "`%s`=%%s" % c.replace("`", "``") for c in pk)
                with conn.cursor() as cur:
                    cur.execute("SELECT %s FROM `%s`.`%s` WHERE %s" % (
                        col_sql, schema.replace("`", "``"),
                        table.replace("`", "``"), where), k)
                    row = cur.fetchone()
                    if row:
                        out.append(dict(row))
            return out

        return {"ok": True, "pk_cols": pk, "columns": cols,
                "only_in_master": _fetch(mconn, only_master),
                "only_in_slave": _fetch(sconn, only_slave),
                "master_count": len(m_keys), "slave_count": len(s_keys),
                "truncated": truncated}
    except Exception as e:
        return {"ok": False, "error": "%s: %s" % (type(e).__name__, e)}
    finally:
        for c in (mconn, sconn):
            try:
                c.close()
            except Exception:
                pass


RECYCLE_TABLE = "_custos_recycle"


def _ensure_recycle():
    """在管理库创建回收表（幂等）：存「以主库为准收敛」时将被动掉的从库独有行，便于一键回灌主库。"""
    try:
        execute(
            "CREATE TABLE IF NOT EXISTS `%s` ("
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, cluster_id INT, host VARCHAR(64), port INT, "
            "`schema` VARCHAR(64), `table` VARCHAR(64), pk_cols TEXT, rows_json MEDIUMTEXT, "
            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, "
            "status VARCHAR(16) DEFAULT 'quarantined', "
            "INDEX idx_host (cluster_id, host, port)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            % RECYCLE_TABLE)
    except Exception as e:
        print("[consistency] 创建回收表失败(可忽略): %s" % e)


def _sqlval(v):
    if v is None:
        return "NULL"
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            v = repr(v)
    if isinstance(v, str):
        return "'%s'" % v.replace("'", "''")
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    return "'%s'" % str(v).replace("'", "''")


def export_divergent_rows(cid, host, port, schema, table):
    """（d）生成差异处置 SQL 并落 quarantine。

    返回 {ok, orphan_rows, remaster_sql, converge_sql, recycle_id, pk_cols, columns, truncated, error}
    - orphan_rows: 从库独有行（分裂窗口嫌疑真实写入），完整列值。
    - remaster_sql: 把这些行 INSERT 回【当前主库】（推荐，零丢失，复制自然下发）。
    - converge_sql: 在【从库】上 DELETE 这些行（以主库为准收敛，危险，已 quarantine 兜底）。
    默认动作 = 导出/回灌优先；收敛删除仅作带闸门的子动作。
    """
    rd = row_diff(cid, host, port, schema, table)
    if not rd.get("ok"):
        return {"ok": False, "error": rd.get("error", "行级差异失败")}
    orphans = rd["only_in_slave"]
    if not orphans:
        return {"ok": True, "orphan_rows": [], "remaster_sql": "", "converge_sql": "",
                "recycle_id": None, "note": "该表无非主库侧独有的孤儿行，无需导出。"}
    pk = rd["pk_cols"]
    cols = rd["columns"]
    pk_sql = ", ".join("`%s`" % c.replace("`", "``") for c in pk)
    col_sql = ", ".join("`%s`" % c.replace("`", "``") for c in cols)
    remaster_lines = ["-- 1) 回灌当前主库（推荐：救回分裂窗口内的真实写入，复制自然下发到各节点）",
                      "-- 在【当前主库】执行："]
    converge_lines = ["-- 2) 以主库为准收敛（危险：删除从库独有行；已存入回收表可一键回灌）",
                      "-- 在【从库 %s:%s】执行：" % (host, port)]
    for row in orphans:
        vals = tuple(row.get(c) for c in cols)
        remaster_lines.append("INSERT INTO `%s`.`%s` (%s) VALUES (%s);" % (
            schema.replace("`", "``"), table.replace("`", "``"), col_sql,
            ", ".join(_sqlval(v) for v in vals)))
        where = " AND ".join(
            "`%s`=%s" % (c.replace("`", "``"), _sqlval(row.get(c))) for c in pk)
        converge_lines.append("DELETE FROM `%s`.`%s` WHERE %s;" % (
            schema.replace("`", "``"), table.replace("`", "``"), where))
    remaster_sql = "\n".join(remaster_lines)
    converge_sql = "\n".join(converge_lines)
    recycle_id = None
    try:
        import json
        _ensure_recycle()
        recycle_id = execute(
            "INSERT INTO `%s` (cluster_id, host, port, `schema`, `table`, pk_cols, rows_json) "
            "VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % RECYCLE_TABLE,
            (cid, host, int(port), schema, table,
             json.dumps(pk, ensure_ascii=False),
             json.dumps(orphans, ensure_ascii=False, default=str)))
    except Exception as e:
        print("[consistency] 回收表写入失败(可忽略，导出仍可用): %s" % e)
    return {"ok": True, "orphan_rows": orphans, "remaster_sql": remaster_sql,
            "converge_sql": converge_sql, "recycle_id": recycle_id,
            "pk_cols": pk, "columns": cols, "truncated": rd.get("truncated")}


def remaster_from_recycle(recycle_id):
    """把回收表中的从库独有行回灌到【当前主库】（救回真实写入）。

    用 INSERT ... ON DUPLICATE KEY UPDATE 避免主键冲突（若已通过其它途径落主库）。
    成功后将回收记录标记为 remastered。
    """
    rows = query("SELECT * FROM `%s` WHERE id=%%s" % RECYCLE_TABLE, (recycle_id,))
    if not rows:
        return {"ok": False, "error": "回收记录不存在"}
    rec = rows[0]
    cid = rec["cluster_id"]
    schema, table = rec["schema"], rec["table"]
    import json
    pk = json.loads(rec["pk_cols"])
    orphans = json.loads(rec["rows_json"])
    m_host, m_port = get_current_master(cid)
    m_port = int(m_port or 3306)
    if not m_host:
        return {"ok": False, "error": "集群当前主库未知"}
    m_user, m_pass = _node_root(cid, m_host)
    mconn = _connect(m_host, m_port, m_user, m_pass)
    if not mconn:
        return {"ok": False, "error": "无法连接当前主库"}
    cols = _table_columns(mconn, schema, table)
    col_sql = ", ".join("`%s`" % c.replace("`", "``") for c in cols)
    upd_sql = ", ".join("`%s`=VALUES(`%s`)" % (c.replace("`", "``"), c.replace("`", "``"))
                        for c in cols)
    applied = 0
    try:
        with mconn.cursor() as cur:
            for row in orphans:
                vals = tuple(row.get(c) for c in cols)
                cur.execute(
                    "INSERT INTO `%s`.`%s` (%s) VALUES (%s) "
                    "ON DUPLICATE KEY UPDATE %s" % (
                        schema.replace("`", "``"), table.replace("`", "``"), col_sql,
                        ", ".join(["%s"] * len(cols)), upd_sql), vals)
                applied += 1
        mconn.commit()
        try:
            execute("UPDATE `%s` SET status='remastered' WHERE id=%%s" % RECYCLE_TABLE,
                    (recycle_id,))
        except Exception:
            pass
        return {"ok": True, "applied": applied,
                "note": "已回灌主库 %d 行，复制将自动下发到各节点。" % applied}
    except Exception as e:
        return {"ok": False, "error": "%s: %s" % (type(e).__name__, e)}
    finally:
        try:
            mconn.close()
        except Exception:
            pass
