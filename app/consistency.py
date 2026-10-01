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

⚠ 结论的可复现性（2026-09-23 生产事故后重写，务必先读）
--------------------------------------------------------
生产现场曾出现「同一集群连续两次校验和比对结果不同」「切换主库后结果又变」，根因有 5 条，
且互相叠加。任何一条不治，结论都不可信：

1. **采样不同步**：旧实现先算完主库【全部表】的值，再逐从库算从库的值——两次读取之间存在
   时间差 Δ。生产库持续有写入（或有复制延迟）时，Δ 内发生的写入必然造成「差异」；而 Δ 与
   写入时机每次都不同 ⇒ **结果不可复现**。这是「两次结果不同」的第一因。
   本版对策：先做判定前门禁（复制是否健康/是否追平），未追平不下数据结论；候选差异一律
   做二次复核，只有复核仍不一致才定案。
2. **无延迟感知**：旧实现读了 Seconds_Behind 却只拿去展示，从不参与判定，于是「从库落后 5 秒」
   也会被判成「内容差异(high)」。（migration.verify() 早已修过这个反模式，但本函数没有同步——
   同一件事两条代码路径能力不对等，是本项目反复出现的元凶。）
   本版对策：`_caught_up()` 门禁 + `_wait_catch_up()` 有界等待，未追平的差异归入 transient。
3. **无复核**：旧实现首次不一致即定案，瞬态差异被当成事实差异。
   本版对策：二次复核（追平后重算两侧），复核一致则归入 transient 并说明原因。
4. **从库集合口径错**：旧实现用 hosts.is_current_master=0 挑从库。该标记由 monitor 每周期
   校准一次，**刚切换主库的那个窗口里它还是旧值** ⇒ 新主可能被当成「从库」与自己比对
   （结果必然 0 差异），而真正的从库反被排除。这正是现场「再切 188 就显示未发现差异」的机制。
   本版对策：从库集合 = 集群内除【current_master 权威记录】之外的节点，不依赖 is_current_master；
   该标记与权威口径不一致时写进结果并在页面提示。
5. **覆盖静默**：跳过 >MAX_TABLES 张表 / checksum 模式下跳过超大表时旧实现完全静默，且跳过
   顺序按 information_schema.table_rows **估算值**升序——先丢的恰恰是最大的表，而估算值会漂移
   ⇒ 比较的表集合每次都不同，又一条不可复现来源。
   本版对策：覆盖情况、跳过清单与原因全部写进结果并在页面展示；覆盖不全时结论降级为 partial，
   **绝不宣称「一致」**。

处置方向（与 README/手册一致）：
- **权威副本 = 当前主库**。从库独有行（orphan：主库没有、仅从库存在的行）可能是切换窗口内的真实业务写入，必须先导出
  回灌主库（零丢失），**不得直接以主库为准删除**。
- 从库缺失（missing）在复制已追平时才说明真有缺口；未追平时应先等追平。
"""
import time
from datetime import datetime

from pymysql.cursors import DictCursor

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
# - test：MariaDB/MySQL 安装默认带出的空库（裸机有、容器版没有），属安装产物而非业务库，
#   排除以免 bare-metal/容器混合部署时把默认 test 的异质误判为业务分叉。
SYSTEM_DBS = ("mysql", "information_schema", "performance_schema", "sys",
              "ha_local", "custos", "test")

# 保护阈值：超过则跳过该表的 checksum（行数比对不受此限，代价极低）
MAX_TABLES = 300
CHECKSUM_MAX_ROWS = 20_000_000

# 候选差异复核前，等待从库追平的秒数上限（0=不等，直接按当前状态判定）。
# 追平是「差异能否被确认为数据问题」的前提：没追平的差异随时可能自然消失。
CONSISTENCY_WAIT_SEC = 20

# 校验结论持久化表（管理库）。用于页面状态与最后一次结论联动：
# 只要存在未处置的差异，集群页就不允许显示「复制正常」。
RESULT_TABLE = "_custos_consistency_runs"


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


def _list_tables(conn):
    """该节点上的业务表集合 set((schema, table))，与 _fetch_tables 同一套排除规则。

    用途：表清单级差异（从库多表/缺表）——旧实现只枚举主库的表，从库上多出来的表
    完全看不见，是最容易漏掉的一类分歧。
    """
    return set((sc, tb) for sc, tb, _n in _fetch_tables(conn, None))


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


def _value(conn, schema, table, mode):
    """按模式取一个表的可比对值（计数或校验和）。"""
    return _checksum(conn, schema, table) if mode == "checksum" else _count(conn, schema, table)


def _node_root(cid, host):
    rows = query("SELECT root_user, root_pass FROM nodes WHERE cluster_id=%s AND ip=%s",
                 (cid, host))
    if rows:
        return (rows[0].get("root_user") or "root"), (rows[0].get("root_pass") or "")
    return "root", ""


# ===================== 复制健康状况子函数 =====================

def _is_yes(v):
    return str(v or "").strip().lower() in ("yes", "on", "1", "true")


def _caught_up(st):
    """从库是否「复制线程都在跑、且已追平」。返回 True/False/None(未知)。

    seconds_behind 为 0 或 NULL 视为已追平（NULL 通常表示 SQL 线程空闲、无积压）。
    读不到复制状态（非从库/权限不足）返回 None —— 未知时不下数据结论，避免冤枉。
    """
    if not st:
        return None
    if not (_is_yes(st.get("io_running")) and _is_yes(st.get("sql_running"))):
        return False
    sb = st.get("seconds_behind")
    try:
        return int(sb or 0) == 0
    except (TypeError, ValueError):
        return False


def _wait_catch_up(conn, seconds=CONSISTENCY_WAIT_SEC):
    """在有界时间内等待从库追平，返回最终复制状态。

    seconds=0 时只读一次当前状态（不等待）——已追平的从库无需等待，避免拖慢校验。
    """
    st = _replica_status(conn)
    deadline = time.monotonic() + max(0, int(seconds))
    while time.monotonic() < deadline and not _caught_up(st):
        if _caught_up(st) is None:
            break                    # 状态读不到，等下去也没意义
        time.sleep(1.0)
        st = _replica_status(conn)
    return st


def _replica_status(conn):
    """读取从库复制状态。兼容 MySQL / MariaDB 的 SHOW REPLICA/SLAVE STATUS。

    额外返回 position：用于判断「复核期间该从库是否仍在应用新事务」——
    位置在变说明采样窗口不稳定，是瞬态差异的判定依据（见模块头 ⚠ 第 1/3 条）。
    """
    # ⚠ 必须显式用 DictCursor：ha_client.connect 建立的是【默认元组游标】连接
    #   （只有管理库 db.py 用 DictCursor）。下面按列名取值在元组游标上永远取不到，
    #   函数会静默返回「全 None」—— 本函数此前正是如此：既让复制健康度无法参与判定，
    #   也让「不一致时段标注」从未真正生效。
    try:
        with conn.cursor(DictCursor) as cur:
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

        # 应用位点：GTID 优先（跨节点可比），退化为「中继日志文件:位点」。
        # ⚠ 两个来源都取不到时必须给空串，不能给 ":"—— 否则「前后都是空」会被误判成
        #   「位点未变化」（假的稳定性证据），把采样时差当成已确认差异。
        pos = g("Gtid_Slave_Pos", "Executed_Gtid_Set", "Gtid_IO_Pos")
        if not pos:
            _f, _p = g("Relay_Master_Log_File"), g("Exec_Master_Log_Pos")
            pos = ("%s:%s" % (_f, _p)) if (_f or _p) else ""
        return {
            "io_running": g("Slave_IO_Running", "Replica_IO_Running"),
            "sql_running": g("Slave_SQL_Running", "Replica_SQL_Running"),
            "last_error": g("Last_SQL_Error", "Last_IO_Error",
                            "Replica_Last_SQL_Error", "Replica_Last_IO_Error"),
            "seconds_behind": g("Seconds_Behind_Master", "Seconds_Behind_Replica"),
            "position": pos,
        }
    except Exception:
        return {}


def _divergence_window(cid, host, port):
    """基于监控快照（metric_history）估算复制最近一次异常时段（不一致时段标注）。

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


def _classify(mv, sv, mode):
    """把一处表级差异分类：orphan=从库独有行（主库没有） / missing=从库缺失 / content=内容差异。

    ⚠ checksum 模式下**不臆断行数关系**：旧实现在这里直接断言「行数相同但内容有差异」，
    而它根本没读过行数——这是一句假话，会误导处置方向。真正的分类由 _classify_checked()
    在 checksum 模式下补取行数后给出。
    """
    if mode == "checksum":
        return "content", "high", "校验和不一致（内容有差异）"
    try:
        mi, si = int(mv), int(sv)
    except Exception:
        return "unknown", "medium", "数值无法解析"
    if si > mi:
        return "orphan", "high", "从库行数多于主库：这些是主库没有、仅从库存在的行（可能是切换窗口内的真实业务写入）"
    if si < mi:
        return "missing", "medium", "从库行数少于主库，可能复制未追平或数据缺失"
    return "unknown", "low", "行数一致"


def _classify_checked(mv, sv, mode, mconn, sconn, schema, table):
    """分类（含 checksum 模式的补取行数）。

    checksum 模式下只对【差异表】补取两侧 COUNT，代价随差异数有界；
    这样 orphan/missing/content 的语义在两种模式下都准确，用户才知道该救数据还是该等追平。
    """
    if mode != "checksum":
        return _classify(mv, sv, mode)
    try:
        mi = _count(mconn, schema, table)
        si = _count(sconn, schema, table)
    except Exception:
        return ("content", "high",
                "校验和不一致（内容有差异；行数未能取得，无法判断哪一侧数据更多）")
    _cat, _sev, _note = _classify(mi, si, "rows")
    if _cat == "orphan":
        return ("orphan", "high",
                "校验和不一致，且从库行数多于主库（从库独有数据，疑似真实写入）")
    if _cat == "missing":
        return ("missing", "medium",
                "校验和不一致，且从库行数少于主库（复制未追平或数据缺失）")
    return ("content", "high", "校验和不一致，行数相同但内容有差异")


def check_consistency(cid, mode="rows", confirm=True, wait_sec=None):
    """比对主库与各从库的数据一致性（只读）。

    返回
    ----
    {ok, mode, ts, master, authoritative, verdict, verdict_text,
     diffs:[仅【已确认】的差异], transient:[候选但未确认], checked, tables_total,
     skipped:[{table,reason,est_rows}], coverage_text, slaves, slave_status,
     divergence_windows, flag_mismatch, repl_broken, notes}

    verdict 取值与含义（页面据此显示，务必区分「有数据问题」与「没判定」）：
    - inconsistent     有已确认差异（或从库缺表/多表/连不上）——需要人工处置
    - inconclusive     有候选差异但未获确认（从库未追平 / 复核后已一致）——不构成数据问题结论
    - repl_broken      复制中断，无法比对——先修复制
    - partial          复制正常且未见差异，但覆盖不全（跳过表）——不得宣称「一致」
    - consistent       复制正常、追平，且覆盖范围内确认一致
    """
    mode = "checksum" if str(mode).lower() == "checksum" else "rows"
    wait_sec = CONSISTENCY_WAIT_SEC if wait_sec is None else int(wait_sec)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---- 权威口径：以 current_master 表为准；从库 = 其余节点（不依赖 is_current_master）----
    m_host, m_port = get_current_master(cid)
    m_port = int(m_port or 3306)
    if not m_host:
        return {"ok": False, "error": "集群当前主库未知"}

    m_user, m_pass = _node_root(cid, m_host)
    host_rows = query("SELECT host, port, COALESCE(is_current_master,0) AS flag "
                      "FROM hosts WHERE cluster_id=%s", (cid,)) or []
    slaves = [r for r in host_rows
              if not (r["host"] == m_host and int(r["port"]) == m_port)]
    # 已删除节点的 hosts 残留行不参与校验（P1，2026-10-02 实测）：
    # 这类行不在 nodes 表里、也永远连不上，会让「刚删掉一个节点」把整集群的一致性校验
    # 直接顶成失败——用户看到的是「校验不可用」，而不是「那个节点不存在」。故先按
    # nodes 表过滤，并在 notes 里说明跳过了什么（保持覆盖透明）。
    try:
        _valid = set((r["ip"], int(r["port"])) for r in (
            query("SELECT ip, port FROM nodes WHERE cluster_id=%s", (cid,)) or []))
    except Exception:
        _valid = set()
    _stale_note = None
    if _valid:
        _stale = [r for r in slaves if (r["host"], int(r["port"])) not in _valid]
        if _stale:
            _stale_note = (
                "已跳过 %d 个不在节点表中的残留状态条目（%s）：多为已删除节点的历史行，"
                "不参与本次一致性判定。" % (
                    len(_stale),
                    ", ".join("%s:%s" % (r["host"], r["port"]) for r in _stale[:5])))
        slaves = [r for r in slaves if (r["host"], int(r["port"])) in _valid]
    # is_current_master 标记与权威口径不一致的节点（切换后 monitor 尚未校准的窗口期）。
    # 旧实现正是用这个标记挑从库，会在该窗口把新主当从库比、把真从库漏掉。
    flag_mismatch = ["%s:%s(标记=%d)" % (r["host"], r["port"], int(r["flag"] or 0))
                     for r in host_rows
                     if (int(r["flag"] or 0) == 1) != (r["host"] == m_host and int(r["port"]) == m_port)]

    mconn = _connect(m_host, m_port, m_user, m_pass)
    if not mconn:
        return {"ok": False, "error": f"无法连接主库 {m_host}:{m_port}"}

    diffs, transient, checked = [], [], 0
    slave_status, divergence_windows, done_slaves = {}, {}, []
    repl_broken, notes = [], []
    if _stale_note:
        notes.append(_stale_note)
    tables_total, skipped = 0, []
    try:
        all_tables = _fetch_tables(mconn, None)          # 全量（含估算行数），升序
        tables_total = len(all_tables)
        master_set = set((sc, tb) for sc, tb, _n in all_tables)

        # 比较范围（覆盖必须透明：跳过什么、为什么跳过，全部记入结果）
        tables = []
        for sc, tb, n in all_tables:
            if mode == "checksum" and n > CHECKSUM_MAX_ROWS:
                skipped.append({"table": "%s.%s" % (sc, tb), "reason": "too_large",
                                "est_rows": n})
                continue
            tables.append((sc, tb, n))
        if len(tables) > MAX_TABLES:
            for sc, tb, _n in tables[MAX_TABLES:]:
                skipped.append({"table": "%s.%s" % (sc, tb), "reason": "over_limit"})
            tables = tables[:MAX_TABLES]
        checked = len(tables)

        # 主库侧先算一遍，避免每个从库重复计算
        m_vals = {}
        for sc, tb, _n in tables:
            try:
                m_vals["%s.%s" % (sc, tb)] = _value(mconn, sc, tb, mode)
            except Exception:
                m_vals["%s.%s" % (sc, tb)] = None

        for s in slaves:
            s_host, s_port = s["host"], int(s["port"])
            s_user, s_pass = _node_root(cid, s_host)
            # _connect 失败会【抛异常】而非返回 None（连接库 connect_robust 的行为）。
            # 旧代码只判空，异常直接冒泡到 API —— 一个连不上的节点会让整集群校验失败、
            # 页面只看到一句连接错误。这里统一兜住，按「不可比对」处理，其余节点照常校验。
            _conn_err = ""
            try:
                sconn = _connect(s_host, s_port, s_user, s_pass)
            except Exception as e:
                sconn = None
                _conn_err = str(e)[:160]
            if not sconn:
                # 连不上=无法比对，按阻碍性差异上报（会顶成 inconsistent，避免「静默放过」）
                diffs.append({"host": s_host, "port": s_port, "table": "-",
                              "master": None, "slave": None, "kind": "connect",
                              "category": "unreachable", "severity": "high",
                              "confirmed": True,
                              "note": ("无法连接从库，本节点数据状态未知（并非「一致」）"
                                       + ("：%s" % _conn_err if _conn_err else ""))})
                continue
            try:
                st0 = _replica_status(sconn)
                if st0:
                    slave_status["%s:%s" % (s_host, s_port)] = st0
                _dw = _divergence_window(cid, s_host, s_port)
                if _dw.get("available"):
                    divergence_windows["%s:%s" % (s_host, s_port)] = _dw

                reachable = _caught_up(st0)
                # 「可比对」的前提 = 复制状态读得到、且 IO/SQL 线程都在跑（暂不论是否追平）。
                # 前提不成立时【不做任何数据判定】—— 否则「这个节点根本没在复制」会被渲染成
                # 成百上千处「数据差异」，比旧实现更吓人、更误导。
                comparable = bool(st0) and _is_yes(st0.get("io_running")) \
                    and _is_yes(st0.get("sql_running"))
                if not st0:
                    # 读不到复制状态：节点未配置复制、或账号权限受限。集群内非主节点本应
                    # 都在复制，读不到就必须明说「无法比对」，不能悄悄当成「一致」。
                    repl_broken.append("%s:%s（读不到复制状态/未配置复制）" % (s_host, s_port))
                    notes.append(
                        "从库 %s:%s 读不到复制状态（未配置复制或权限受限）：本次【不判定】"
                        "其数据是否一致。" % (s_host, s_port))
                elif reachable is False:
                    _err = (st0.get("last_error") or "").strip()[:160]
                    repl_broken.append("%s:%s" % (s_host, s_port))
                    notes.append(
                        "从库 %s:%s 复制未处于正常运行状态%s：本次【不判定】其数据是否一致"
                        "（先修复复制，再重跑校验）。"
                        % (s_host, s_port, ("（%s）" % _err) if _err else ""))

                # ---- 表清单级差异（旧实现完全看不到这类分歧）----
                # 仅在「可比对」时进行：不可比对时把 sc_tables 取成 master_set，使下面
                # 两侧差集都为空 —— 否则「节点根本没在复制」会被渲染成成百上千处
                # 「表缺失」差异，比旧实现更误导。
                try:
                    sc_tables = _list_tables(sconn) if comparable else master_set
                    for sc, tb in sorted(sc_tables - master_set):
                        diffs.append({"host": s_host, "port": s_port,
                                      "table": "%s.%s" % (sc, tb),
                                      "master": "表不存在", "slave": "表存在",
                                      "kind": "table", "category": "table_extra",
                                      "severity": "high", "confirmed": True,
                                      "note": "该表只存在于从库（主库无此表）——属结构级分歧，请人工确认"})
                    for sc, tb in sorted(master_set - sc_tables):
                        diffs.append({"host": s_host, "port": s_port,
                                      "table": "%s.%s" % (sc, tb),
                                      "master": "表存在", "slave": "表不存在",
                                      "kind": "table", "category": "table_missing",
                                      "severity": "high", "confirmed": True,
                                      "note": "该表在主库存在、从库缺失——复制未追平或数据丢失"})
                except Exception as e:
                    notes.append("从库 %s:%s 表清单枚举失败：%s" % (s_host, s_port, str(e)[:120]))

                # ---- 逐表比对，收集候选差异（不可比对时不做任何表内容判定）----
                cand = []
                for sc, tb, _n in (tables if comparable else []):
                    key = "%s.%s" % (sc, tb)
                    mv = m_vals.get(key)
                    try:
                        sv = _value(sconn, sc, tb, mode)
                    except Exception as e:
                        diffs.append({"host": s_host, "port": s_port, "table": key,
                                      "master": mv, "slave": None, "kind": mode,
                                      "category": "read_error", "severity": "medium",
                                      "confirmed": True,
                                      "note": f"从库读取失败：{str(e)[:120]}"})
                        continue
                    if mv is None or sv is None:
                        continue
                    if str(mv) != str(sv):
                        cand.append((sc, tb, key, mv, sv))

                # ---- 候选差异的复核（本版新增：把「瞬态时差」与「真实差异」分开）----
                # ⚠ 追平等待【每个从库只做一次】，绝不能放进下面的候选循环 —— 否则一个落后的
                #   从库若有 N 处候选，就要等 N × wait_sec，能把一次校验拖到十几分钟。
                st_now = st0
                if confirm and cand and _caught_up(st0) is not True:
                    st_now = _wait_catch_up(sconn, wait_sec)
                for sc, tb, key, mv, sv in cand:
                    if not confirm:
                        transient.append(_mk_t(s_host, s_port, key, mv, sv, mode,
                                               "未启用复核，候选差异未确认"))
                        continue
                    # ① 追平是前提：未追平则不做数据结论（等待已在上面统一做过一次）
                    if not _caught_up(st_now):
                        transient.append(_mk_t(
                            s_host, s_port, key, mv, sv, mode,
                            "从库未追平（延迟 %s 秒 / 复制线程未正常运行），本次不判定为数据差异"
                            % (st_now.get("seconds_behind") if st_now else "未知")))
                        continue
                    # ② 复核：反复重算，直到取到【稳定采样窗口】为止（有界重试）
                    #    稳定 = 本次读取前后该从库没有应用新事务（position 未变化）。
                    #    为什么必须要求稳定：主库持续写入时，「取主库值 → 取从库值」之间只要有
                    #    新写入落到主库，就会凭空产生一处「差异」。这正是结论来回摇摆的来源
                    #    （现场表现为同一集群两次校验结果不同、切换主库后又变）。
                    #    取舍：宁可在写入繁忙时报「未获结论，请复测」，也不放出会摇摆的结论。
                    _attempts = 3 if mode == "rows" else 2
                    _outcome, _reason, _miss = "", "", 0
                    mv2 = sv2 = None
                    for _i in range(_attempts):
                        st_before = _replica_status(sconn)
                        _pos_before = st_before.get("position") or ""
                        try:
                            mv2 = _value(mconn, sc, tb, mode)
                            sv2 = _value(sconn, sc, tb, mode)
                        except Exception as e:
                            _outcome, _reason = "error", "复核读取失败：%s" % str(e)[:120]
                            break
                        if str(mv2) == str(sv2):
                            _outcome = "transient"
                            _reason = ("首次不一致、复核已一致"
                                       "（采样时差或复制延迟所致，非数据问题）")
                            break
                        _miss += 1
                        _pos_after = _replica_status(sconn).get("position") or ""
                        if _pos_before and _pos_after and _pos_before == _pos_after:
                            _outcome = "confirmed"      # 采样窗口内从库没动 ⇒ 差异稳定
                            break
                        if not _pos_before and _miss >= 2:
                            # 取不到复制位点（个别版本/权限受限）时退化为「多次复现」判据，
                            # 既不至于永远拿不到结论，也不会凭一次读数就定案。
                            _outcome = "confirmed"
                            _reason = "（注：无法读取复制位点，稳定性以多次复现判定）"
                            break
                        _reason = "复核期间从库仍在应用新事务（写入繁忙），未能取得稳定采样窗口"
                        time.sleep(1.0)
                    if _outcome != "confirmed":
                        transient.append(_mk_t(s_host, s_port, key, mv, sv, mode,
                                               _reason or "复核未获结论"))
                        continue
                    # ③ 分类（checksum 模式补取行数，使 orphan/missing/content 语义准确）
                    _cat, _sev, _note = _classify_checked(
                        mv2, sv2, mode, mconn, sconn, sc, tb)
                    _d = {"host": s_host, "port": s_port, "table": key,
                          "master": mv2, "slave": sv2, "kind": mode,
                          "category": _cat, "severity": _sev, "note": _note,
                          "confirmed": True, "pos_stable": True}
                    if _reason:
                        _d["note"] += _reason
                    if _cat == "orphan":
                        _dwx = divergence_windows.get("%s:%s" % (s_host, s_port))
                        if _dwx and _dwx.get("last_interrupted_ts"):
                            _d["window_hint"] = (
                                "分裂窗口嫌疑：复制最近一次异常于 %s（基于监控快照），"
                                "此期间从库独有行可能是真实业务写入，处置前请先导出回灌主库。"
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

    # ---- 结论（可复现、可解释、不冤枉）----
    verdict, verdict_text = _verdict(diffs, transient, repl_broken, skipped, checked)
    if skipped:
        notes.append(
            "本次未覆盖 %d 张表（checksum 模式跳过超 %d 行的表 / 超过单次上限 %d 张）：%s"
            % (len(skipped), CHECKSUM_MAX_ROWS, MAX_TABLES,
               "、".join(x["table"] for x in skipped[:8]) + ("…" if len(skipped) > 8 else "")))
    if flag_mismatch:
        notes.append("当前主库标记与权威口径不一致（切换后的窗口期，monitor 下个周期会校准）：%s"
                     % "、".join(flag_mismatch))
    coverage_text = ("已比对 %d/%d 张表%s"
                     % (checked, tables_total,
                        "（覆盖不全，见跳过清单）" if skipped else "（覆盖完整）"))
    return {"ok": True, "mode": mode, "ts": ts,
            "master": f"{m_host}:{m_port}", "authoritative": f"{m_host}:{m_port}",
            "verdict": verdict, "verdict_text": verdict_text,
            "diffs": diffs, "transient": transient,
            "checked": checked, "tables_total": tables_total, "skipped": skipped,
            "coverage_text": coverage_text,
            "slaves": done_slaves, "slave_status": slave_status,
            "divergence_windows": divergence_windows,
            "flag_mismatch": flag_mismatch, "repl_broken": repl_broken,
            "notes": notes}


def _mk_t(host, port, table, mv, sv, mode, reason):
    """构造一条「候选但未确认」的差异（页面单列一区，不参与数据问题判定）。"""
    return {"host": host, "port": port, "table": table, "master": mv, "slave": sv,
            "kind": mode, "category": "transient", "severity": "low",
            "confirmed": False, "reason": reason, "note": reason}


def _verdict(diffs, transient, repl_broken, skipped, checked):
    """把结果归成一个明确结论 + 一句人话解释。

    顺序即优先级：**有已确认的实质问题 > 没判定 > 复制中断 > 覆盖不全 > 一致**。
    关键点：只要「没判定」就不能说一致；只要覆盖不全就不能说一致。
    """
    if diffs:
        _nodes = sorted(set("%s:%s" % (d["host"], d["port"]) for d in diffs))
        return ("inconsistent",
                "发现 %d 处【已确认】差异，涉及 %s；权威副本为当前主库，"
                "从库独有数据请先导出回灌主库（勿直接删除）。"
                % (len(diffs), "、".join(_nodes)))
    if transient:
        return ("inconclusive",
                "有 %d 处候选差异未获确认（从库未追平 / 复核后已一致），"
                "本次【不构成】数据不一致结论；建议待复制追平后重跑校验。" % len(transient))
    if repl_broken:
        return ("repl_broken",
                "从库 %s 复制未正常运行，无法比对；请先修复复制再重跑校验。"
                % "、".join(repl_broken))
    if skipped:
        return ("partial",
                "已比对范围内未发现差异，但本次未覆盖 %d 张表，"
                "【不能据此认定整个集群一致】；如需全量结论请缩小数据量或分批校验。"
                % len(skipped))
    return ("consistent",
            "覆盖 %d 张表全部确认一致，且复制处于正常运行/已追平状态。" % checked)


# ===================== 结论持久化（页面状态联动）=====================
# 为什么必须持久化：校验结论原本只出现在一个弹窗里，刷新页面就没了。于是「刚校验出
# 2 处差异」与「页面显示复制正常」可以同时成立，用户无从判断哪个库可信。现在把每次
# 结论落管理库，集群页据此显示状态徽章——有未处置的差异就不许显示正常。

def _ensure_result_table():
    """幂等创建校验结论表。"""
    execute(
        "CREATE TABLE IF NOT EXISTS `%s` ("
        "id BIGINT AUTO_INCREMENT PRIMARY KEY, cluster_id INT, ts DATETIME, "
        "mode VARCHAR(16), master VARCHAR(80), verdict VARCHAR(24), "
        "diffs INT, transient INT, checked INT, tables_total INT, skipped INT, "
        "detail_json MEDIUMTEXT, "
        "INDEX idx_cid (cluster_id, id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        % RESULT_TABLE)


def save_result(cid, res):
    """保存一次校验结论（失败不影响校验本身）。返回 id 或 None。"""
    try:
        import json
        _ensure_result_table()
        detail = dict(res)
        detail["diffs"] = (res.get("diffs") or [])[:50]      # 详情留存上限，避免表膨胀
        detail["transient"] = (res.get("transient") or [])[:50]
        return execute(
            "INSERT INTO `%s` (cluster_id, ts, mode, master, verdict, diffs, transient, "
            "checked, tables_total, skipped, detail_json) "
            "VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % RESULT_TABLE,
            (cid, res.get("ts") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             res.get("mode"), res.get("master"), res.get("verdict"),
             len(res.get("diffs") or []), len(res.get("transient") or []),
             res.get("checked") or 0, res.get("tables_total") or 0,
             len(res.get("skipped") or []),
             json.dumps(detail, ensure_ascii=False, default=str)))
    except Exception as e:
        print("[consistency] 保存校验结论失败(可忽略，不影响校验): %s" % e)
        return None


def last_result(cid):
    """取该集群最近一次校验结论（页面徽章/横幅用）。无记录返回 {ok:True, has:False}。"""
    try:
        _ensure_result_table()
        rows = query("SELECT * FROM `%s` WHERE cluster_id=%%s ORDER BY id DESC LIMIT 1"
                     % RESULT_TABLE, (cid,))
    except Exception as e:
        return {"ok": False, "error": "%s: %s" % (type(e).__name__, e)}
    if not rows:
        return {"ok": True, "has": False}
    r = rows[0]
    detail = {}
    try:
        import json
        detail = json.loads(r.get("detail_json") or "{}")
    except Exception:
        detail = {}
    return {"ok": True, "has": True,
            "id": r.get("id"), "ts": r.get("ts"), "mode": r.get("mode"),
            "master": r.get("master"), "verdict": r.get("verdict"),
            "diffs": r.get("diffs"), "transient": r.get("transient"),
            "checked": r.get("checked"), "tables_total": r.get("tables_total"),
            "skipped": r.get("skipped"),
            "verdict_text": detail.get("verdict_text"),
            "coverage_text": detail.get("coverage_text"),
            "repl_broken": detail.get("repl_broken"),
            "flag_mismatch": detail.get("flag_mismatch"),
            "diff_items": (detail.get("diffs") or [])[:50],
            "transient_items": (detail.get("transient") or [])[:50]}


# ===================== abc / d 新增（行级差异、不一致时段、导出回灌）=====================
# 仅追加，不改动上方既有只读比对逻辑。

MAX_ROWDIFF_KEYS = 2000   # 行级差异最多比对的主键数（防止超大表拖垮管理器/节点）
MAX_ROWDIFF_ROWS = 500    # 实际展开返回的行数上限


def _pk_columns(conn, schema, table):
    # 显式 DictCursor：下面按列名取值，元组游标下会抛 TypeError（此前被 except 吞掉，
    # 表现为「展开」按钮一直失败）。
    with conn.cursor(DictCursor) as cur:
        cur.execute(
            "SELECT k.COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE k "
            "JOIN information_schema.TABLE_CONSTRAINTS t "
            "ON k.CONSTRAINT_NAME=t.CONSTRAINT_NAME AND k.TABLE_SCHEMA=t.TABLE_SCHEMA "
            "AND k.TABLE_NAME=t.TABLE_NAME "
            "WHERE t.CONSTRAINT_TYPE='PRIMARY KEY' AND k.TABLE_SCHEMA=%s AND k.TABLE_NAME=%s "
            "ORDER BY k.ORDINAL_POSITION", (schema, table))
        return [r["COLUMN_NAME"] for r in cur.fetchall()]


def _table_columns(conn, schema, table):
    # 同 _pk_columns：按列名取值，必须显式 DictCursor。
    with conn.cursor(DictCursor) as cur:
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
                # 显式 DictCursor：下面 dict(row) 需要「列名→值」，元组游标会直接报错
                # （此前正是这里 + _pk_columns 报 TypeError，被 except 吞掉 ⇒「展开」一直失败）。
                with conn.cursor(DictCursor) as cur:
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
    - orphan_rows: 从库独有行（主库缺失，可能是切换窗口内的真实写入），完整列值。
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
                "recycle_id": None, "note": "该表没有主库缺失、仅从库存在的行，无需导出。"}
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
