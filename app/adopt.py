"""adopt.py — 存量集群「纳管/接管」（v2.4.1）。

设计立场：**接管 = 纳管，不是重建**
-----------------------------------
老库已经在跑业务，接管的第一原则是「别把它搞坏」。因此本模块严格分三层，
且**探测层（本文件）只做只读操作，一个字节都不写**：

  阶段 0  probe   —— 只读探测，幂等，可反复跑；产出「能力/健康报告 + 结论」
  阶段 1  adopt   —— 不动数据：建账号、登记进 nodes/hosts、装 agent + keepalived + VIP
  阶段 2  extend  —— 仅单机场景：开 binlog（需重启）→ 节点本机导出 → 导入 → 建复制

探测层为什么必须独立且只读：
  - 探测可以（也应该）在用户还没决定是否接管时就跑，跑坏了 = 事故；
  - 探测结论要能驱动 UI 显示「能不能接、差哪一步」，所以必须是结构化数据而非日志；
  - 幂等 + 只读 = 可以大胆重试，不用担心副作用。

场景
----
  场景 1（已有主从）：多节点、彼此之间已建复制 → 只加 VIP + agent 做容灾，**不动复制**
  场景 2（单机）    ：仅 1 台、无复制 → 以它为**主库**，另加一台同版本节点做从库

硬约束（探测阶段必须校验，不满足直接判定为 blocker）
--------------------------------------------------
  - 同集群必须**同版本**（用户明确要求非混布）
  - server_id 必须非 0 且互不相同（撞 id 会直接搞坏复制）
  - 节点必须在**同一二层网络**：VRRP 跨网段不工作，VIP 漂移方案直接不成立
"""
import ipaddress
import logging

from .ha_client import connect
from .mysql_compat import detect_flavour, show_replica_status_sql, show_replicas_sql
from . import version_profile as _vp

logger = logging.getLogger(__name__)

# 场景
SCENARIO_EXISTING_REPL = "existing_repl"  # 场景1：已存在主从复制
SCENARIO_SINGLE = "single"                # 场景2：单机，需扩从库
SCENARIO_UNKNOWN = "unknown"

SCENARIO_LABEL = {
    SCENARIO_EXISTING_REPL: "已有主从复制（加 VIP + agent 做容灾）",
    SCENARIO_SINGLE: "单机（以其为主库，另加一台同版本从库）",
    SCENARIO_UNKNOWN: "拓扑未识别，需人工确认",
}

# 结论
VERDICT_READY = "ready"              # 可直接纳管
VERDICT_NEEDS_CHANGE = "needs_change"  # 需先在节点上整改（附具体步骤）
VERDICT_UNSUPPORTED = "unsupported"  # 不支持接管

# 问题级别
LV_BLOCKER = "blocker"
LV_WARN = "warn"

# binlog 非 ROW 时的风险提示（非阻断：老库大量存在 STATEMENT，纳管本身可行）
_ROW_FORMAT = "ROW"


def _issue(level, code, msg, fix=""):
    return {"level": level, "code": code, "msg": msg, "fix": fix}


def _scalar(cur, sql):
    """执行单值查询，失败返回 None（不同版本变量名不同，缺失属正常）。"""
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _dict_cursor(conn):
    """SHOW SLAVE/REPLICA STATUS 字段多且各版本命名不一，必须按列名取值。

    ha_client.connect 默认是元组游标（见文档：游标铁律），按列名取值必须显式
    换成 DictCursor，否则 show_replica_status_sql 返回的行无法可靠解析。
    """
    try:
        from pymysql.cursors import DictCursor
        return conn.cursor(DictCursor)
    except Exception:
        return conn.cursor()


def _pick(row, *names):
    """在行里按多个候选列名取值（MySQL 8.0.22+ 与旧版字段名不同）。"""
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return None


def probe_node(host, port, user, password, timeout=5):
    """只读探测单个节点，返回结构化报告（**绝不写入任何数据**）。

    返回 dict（永远不抛异常，失败时 ok=False 且带 error）：
      ok / error / host / port
      version / family / series / level / supported / profile_note
      server_id / read_only / super_read_only / log_bin / binlog_format
      gtid_enabled / repl_mode
      role                : master / slave / standalone
      replica             : 复制状态（本节点作为从库时）/ None
      replicas            : 有多少从库连着本节点（判断它是不是主库）
      gtid_executed
      data_size_bytes
      grants              : 当前连接用户的权限（字符串列表）
      issues              : [{level, code, msg, fix}]
    """
    out = {
        "ok": False, "error": "", "host": host, "port": int(port),
        "version": "", "family": "", "series": "", "level": _vp.LEVEL_UNKNOWN,
        "supported": False, "profile_note": "",
        "server_id": None, "read_only": None, "super_read_only": None,
        "log_bin": None, "binlog_format": "",
        "gtid_enabled": False, "repl_mode": _vp.REPL_NONE, "gtid_domain_id": None,
        "role": "standalone", "replica": None, "replicas": 0,
        "gtid_executed": "", "data_size_bytes": 0,
        "grants": [], "issues": [],
    }
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
    except Exception as e:
        out["error"] = "连接失败：%s" % e
        out["issues"].append(_issue(
            LV_BLOCKER, "connect_failed", out["error"],
            "检查地址/端口/账号密码，以及节点是否放行管理器来源 IP"))
        return out
    if conn is None:
        out["error"] = "连接失败（connect 返回 None）"
        out["issues"].append(_issue(LV_BLOCKER, "connect_failed", out["error"], ""))
        return out

    try:
        cur = conn.cursor()
        out["ok"] = True

        # ---- 版本与能力档案 ----
        ver = _scalar(cur, "SELECT VERSION()") or ""
        out["version"] = str(ver)
        p = _vp.profile(ver)
        out["family"] = p["family"]
        out["series"] = p["series"]
        out["level"] = p["level"]
        out["supported"] = _vp.is_supported(ver)
        out["profile_note"] = p.get("notes", "")
        # 接管可能需要显式开启的开关（log_bin 默认 OFF 等），向导作为 opt-in 选项提供
        out["switches"] = _vp.adopt_switch_requirements(ver)

        flavour = detect_flavour(cur)

        # ---- 关键变量（逐项取，缺失属正常）----
        out["server_id"] = _scalar(cur, "SELECT @@server_id")
        out["read_only"] = _scalar(cur, "SELECT @@read_only")
        out["super_read_only"] = _scalar(cur, "SELECT @@super_read_only")
        out["log_bin"] = _scalar(cur, "SELECT @@log_bin")
        out["binlog_format"] = str(_scalar(cur, "SELECT @@binlog_format") or "")

        # ---- GTID 与复制模式 ----
        if p["family"] == _vp.FAMILY_MARIADB:
            out["gtid_executed"] = str(_scalar(cur, "SELECT @@gtid_slave_pos") or "")
            out["gtid_domain_id"] = _scalar(cur, "SELECT @@gtid_domain_id")
            # MariaDB 用 SHOW SLAVE STATUS 的 Using_Gtid 判定（No / Slave_pos / Current_pos）
        else:
            out["gtid_executed"] = str(_scalar(cur, "SELECT @@gtid_executed") or "")
            gm = str(_scalar(cur, "SELECT @@gtid_mode") or "")
            out["gtid_enabled"] = (gm.upper() == "ON")

        # ---- 复制状态（本节点作为从库）----
        try:
            dcur = _dict_cursor(conn)
            dcur.execute(show_replica_status_sql(flavour))
            row = dcur.fetchone()
            if row:
                row = dict(row) if not isinstance(row, dict) else row
                io = str(_pick(row, "Replica_IO_Running", "Slave_IO_Running") or "")
                sqlthr = str(_pick(row, "Replica_SQL_Running", "Slave_SQL_Running") or "")
                using_gtid = str(_pick(row, "Using_Gtid") or "")
                auto_pos = _pick(row, "Auto_Position")
                out["replica"] = {
                    "source_host": _pick(row, "Source_Host", "Master_Host"),
                    "source_port": _pick(row, "Source_Port", "Master_Port"),
                    "io_running": io,
                    "sql_running": sqlthr,
                    "seconds_behind": _pick(row, "Seconds_Behind_Source", "Seconds_Behind_Master"),
                    "last_io_error": str(_pick(row, "Last_IO_Error") or ""),
                    "last_sql_error": str(_pick(row, "Last_SQL_Error") or ""),
                    "using_gtid": using_gtid,
                    "auto_position": auto_pos,
                }

                if p["family"] == _vp.FAMILY_MARIADB:
                    out["repl_mode"] = (_vp.REPL_GTID if using_gtid.upper() in ("SLAVE_POS", "CURRENT_POS")
                                        else _vp.REPL_FILE_POS)
                else:
                    out["repl_mode"] = (_vp.REPL_GTID if str(auto_pos) == "1" else _vp.REPL_FILE_POS)
                out["role"] = "slave"
        except Exception:
            out["replica"] = None

        # ---- 有多少从库连着本节点（判断它是不是主库）----
        try:
            dcur = _dict_cursor(conn)
            dcur.execute(show_replicas_sql(flavour))
            rows = dcur.fetchall() or []
            out["replicas"] = len(rows)
            if out["replicas"] > 0:
                out["role"] = "master"
        except Exception:
            out["replicas"] = 0

        # ---- 数据量（评估导出耗时）----
        try:
            cur.execute(
                "SELECT IFNULL(SUM(data_length + index_length), 0) FROM information_schema.tables "
                "WHERE table_schema NOT IN "
                "('information_schema','mysql','performance_schema','sys','test')")
            r = cur.fetchone()
            out["data_size_bytes"] = int(r[0]) if r and r[0] is not None else 0
        except Exception:
            out["data_size_bytes"] = 0

        # ---- 当前连接用户权限 ----
        try:
            cur.execute("SHOW GRANTS FOR CURRENT_USER()")
            out["grants"] = [str((g[0] if isinstance(g, (tuple, list)) else g))
                             for g in (cur.fetchall() or [])]
        except Exception:
            out["grants"] = []

        # ---- 阻断项 / 提示项 ----
        if not out["supported"]:
            out["issues"].append(_issue(
                LV_WARN, "version_unknown",
                "版本 %s 未登记在支持矩阵：可纳管（装 agent + VIP），但「建复制/切主」未经验证"
                % (out["version"] or "?")[:40],
                "确认该版本的复制/权限行为后再启用切主；或先升级到已登记版本"))

        sid = out["server_id"]
        if sid in (None, 0):
            out["issues"].append(_issue(
                LV_BLOCKER, "server_id_missing",
                "server_id 为 0 或未设置，无法建立复制",
                "在 my.cnf 设置 server_id（非 0 且集群内唯一）后重启数据库"))

        if str(out["log_bin"]).upper() in ("OFF", "0", "NONE"):
            out["issues"].append(_issue(
                LV_BLOCKER, "binlog_off",
                "未开启 binlog，无法做复制（单机扩从库场景必须先开）",
                "在 my.cnf 开启 log_bin 与 server_id 后**重启数据库**（有停机窗口）"))

        if out["binlog_format"] and out["binlog_format"].upper() != _ROW_FORMAT:
            out["issues"].append(_issue(
                LV_WARN, "binlog_format_not_row",
                "binlog_format=%s，非 ROW 格式在部分语句下复制不安全" % out["binlog_format"],
                "建议改为 ROW（需重启；STATEMENT 老库需先评估）"))

        r = out["replica"]
        if r and (str(r.get("io_running")).upper() != "YES" or str(r.get("sql_running")).upper() != "YES"):
            out["issues"].append(_issue(
                LV_BLOCKER, "repl_broken",
                "现有复制未正常运行（IO=%s SQL=%s）" % (r.get("io_running"), r.get("sql_running")),
                "先修复现有复制再接管；错误：%s"
                % (r.get("last_io_error") or r.get("last_sql_error") or "无")[:200]))

        return out
    except Exception as e:
        out["ok"] = False
        out["error"] = "探测异常：%s" % e
        out["issues"].append(_issue(LV_BLOCKER, "probe_error", out["error"], ""))
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def same_l2(ips, prefix=24):
    """判断一组 IP 是否落在同一网段（VRRP/VIP 漂移要求同二层）。

    为什么只比网段：老库接管时节点上还没有 custos-node-agent，无法做真正的
    二层探测（ARP/VRRP 报文）。网段一致是必要条件，能拦掉绝大多数「跨机房/跨 VLAN」
    这类 VIP 根本漂不过去的情况；真正的 L2 连通性在 agent 装好后由 keepalived 实测。

    返回 (bool, 说明)
    """
    nets = []
    bad = []
    for s in (ips or []):
        try:
            nets.append(ipaddress.ip_network("%s/%d" % (s.strip(), int(prefix)), strict=False))
        except Exception:
            bad.append(s)
    if bad:
        return False, "IP 无法解析：%s" % "、".join(bad)
    if not nets:
        return False, "未给出任何节点 IP"
    first = nets[0]
    for n in nets[1:]:
        if n != first:
            return False, "节点不在同一网段：%s 与 %s（VIP 依赖 VRRP，跨网段不可用）" % (first, n)
    return True, "同一网段 %s" % first


def probe_cluster(nodes, l2_prefix=24):
    """探测一组节点并给出**集群级**结论（只读，可反复跑）。

    nodes: [{"host":..., "port":..., "user":..., "password":...}, ...]

    返回：
      nodes    : 各节点探测结果
      scenario : existing_repl / single / unknown
      verdict  : ready / needs_change / unsupported
      issues   : 集群级问题（版本一致性、server_id 唯一、同网段、复制健康）
      l2_ok / l2_note
      master   : 识别出的主库 host（可能为空）
    """
    res = {
        "nodes": [], "scenario": SCENARIO_UNKNOWN, "verdict": VERDICT_UNSUPPORTED,
        "issues": [], "l2_ok": False, "l2_note": "", "master": "",
        "repl_mode": _vp.REPL_NONE,
    }
    nodes = list(nodes or [])
    if not nodes:
        res["issues"].append(_issue(LV_BLOCKER, "no_nodes", "未给出任何节点", ""))
        return res

    probed = [probe_node(n.get("host"), n.get("port"), n.get("user"), n.get("password"))
              for n in nodes]
    res["nodes"] = probed

    # ---- 同网段（VIP 前提）----
    ok, note = same_l2([p.get("host") for p in probed if p.get("host")], l2_prefix)
    res["l2_ok"], res["l2_note"] = ok, note
    if not ok:
        res["issues"].append(_issue(LV_BLOCKER, "not_same_l2", note,
                                    "VIP 漂移依赖 VRRP，必须同二层网络；"
                                    "把节点迁到同一 VLAN 或改用其他容灾方案"))

    alive = [p for p in probed if p.get("ok")]
    if not alive:
        res["issues"].append(_issue(
            LV_BLOCKER, "all_unreachable", "所有节点都无法连接", "检查地址/端口/账号密码"))
        return res

    # ---- 同版本（用户要求非混布、只做同版本集群）----
    series = {p.get("series") for p in alive if p.get("series")}
    families = {p.get("family") for p in alive if p.get("family")}
    if len(families) > 1:
        res["issues"].append(_issue(
            LV_BLOCKER, "family_mismatch",
            "混布了不同引擎族：%s（MySQL 与 MariaDB 的 GTID 机制根本不兼容）" % "、".join(sorted(families)),
            "一个集群只能是一种引擎族"))
    if len(series) > 1:
        res["issues"].append(_issue(
            LV_BLOCKER, "version_mismatch",
            "集群内版本不一致：%s（要求同版本）" % "、".join(sorted(series)),
            "统一到同一版本后再接管"))

    # ---- server_id 唯一性 ----
    seen = {}
    for p in alive:
        sid = p.get("server_id")
        if sid in (None, 0):
            continue
        if sid in seen:
            res["issues"].append(_issue(
                LV_BLOCKER, "server_id_dup",
                "server_id %s 在 %s 与 %s 上重复（撞 id 会直接搞坏复制）"
                % (sid, seen[sid], p.get("host")),
                "为其中一个节点改 server_id 后重启数据库"))
        else:
            seen[sid] = p.get("host")

    # ---- 场景识别 ----
    slaves = [p for p in alive if p.get("role") == "slave"]
    masters = [p for p in alive if p.get("role") == "master"]
    if slaves:
        res["scenario"] = SCENARIO_EXISTING_REPL
        # 主库 = 被从库指向的那个节点
        target = None
        for p in alive:
            r = p.get("replica") or {}
            target = r.get("source_host") or target
        if not target and masters:
            target = masters[0].get("host")
        res["master"] = target or ""
        res["repl_mode"] = (slaves[0].get("repl_mode") or _vp.REPL_NONE)
    elif len(alive) == 1:
        res["scenario"] = SCENARIO_SINGLE
        res["master"] = alive[0].get("host") or ""
    else:
        res["scenario"] = SCENARIO_UNKNOWN

    # ---- 汇总结论 ----
    blockers = [i for i in res["issues"] if i["level"] == LV_BLOCKER]
    for p in probed:
        blockers.extend([i for i in (p.get("issues") or []) if i["level"] == LV_BLOCKER])

    if not alive:
        res["verdict"] = VERDICT_UNSUPPORTED
    elif blockers:
        res["verdict"] = VERDICT_NEEDS_CHANGE
    elif not any(p.get("supported") for p in alive):
        res["verdict"] = VERDICT_UNSUPPORTED
    else:
        res["verdict"] = VERDICT_READY

    res["scenario_label"] = SCENARIO_LABEL.get(res["scenario"], res["scenario"])
    return res


# --------------------------------------------------------------------------
# 阶段 1：adopt（建账号 + 登记 + 装 sidecar）
#
# 只读探测见 probe_node / probe_cluster（幂等、可反复跑、绝不改动老库）。
# 以下为「动手」部分——仅建立 Custos 接管账号，不动任何复制拓扑。
# 登记 nodes/hosts 与装 sidecar 由 API 层在账号建立成功后调用（阶段 1 后续）。
# --------------------------------------------------------------------------

def adopt_node(node, root_user, root_pass, creds, manager_ip="", manager_subnet=None):
    """在旧节点上建立 Custos 接管账号（ha_monitor / replication / app / root@'<管理器IP>'）。

    复用 ha_client.credentials.ensure_ha_creds 的完整自愈（账号不存在则 CREATE + GRANT、
    密码漂移则 ALTER、root 回退），MariaDB 与 MySQL 风味自动分派。host 集合由
    config.ACCOUNT_HOST_MODE 决定（segment 模式 = 仅网段%，落实「去 %」策略，最多开放内网段%）。

    :param node: probe_node 结果 dict（需含 host / port / flavour / reachable）
    :param root_user / root_pass: 旧节点 root（compose 固定 MYSQL_ROOT_PASSWORD）
    :param creds: Custos 接管账号凭据
        {"ha_user","ha_pass","repl_user","repl_pass","app_user","app_pass"}
        取自管理器统一凭据（AUTH 设置），全 Custos 集群一致
    :return: (ok: bool, msg: str)
    """
    # ⚠ 修正：probe_node 的输出里是 "ok"，没有 "reachable" 字段。
    # 原写法 `node.get("reachable")` 恒为假，会让每个节点都直接返回失败——
    # 接管流程在第一步就卡死，且错误信息「节点不可达」极具误导性（其实是字段对不上）。
    if not (node.get("ok") or node.get("reachable")):
        return False, "节点探测未通过，无法建立账号（请先确认探测结果）"
    from app.ha_client.credentials import ensure_ha_creds
    ok, msg = ensure_ha_creds(
        host=node["host"], port=node["port"],
        root_user=root_user, root_pass=root_pass,
        ha_user=creds.get("ha_user", ""), ha_pass=creds.get("ha_pass", ""),
        repl_user=creds.get("repl_user", ""), repl_pass=creds.get("repl_pass", ""),
        app_user=creds.get("app_user", ""), app_pass=creds.get("app_pass", ""),
        manager_ip=manager_ip, manager_subnet=manager_subnet,
    )
    return ok, msg


def adopt_cluster(probe, root_user, root_pass, creds, manager_ip="", manager_subnet=None):
    """对 probe_cluster 结果逐个节点建立接管账号。

    :param probe: probe_cluster 返回（含 nodes 列表）
    :return: [{"host","ok","msg"}, ...]
    """
    results = []
    for node in probe.get("nodes", []):
        ok, msg = adopt_node(node, root_user, root_pass, creds, manager_ip, manager_subnet)
        results.append({"host": node.get("host"), "ok": ok, "msg": msg})
    return results


# --------------------------------------------------------------------------
# 阶段 1 后半：登记 nodes/hosts + 生成 sidecar 单元 + 给出引导命令
#
# 接管**不是重建**：老库正在跑业务，所以
#   1. 只登记，不动数据库本体；
#   2. sidecar（keepalived + agent）走 curl 引导命令在节点上装，
#      且**必须 sidecar_only**——不能跑 Install-ha-mariadb-Node.sh，那等于重装数据库。
# --------------------------------------------------------------------------

def _node_type_for(probed):
    """按探测结果决定登记形态。

    接管的库都是**已经在跑**的实例，按裸机 MariaDB 登记（mariadb）：
    它的部署单元只含 keepalived + agent 两个 sidecar，不会碰数据库本体。
    MySQL 族登记为容器形态（docker），同理只装 sidecar。
    """
    family = (probed.get("family") or "").lower()
    return "docker" if family == "mysql" else "mariadb"


def register_and_bootstrap(probe, cluster_id, root_user="root", root_pass="",
                           actor="", ttl_hours=24):
    """登记被接管节点并生成「一行引导命令」。

    :return: [{"ip","ok","command","msg"}, ...]
    """
    from . import node_gen, bootstrap
    results = []
    master = (probe.get("master") or "").strip()
    for p in probe.get("nodes", []):
        ip = (p.get("host") or "").strip()
        if not ip or not p.get("ok"):
            results.append({"ip": ip, "ok": False, "command": "",
                            "msg": "节点探测失败，跳过登记"})
            continue
        role = "master" if (master and ip == master) else "slave"
        ntype = _node_type_for(p)
        try:
            node_gen.generate_node(
                ip, int(p.get("port") or 3306), root_user, root_pass,
                role, int(cluster_id),
                node_type=ntype,
                mysql_version=(p.get("version") or "")[:48] or None,
                server_id=p.get("server_id") or None,
            )
        except Exception as e:
            results.append({"ip": ip, "ok": False, "command": "",
                            "msg": "登记失败：%s" % e})
            continue
        try:
            info = bootstrap.issue_for_node(
                ip, node_type=ntype, cluster_id=int(cluster_id), actor=actor,
                ttl_hours=ttl_hours, sidecar_only=True)
            results.append({"ip": ip, "ok": True, "command": info["command"],
                            "msg": "已登记，到节点执行引导命令装 sidecar（keepalived + agent）"})
        except Exception as e:
            results.append({"ip": ip, "ok": False, "command": "",
                            "msg": "生成引导命令失败：%s" % e})
    return results
