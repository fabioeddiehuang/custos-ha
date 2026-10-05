"""config_drift.py — v2.1 跨节点配置一致性巡检。

对同一集群内各节点做关键复制参数的两两一致性比对，结果写入 config_drift 表：
- server_id：必须唯一（重复则 DRIFT）
- gtid_mode / log_bin / binlog_format：主从必须一致，且期望分别为 ON / ON / ROW
- read_only：须与角色匹配（master=0，slave=1）
- version（主版本）：同集群须一致

巡检只读取、只告警，绝不修改节点配置。
"""
from .db import query, execute
from .ha_client import connect_robust

# 每个集群上一次落库巡检结果的签名，用于变更检测（一致则不 INSERT）
_LAST_SIG = {}


def _var(conn, name):
    try:
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE %s", (name,))
        row = cur.fetchone()
        return row[1] if row else None
    except Exception:
        return None


def _fetch_all(ip, port, user, password, root_user="root", root_pass=""):
    conn = connect_robust(ip, port, user, password, root_user=root_user, root_pass=root_pass)
    if not conn:
        return None
    out = {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT VERSION()")
        r = cur.fetchone()
        out["version"] = r[0] if r else None
        is_maria = "mariadb" in (out["version"] or "").lower()
        out["is_mariadb"] = is_maria
        vars_to_fetch = ["server_id", "log_bin", "binlog_format", "read_only"]
        # MariaDB 无 gtid_mode 变量（用 gtid_domain_id），不取 gtid_mode（避免 None 误报）
        if not is_maria:
            vars_to_fetch.append("gtid_mode")
        else:
            out["gtid_domain_id"] = _var(conn, "gtid_domain_id")
        for v in vars_to_fetch:
            out[v] = _var(conn, v)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return out


def check_cluster(cluster_id):
    """巡检指定集群，写入 config_drift，返回漂移项数。

    【空闲零写入】本次巡检结果与上一次完全一致（含各项 OK）时**一条都不 INSERT**。
    旧实现每 ~60s 对每个节点插入约 6 行（连 status='OK' 的正常项也照插），
    稳态下管理库因此持续产生写入与表膨胀。
    """
    rows = query(
        "SELECT n.ip, n.port, n.root_user, n.root_pass, h.role "
        "FROM nodes n LEFT JOIN hosts h ON h.host=n.ip AND h.cluster_id=n.cluster_id "
        "WHERE n.cluster_id=%s", (cluster_id,))
    # 巡检只读监控，优先用各集群的 ha_monitor 账号（带 @'%'，可从管理器服务端后台 IP 连入），
    # 仅在 ha_monitor 不可用时回退 root。避免依赖 root@'<管理器IP>' 这种易漂移的特定 host 账号
    # （一旦节点上该账号密码与管理器库存不一致，就报 1045 'root'@'<管理器IP>'，误伤预检）。
    from .clusters import get_cluster_creds
    creds = get_cluster_creds(cluster_id) or {}
    ha_user = creds.get("ha_user") or "ha_monitor"
    ha_pass = creds.get("ha_pass") or ""
    snapshots = []
    out = []   # 本次巡检结果行：(host, port, var_name, expected, actual, status)
    for n in rows:
        snap = _fetch_all(n["ip"], int(n["port"]), ha_user, ha_pass,
                          n.get("root_user") or "root", n.get("root_pass") or "")
        if snap is None:
            # 无法连接该节点，记为一条 DRIFT（连通性问题）
            out.append((n["ip"], int(n["port"]), "connectivity",
                        "reachable", "unreachable", "DRIFT"))
            continue
        snap["_ip"] = n["ip"]
        snap["_port"] = int(n["port"])
        snap["_role"] = n.get("role") or "unknown"
        snapshots.append(snap)

    # 期望基准：第一个可达节点
    base = snapshots[0] if snapshots else None

    # 1) server_id 唯一性
    seen = set()
    for snap in snapshots:
        sid = snap.get("server_id")
        status = "DRIFT" if sid in seen else "OK"   # 重复 server_id：必为 DRIFT
        seen.add(sid)
        out.append((snap["_ip"], snap["_port"], "server_id",
                    f"unique(!={sid})", str(sid), status))

    # 2) 一致性参数
    for snap in snapshots:
        # MariaDB 无 gtid_mode（用 gtid_domain_id），跳过该项避免误报 DRIFT
        if snap.get("is_mariadb"):
            var_checks = (("log_bin", "ON"), ("binlog_format", "ROW"),
                          ("gtid_domain_id", "0"))  # 期望非 0（由 my.cnf 指定）
        else:
            var_checks = (("gtid_mode", "ON"), ("log_bin", "ON"), ("binlog_format", "ROW"))
        for var, exp in var_checks:
            actual = snap.get(var)
            if var == "gtid_domain_id":
                # gtid_domain_id 期望非 0（0 为未显式指定，视为漂移）
                status = "OK" if str(actual).upper() not in ("0", "NONE", "") else "DRIFT"
            else:
                status = "OK" if str(actual).upper() == exp else "DRIFT"
            out.append((snap["_ip"], snap["_port"], var, exp, str(actual), status))
        # 3) read_only：正确 HA 语义 = 全集群「恰好一个」可写节点（主），其余只读。
        #    不依赖 hosts.role 表（该表易缺失/滞后，会把真正的主库误判为漂移——这是
        #    之前「没切主从也反复出现配置漂移」的根因）。
        _ro = {}
        for snap in snapshots:
            ro = snap.get("read_only")
            _ro[snap["_ip"]] = "1" if str(ro).strip().upper() in ("ON", "1", "TRUE", "YES") else "0"
        _writable = [ip for ip, v in _ro.items() if v == "0"]
        if len(_writable) == 1:
            # 健康：唯一可写=主，其余只读，read_only 全部 OK
            for snap in snapshots:
                out.append((snap["_ip"], snap["_port"], "read_only",
                            "0" if snap["_ip"] in _writable else "1",
                            str(snap.get("read_only")), "OK"))
        elif len(_writable) == 0:
            for snap in snapshots:
                out.append((snap["_ip"], snap["_port"], "read_only",
                            "1（需恰好 1 个主库）", str(snap.get("read_only")), "DRIFT"))
        else:
            # 多于一个可写：脑裂风险，把所有可写节点标 DRIFT
            for snap in snapshots:
                out.append((snap["_ip"], snap["_port"], "read_only",
                            "恰好 1 个主库", str(snap.get("read_only")),
                            "OK" if _ro.get(snap["_ip"]) != "0" else "DRIFT"))
        # 4) 版本主版本一致
        if base:
            b_mm = (base.get("version") or "").split("-")[0]
            s_mm = (snap.get("version") or "").split("-")[0]
            # 仅比较主版本.次版本（如 8.0 vs 8.4）
            b_mm = ".".join(b_mm.split(".")[:2])
            s_mm = ".".join(s_mm.split(".")[:2])
            out.append((snap["_ip"], snap["_port"], "version", b_mm, s_mm,
                        "OK" if b_mm == s_mm else "DRIFT"))

    # 变更检测：与上一次巡检结果完全一致 → 一条都不写（稳态零写入）
    sig = tuple(out)
    if _LAST_SIG.get(cluster_id) == sig:
        return sum(1 for r in out if r[5] == "DRIFT")
    _LAST_SIG[cluster_id] = sig

    # 整批原子提交：避免 monitor 巡检与 remediate 并发时，get_latest_drift 读到
    # 「只插入了一半变量」的最新批次（逐行 execute 各自提交会暴露半批），从而误判无漂移。
    from .db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for host, port, var_name, expected, actual, status in out:
                cur.execute("INSERT INTO config_drift (cluster_id, host, port, var_name, expected, actual, status) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                            (cluster_id, host, port, var_name, expected, actual, status))
        conn.commit()
    finally:
        conn.close()
    _purge()
    return sum(1 for r in out if r[5] == "DRIFT")


_LAST_PURGE = [0.0]


def _purge():
    """保留期清理。限频 1 小时一次：DELETE ... WHERE ts < ... 若缺 ts 单列索引会全表扫描，
    把整表页读进 buffer pool 产生脏页与后续 checkpoint 写盘（索引已在 db.py 迁移里补建）。"""
    import time
    if time.monotonic() - _LAST_PURGE[0] < 3600:
        return
    _LAST_PURGE[0] = time.monotonic()
    try:
        execute("DELETE FROM config_drift WHERE ts < DATE_SUB(NOW(), INTERVAL 7 DAY)")
    except Exception as e:
        print(f"[config_drift] 清理失败(可忽略): {e}")


def get_latest_drift(cluster_id):
    """返回该集群最近一次巡检的漂移汇总（按 ts 取最新一批）。"""
    try:
        rows = query("SELECT ts FROM config_drift WHERE cluster_id=%s ORDER BY ts DESC LIMIT 1",
                     (cluster_id,))
        if not rows:
            return [], 0
        latest = rows[0]["ts"]
        rows = query("SELECT host, port, var_name, expected, actual, status FROM config_drift "
                     "WHERE cluster_id=%s AND ts=%s ORDER BY host, var_name", (cluster_id, latest))
        drift = sum(1 for r in rows if r["status"] == "DRIFT")
        return rows, drift
    except Exception:
        return [], 0


def remediate_latest(cluster_id):
    """对最近一次巡检的 DRIFT 项尝试在线修复（同时兼容 MySQL 与 MariaDB）。

    持久化差异（必须区分，否则 MariaDB 上会语法报错）：
      - MySQL 8.0+：SET PERSIST 写入 mysqld-auto.cnf，重启仍生效。
      - MariaDB：无 SET PERSIST 语法，改用 SET GLOBAL（仅运行时生效，重启需改 my.cnf 持久化）。
    故按节点 flavour 选择 PERSIST / GLOBAL 关键字。

    可在线修复：
      - read_only：按角色设 0(master)/1(slave)（两款均支持）
      - gtid_mode：仅 MySQL（MariaDB 无该变量）；三阶段 + FLUSH BINARY LOGS
      - gtid_domain_id：仅 MariaDB（MySQL 无该变量）；设回基准节点值

    不能在线修复（标 manual）：server_id 冲突 / log_bin=OFF / binlog_format /
      version / connectivity。

    返回 {"ok":bool,"fixed":[...],"manual":[...],"errors":[...],"msg":str}
    """
    rows, _ = get_latest_drift(cluster_id)
    drifts = [r for r in rows if r["status"] == "DRIFT"]
    if not drifts:
        return {"ok": True, "fixed": [], "manual": [], "errors": [], "msg": "无漂移项，无需修复"}

    from .clusters import get_cluster_creds
    nodes = query(
        "SELECT n.ip, n.port, n.root_user, n.root_pass, h.role "
        "FROM nodes n LEFT JOIN hosts h ON h.host=n.ip AND h.cluster_id=n.cluster_id "
        "WHERE n.cluster_id=%s", (cluster_id,))
    creds = get_cluster_creds(cluster_id) or {}
    ha_user = creds.get("ha_user") or "ha_monitor"
    ha_pass = creds.get("ha_pass") or ""

    # 重新 gather 快照，确定基准节点（base）、各节点角色与 flavour
    snaps = []
    for n in nodes:
        s = _fetch_all(n["ip"], int(n["port"]), ha_user, ha_pass,
                       n.get("root_user") or "root", n.get("root_pass") or "")
        if s:
            s["_ip"] = n["ip"]; s["_port"] = int(n["port"])
            s["_role"] = (n.get("role") or "unknown").lower()
            s["_root_user"] = n.get("root_user") or "root"
            s["_root_pass"] = n.get("root_pass") or ""
            s["_is_maria"] = bool(s.get("is_mariadb"))
            snaps.append(s)
    if not snaps:
        return {"ok": False, "fixed": [], "manual": [], "errors": ["所有节点均不可达，无法修复"],
                "msg": "无法连接任何节点"}
    base = snaps[0]
    base_gtid_domain = base.get("gtid_domain_id")

    fixed, manual, errors = [], [], []
    by_node = {}
    for d in drifts:
        by_node.setdefault((d["host"], int(d["port"])), []).append(d)

    for (ip, port), items in by_node.items():
        snap = next((s for s in snaps if s["_ip"] == ip and s["_port"] == port), None)
        if not snap:
            for d in items:
                manual.append({"host": ip, "port": port, "var": d["var_name"],
                               "reason": "节点不可达，无法在线修复"})
            continue
        role = snap["_role"]
        is_maria = snap["_is_maria"]
        # MySQL 用 SET PERSIST；MariaDB 不支持该语法，改用 SET GLOBAL（重启需 my.cnf 持久化）
        kw = "PERSIST" if not is_maria else "GLOBAL"
        note = "" if not is_maria else "（MariaDB 仅运行时生效，重启需 my.cnf 持久化）"
        # 修复是写操作（SET PERSIST / SET GLOBAL），需要 SYSTEM_VARIABLES_ADMIN /
        # PERSIST_RO_VARIABLES_ADMIN 等权限；ha_monitor 为只读监控账号，通常没有
        # （执行 SET read_only 会报 1227）。故**优先用 root**，ha_monitor 仅作最后回退。
        # 注意：connect_robust 内部也优先 ha_monitor，只在「连不上」时才回退 root，
        # 权限不足不会触发其 root 回退 —— 所以这里必须显式先试 root，否则会拿 ha_monitor
        # 连接去执行写语句导致全部修复失败。
        conn = None
        if snap.get("_root_pass"):
            try:
                from .ha_client import connect
                conn = connect(ip, port, snap["_root_user"], snap["_root_pass"], timeout=5)
            except Exception:
                conn = None
        if conn is None and ha_pass:
            try:
                from .ha_client import connect
                conn = connect(ip, port, ha_user, ha_pass, timeout=5)
            except Exception:
                conn = None
        if conn is None:
            conn = connect_robust(ip, port, ha_user, ha_pass,
                                  snap["_root_user"], snap["_root_pass"])
        if conn is None:
            for d in items:
                errors.append({"host": ip, "port": port, "var": d["var_name"],
                               "reason": "无法连接（ha_monitor 与 root 均失败）"})
            continue
        cur = conn.cursor()
        try:
            for d in items:
                vn = d["var_name"]
                try:
                    if vn == "read_only":
                        target = "0" if role in ("master", "primary") else "1"
                        cur.execute(f"SET {kw} read_only={target}")
                        fixed.append({"host": ip, "port": port, "var": vn,
                                      "action": f"SET {kw} read_only={target}（按角色 {role}）{note}"})
                    elif vn == "gtid_mode":
                        # 仅 MySQL 存在该变量；MariaDB 不产出此项漂移
                        cur.execute(f"SET {kw} enforce_gtid_consistency=ON")
                        ok_g = True
                        for stp in ("OFF_PERMISSIVE", "ON_PERMISSIVE", "ON"):
                            try:
                                cur.execute(f"SET {kw} gtid_mode={stp}")
                            except Exception as e:
                                ok_g = False
                                errors.append({"host": ip, "port": port, "var": vn,
                                               "reason": f"gtid_mode={stp} 失败: {e}"})
                                break
                        if ok_g:
                            try:
                                cur.execute("FLUSH BINARY LOGS")
                            except Exception:
                                pass
                            fixed.append({"host": ip, "port": port, "var": vn,
                                          "action": f"SET {kw} gtid_mode=ON（三阶段+FLUSH BINARY LOGS）{note}"})
                    elif vn == "gtid_domain_id":
                        # 仅 MariaDB 存在；MySQL 不产出此项漂移
                        if base_gtid_domain and str(base_gtid_domain) not in ("0", "NONE", ""):
                            cur.execute(f"SET {kw} gtid_domain_id={int(base_gtid_domain)}")
                            fixed.append({"host": ip, "port": port, "var": vn,
                                          "action": f"SET {kw} gtid_domain_id={int(base_gtid_domain)}（按基准）{note}"})
                        else:
                            manual.append({"host": ip, "port": port, "var": vn,
                                           "reason": "基准节点 gtid_domain_id 为 0，无参考值，需人工"})
                    elif vn == "server_id":
                        manual.append({"host": ip, "port": port, "var": vn,
                                       "reason": "server_id 冲突需在复制拓扑谨慎分配唯一值，未自动修改；请人工重设后重启"})
                    elif vn == "log_bin":
                        manual.append({"host": ip, "port": port, "var": vn,
                                       "reason": "log_bin 不能在线开启，需到节点改 my.cnf 并重启（MySQL/MariaDB）"})
                    elif vn == "binlog_format":
                        manual.append({"host": ip, "port": port, "var": vn,
                                       "reason": "binlog_format 需改 my.cnf 并重启后生效，无法在线持久化（MySQL 为只读变量；MariaDB 亦无 PERSIST）"})
                    elif vn == "version":
                        manual.append({"host": ip, "port": port, "var": vn,
                                       "reason": "版本不一致需升级节点，无法在线修复"})
                    elif vn == "connectivity":
                        manual.append({"host": ip, "port": port, "var": vn,
                                       "reason": "节点不可达，请检查网络/MySQL 状态"})
                    else:
                        manual.append({"host": ip, "port": port, "var": vn,
                                       "reason": "未知漂移项，未自动处理"})
                except Exception as e:
                    errors.append({"host": ip, "port": port, "var": vn,
                                   "reason": f"修复失败: {type(e).__name__}: {e}"})
            conn.commit()
        except Exception as e:
            errors.append({"host": ip, "port": port, "var": "*",
                           "reason": f"节点修复异常: {type(e).__name__}: {e}"})
        finally:
            try:
                conn.close()
            except Exception:
                pass

    return {"ok": True, "fixed": fixed, "manual": manual, "errors": errors,
            "msg": f"修复完成：{len(fixed)} 项已在线修复，{len(manual)} 项需人工，{len(errors)} 项失败"}
