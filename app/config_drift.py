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
        # 3) read_only 与角色匹配（SHOW VARIABLES 返回 ON/OFF，需归一化为 0/1 再比对）
        ro = snap.get("read_only")
        exp_ro = "0" if snap["_role"] == "master" else "1"
        ro_norm = "1" if str(ro).strip().upper() in ("ON", "1", "TRUE", "YES") else "0"
        out.append((snap["_ip"], snap["_port"], "read_only", exp_ro, str(ro),
                    "OK" if ro_norm == exp_ro else "DRIFT"))
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

    for host, port, var_name, expected, actual, status in out:
        execute("INSERT INTO config_drift (cluster_id, host, port, var_name, expected, actual, status) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (cluster_id, host, port, var_name, expected, actual, status))
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
