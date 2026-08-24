"""precheck.py — v2.1 节点预检（仅告警，绝不重启/修改）。

用于新增节点或排障前快速核对：
- 连通性：管理器能否 TCP 连入节点 MySQL（manager->node 段）
- 复制关键参数：gtid_mode / log_bin / binlog_format
- server_id 唯一性（同集群）
- read_only 与角色匹配（master 应为 0，slave 应为 1）
- 主版本一致性

返回结构化 findings 列表，前端展示为 OK / WARN / ERR。
"""
from .db import query
from .ha_client import connect_robust


def _var(conn, name):
    try:
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE %s", (name,))
        row = cur.fetchone()
        return row[1] if row else None
    except Exception:
        return None


def check_node(ip, port, user, password, role=None):
    """单节点预检，返回 findings 列表（每项 {level, var, message}）。"""
    findings = []
    conn = connect_robust(ip, port, user, password)
    if not conn:
        return [{"level": "ERR", "var": "connectivity",
                 "message": f"管理器无法 TCP 连入 {ip}:{port}（manager→node 段不可达或凭据错误）"}]
    try:
        # 判定风味：MariaDB 没有 gtid_mode 变量（用 gtid_domain_id），该项检查须跳过，
        # 否则 MariaDB 节点永远报 gtid_mode=None（ERR），造成误报。
        flavour = "mariadb"
        try:
            cur = conn.cursor()
            cur.execute("SELECT VERSION()")
            r = cur.fetchone()
            v = (r[0] if r else "") or ""
            if "mariadb" not in v.lower():
                flavour = "mysql"
        except Exception:
            flavour = "mysql"
        gtid = _var(conn, "gtid_mode")
        logbin = _var(conn, "log_bin")
        fmt = _var(conn, "binlog_format")
        ro = _var(conn, "read_only")
        sid = _var(conn, "server_id")
        if flavour == "mariadb":
            # MariaDB：检查 gtid_domain_id 已设置（非 0 或已显式配置），log_bin 开启即可
            dom = _var(conn, "gtid_domain_id")
            if str(logbin).upper() != "ON":
                findings.append({"level": "ERR", "var": "log_bin",
                                 "message": f"log_bin={logbin}（应为 ON，否则无法复制）"})
            if dom in (None, "", "0"):
                findings.append({"level": "WARN", "var": "gtid_domain_id",
                                 "message": f"gtid_domain_id={dom}（MariaDB 建议显式指定非 0 域，避免跨节点 GTID 冲突）"})
        else:
            if str(gtid).upper() != "ON":
                findings.append({"level": "ERR", "var": "gtid_mode",
                                 "message": f"gtid_mode={gtid}（应为 ON，否则无法基于 GTID 复制/切换）"})
            if str(logbin).upper() != "ON":
                findings.append({"level": "ERR", "var": "log_bin",
                                 "message": f"log_bin={logbin}（应为 ON，否则无法复制）"})
        if str(fmt).upper() != "ROW":
            findings.append({"level": "WARN", "var": "binlog_format",
                             "message": f"binlog_format={fmt}（建议 ROW，Mixed/Statement 复制易出错）"})
        if role == "master" and str(ro) == "1":
            findings.append({"level": "ERR", "var": "read_only",
                             "message": "当前为主库角色但 read_only=1（VIP 不会上浮，业务写会失败）"})
        if role == "slave" and str(ro) == "0":
            findings.append({"level": "WARN", "var": "read_only",
                             "message": "当前为从库角色但 read_only=0（存在双写/数据分歧风险）"})
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return findings


def check_cluster(cluster_id):
    """集群级预检：逐节点检查 + 跨节点 server_id 唯一性 + 主版本一致性。"""
    rows = query(
        "SELECT n.ip, n.port, n.root_user, n.root_pass, h.role "
        "FROM nodes n LEFT JOIN hosts h ON h.host=n.ip AND h.cluster_id=n.cluster_id "
        "WHERE n.cluster_id=%s", (cluster_id,))
    result = []
    sids = {}
    vers = {}
    for n in rows:
        ip = n["ip"]
        port = int(n["port"])
        findings = check_node(ip, port, n.get("root_user") or "root", n.get("root_pass") or "",
                              n.get("role"))
        for f in findings:
            result.append({"host": ip, "port": port, **f})
        # 汇总 server_id / version 用于跨节点比对
        conn = connect_robust(ip, port, n.get("root_user") or "root", n.get("root_pass") or "")
        if conn:
            try:
                sid = _var(conn, "server_id")
                cur = conn.cursor()
                cur.execute("SELECT VERSION()")
                r = cur.fetchone()
                ver = (r[0].split("-")[0] if r else None)
                sids.setdefault(sid, []).append(ip)
                vers.setdefault(".".join((ver or "").split(".")[:2]), []).append(ip)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
    # server_id 重复
    for sid, hosts in sids.items():
        if len(hosts) > 1:
            result.append({"host": ",".join(hosts), "port": 0, "level": "ERR",
                           "var": "server_id",
                           "message": f"server_id={sid} 在节点 {','.join(hosts)} 重复（复制会出错）"})
    # 版本不一致
    if len([k for k in vers if k]) > 1:
        msg = "；".join(f"{k}: {','.join(v)}" for k, v in vers.items())
        result.append({"host": "*", "port": 0, "level": "WARN", "var": "version",
                       "message": f"集群内 MySQL 主版本不一致：{msg}"})
    if not result:
        result.append({"host": "*", "port": 0, "level": "OK", "var": "all",
                       "message": "预检通过：复制参数正常、server_id 唯一、版本一致"})
    return result
