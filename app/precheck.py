"""precheck.py — v2.1 节点预检（仅告警，绝不重启/修改）。

用于新增节点或排障前快速核对：
- 连通性：管理器能否 TCP 连入节点 MySQL（manager->node 段）
- 复制关键参数：gtid_mode / log_bin / binlog_format
- server_id 唯一性（同集群）
- read_only 与角色匹配（master 应为 0，slave 应为 1）
- 主版本一致性
- 半同步复制：SEMI_SYNC=1 时校验节点真的加载并启用了半同步插件
  （插件库缺失会让半同步静默退化为异步，高危且难察觉）

返回结构化 findings 列表，前端展示为 OK / WARN / ERR。
"""
from .db import get_setting, query
from .ha_client import connect_robust
from .config import SEMI_SYNC


def _var(conn, name):
    try:
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE %s", (name,))
        row = cur.fetchone()
        return row[1] if row else None
    except Exception:
        return None


def _semi_expected():
    """服务端期望的半同步开关：settings 表优先，回退环境变量（默认关闭）。"""
    try:
        raw = get_setting("SEMI_SYNC", None)
        if raw is None or str(raw).strip() == "":
            raw = SEMI_SYNC
        return str(raw).strip() == "1"
    except Exception:
        return str(SEMI_SYNC or "0").strip() == "1"


def _gstatus(conn, name):
    """读取一个 GLOBAL STATUS 值（字符串），读不到返回 None。"""
    try:
        cur = conn.cursor()
        cur.execute("SHOW GLOBAL STATUS LIKE %s", (name,))
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
            # gtid_strict_mode：HA 场景必须 ON。OFF 时各 domain 的 GTID 不保证单调递增，
            # 故障切换后从库可能停在旧位点而漏掉事务（数据静默丢失）。
            strict = _var(conn, "gtid_strict_mode")
            if str(strict).upper() != "ON":
                findings.append({"level": "WARN", "var": "gtid_strict_mode",
                                 "message": (f"gtid_strict_mode={strict}（HA 场景应为 ON：OFF 时故障切换后"
                                             "从库可能因 GTID 乱序停在旧位点而漏事务。"
                                             "在节点的 my.cnf / docker-my.cnf 加 gtid_strict_mode = ON 后重启生效）")})
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
        # ---- 半同步复制预检 ----
        # 背景：服务端 SEMI_SYNC=1 时，若节点 MySQL/MariaDB 构建缺少半同步插件库
        # （如部分 MariaDB 定制构建无 semisync_master.so），my.cnf 注入的
        # rpl_semi_sync_*_enabled 会失效，复制静默退化为异步且无任何报错——
        # 这是「以为开了半同步、实际一直裸奔异步」的高危盲区，必须在预检阶段暴露。
        want_semi = _semi_expected()
        semi_vars = {}
        try:
            cur2 = conn.cursor()
            cur2.execute("SHOW VARIABLES LIKE 'rpl_semi_sync%'")
            for r in cur2.fetchall():
                try:
                    semi_vars[str(r[0]).lower()] = r[1]
                except Exception:
                    pass
        except Exception:
            semi_vars = {}
        enabled_keys = [k for k in semi_vars if k.endswith("_enabled")]
        if want_semi:
            if not enabled_keys:
                findings.append({"level": "ERR", "var": "rpl_semi_sync",
                    "message": ("服务端已开启半同步（SEMI_SYNC=1），但该节点未加载半同步插件"
                                "（SHOW VARIABLES LIKE 'rpl_semi_sync%' 为空）。"
                                "常见原因：该构建缺少 semisync 插件库，使 my.cnf 中 "
                                "rpl_semi_sync_*_enabled 失效、复制静默退化为异步。"
                                "请确认节点 plugin_dir 下存在 semisync_master.so"
                                "（MariaDB / 老 MySQL）或 semisync_source.so（MySQL 8.0.26+），"
                                "必要时更换为含插件的官方构建。")})
            else:
                off = [k for k in enabled_keys
                       if str(semi_vars.get(k)).upper() not in ("ON", "1")]
                if off:
                    findings.append({"level": "ERR", "var": "rpl_semi_sync",
                        "message": (f"半同步插件已加载，但 {', '.join(off)} 未启用"
                                    f"（当前值 {', '.join(str(semi_vars.get(k)) for k in off)}）。"
                                    "请在节点 my.cnf 设置对应 *_enabled=1 并重启实例。")})
                elif str(role or "").lower() == "master":
                    # 参数已启用还不够：主库必须真的有半同步从库连上，否则仍等同异步
                    st = _gstatus(conn, "Rpl_semi_sync_master_status")
                    try:
                        clients = int(_gstatus(conn, "Rpl_semi_sync_master_clients") or 0)
                    except (TypeError, ValueError):
                        clients = 0
                    if str(st).upper() == "OFF" or clients <= 0:
                        findings.append({"level": "WARN", "var": "rpl_semi_sync",
                            "message": (f"主库半同步参数已启用，但运行时 master_status={st}、"
                                        f"master_clients={clients}（无半同步从库连接）。"
                                        "此时提交不等待 ACK，实际等同异步；"
                                        "请确认从库也已启用半同步且复制链路正常。")})
        elif enabled_keys:
            on = [k for k in enabled_keys
                  if str(semi_vars.get(k)).upper() in ("ON", "1")]
            if on:
                findings.append({"level": "WARN", "var": "rpl_semi_sync",
                    "message": (f"服务端 SEMI_SYNC=0（未启用半同步），但该节点仍启用 {', '.join(on)}。"
                                "属配置漂移：若为历史残留，请清理节点 my.cnf 中的半同步参数；"
                                "若确需半同步，请在服务端显式设置 SEMI_SYNC=1 以保持一致。")})
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
