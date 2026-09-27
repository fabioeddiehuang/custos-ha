# -*- coding: utf-8 -*-
"""selfcheck.py — 启动期自检闸门（v2.5 改造，Phase 3，稳定+安全）。

管理器/节点启动后跑最小自检；不过则明确报错并阻断「就绪/可写」，避免带病运行。
对应 MR3/MR4：初始化与运行时一致、权限齐备、弱口令暴露。
"""
import logging

from app.config import is_weak_password, WEB_PASS
from app.db import get_conn
from app.ha_client import connect
from app.ha_client.db_ops import has_privilege

log = logging.getLogger(__name__)

# 节点接入必须齐备的特权（能力探测，不按版本硬编码）
NODE_REQUIRED_PRIVS = (
    "REPLICATION SLAVE ADMIN", "READ ONLY ADMIN", "BINLOG ADMIN", "REPLICATION SLAVE",
)


def selfcheck_manager():
    """服务端启动自检：返回 [(level, msg)]，level ∈ {error, warn, ok}。"""
    issues = []
    if is_weak_password(WEB_PASS):
        issues.append(("warn", "WEB_PASS 为弱口令/默认值（如 admin），首次登录将被强制改密；"
                               "生产环境建议设为强口令"))
    try:
        conn = get_conn()
        try:
            with conn.cursor() as c:
                c.execute("SELECT 1 FROM users LIMIT 1")
        finally:
            conn.close()
    except Exception as e:
        issues.append(("error", "管理库不可连或缺失 users 表：%s" % e))
    if not issues:
        issues.append(("ok", "服务端启动自检通过"))
    return issues


def selfcheck_node(host, port, user, password):
    """节点接入自检：关键特权齐备。返回 (ok, [(level, msg)])。"""
    issues = []
    try:
        conn = connect(host, port, user, password)
    except Exception as e:
        return False, [("error", "节点不可连：%s" % e)]
    try:
        cur = conn.cursor()
        for priv in NODE_REQUIRED_PRIVS:
            if not has_privilege(cur, priv):
                issues.append(("warn", "节点缺少特权 %s（部分功能可能受限）" % priv))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    ok = not any(l == "error" for l, _ in issues)
    if ok and not issues:
        issues.append(("ok", "节点自检通过"))
    return ok, issues


# 授权行里可能出现的特权 token（用于解析 SHOW GRANTS 输出）
_GRANT_TOKEN_RE = __import__("re").compile(r"GRANT\s+(.+?)\s+ON\s")
# 多词语义特权：从 grants_spec 单一真相源（MariaDB+MySQL 并集）派生，
# 避免解析规则与任一风味的授权定义分叉（MR3）。
from app.grants_spec import ALL_PRIVS_UNION
_MULTI_WORD_PRIVS = tuple(sorted({p.upper() for p in ALL_PRIVS_UNION if " " in p})) or (
    "REPLICATION SLAVE ADMIN", "REPLICATION CLIENT", "REPLICATION SLAVE",
    "REPLICATION MASTER ADMIN", "READ ONLY ADMIN", "BINLOG ADMIN", "SLAVE MONITOR",
    "CREATE TEMPORARY TABLES", "CREATE USER", "SHOW VIEW", "SYSTEM_VARIABLES_ADMIN",
    "ALL PRIVILEGES",
)


def _parse_grant_privs(grant_row):
    """从一行 SHOW GRANTS 解析出特权集合（大写），best-effort。"""
    m = _GRANT_TOKEN_RE.search(grant_row or "")
    if not m:
        return set()
    body = m.group(1).upper()
    privs = set()
    for multi in _MULTI_WORD_PRIVS:
        if multi in body:
            privs.add(multi)
    # 单 token（逗号分隔）
    for tok in body.replace("ALL PRIVILEGES", "").split(","):
        tok = tok.strip()
        if tok and " " not in tok and tok not in ("GRANT", "OPTION", "WITH", "ALL"):
            privs.add(tok)
    return privs


def selfcheck_cluster_grants(cluster_id, master_host, master_port, creds):
    """按 grants_spec 单一真相源核验集群主库账号授权，**MariaDB/MySQL 各自用正确集**。

    与 ensure_ha_creds 形成「边自愈边观测」双保险：自愈负责补，本函数负责看得见漂移。
    **风味感知**：先 detect_flavour 再选对应特权集，MySQL 集群不会误报 MariaDB 专有特权、
    也不会漏检 MySQL 专有特权（如 SYSTEM_VARIABLES_ADMIN）。
    **能力感知**：期望集再按服务器真实支持的特权（SHOW PRIVILEGES）过滤，老版本缺特权
    不误报。best-effort：无 SHOW GRANTS FOR 权限时降级为提示，绝不阻断监控。
    返回 (ok, [(level, msg)])。
    """
    issues = []
    ha_user = creds.get("ha_user") or "ha_monitor"
    ha_pass = creds.get("ha_pass") or ""
    try:
        conn = connect(str(master_host), int(master_port), ha_user, ha_pass)
    except Exception as e:
        # 拿不到主库连接不阻塞监控，仅记录（自愈/监控本身也连不上时会各自降级）
        return True, [("warn", "授权自检跳过（主库不可连：%s）" % e)]
    try:
        from app.grants_spec import REQUIRED_ACCOUNTS, required_privileges
        from app.mariadb_matrix import server_privileges
        from app.mysql_compat import detect_flavour, F_MARIADB
        cur = conn.cursor()
        try:
            flavour = detect_flavour(cur) or F_MARIADB
        except Exception:
            flavour = F_MARIADB  # 探测失败按 MariaDB（项目主线）处理
        available = server_privileges(cur)  # None=探测失败，按全集比对
        for acct in REQUIRED_ACCOUNTS:
            actual = set()
            for host_spec in ("%", "localhost", "127.0.0.1"):
                try:
                    cur.execute("SHOW GRANTS FOR %s@%s", (acct, host_spec))
                    for row in cur.fetchall() or []:
                        actual |= _parse_grant_privs((row[0] if row else "") or "")
                    if actual:
                        break
                except Exception:
                    continue
            if not actual:
                issues.append(("warn", "账号 %s 未找到授权记录（ensure_ha_creds 会自愈补建）" % acct))
                continue
            # 期望集：先按风味选对（MariaDB/MySQL 不同），再按服务器能力过滤，
            # 避免老版本缺特权被误报、也避免跨风味误报。
            expected = required_privileges(acct, flavour)
            if available:
                expected = [p for p in expected if p.upper() in available]
            missing = sorted(set(expected) - set(p.upper() for p in actual))
            if missing:
                issues.append(("warn", "账号 %s 缺少特权 %s（授权漂移，下一轮 ensure_ha_creds 将补齐）"
                               % (acct, ",".join(missing))))
    except Exception as e:
        issues.append(("warn", "授权自检异常(可忽略): %s" % e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if not issues:
        issues.append(("ok", "集群 %s 授权与单一真相源一致" % cluster_id))
    return True, issues
