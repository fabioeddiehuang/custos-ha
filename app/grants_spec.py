# -*- coding: utf-8 -*-
"""grants_spec.py — 账号与权限「单一真相源」（v2.5 改造，Phase 3，治 MR3 双路径漂移）。

历史踩坑：init_mariadb.sql / fix-node-creds.sql / ensure_ha_creds 的授权集各写一份，
内容不一致（如 node_gen 有 REPLICATION MASTER ADMIN、ha_client 没有），导致
「init 建号有、自愈补号无」的漂移。v2.3 已把运行期授权收敛到 ``mariadb_matrix``
（按 SHOW PRIVILEGES 能力过滤，杜绝按版本号硬编码）。

**MariaDB 与 MySQL 必须分清**（对应 MR1）：两者特权集不同，决不可共用一套。
- MariaDB 的语义集直接复用 ``mariadb_matrix``（HA_PRIVS/REPL_PRIVS/APP_PRIVS），
  运行期按服务器真实支持的特权过滤；
- MySQL 8 的语义集单独维护，与 ``ensure_ha_creds`` 的 MySQL 分支一字不差
  （REPLICATION SLAVE ADMIN / SYSTEM_VARIABLES_ADMIN 等是 MySQL 8 专有，
  MariaDB 上并不存在；反之 REPLICATION SLAVE ADMIN/SLAVE MONITOR 等在 MySQL 上也不存在）。
  若共用 MariaDB 集去核验 MySQL 集群，会**误报缺特权**且**漏检 MySQL 专有特权**。

本模块是**语义层**单一真相源：任何新增账号/特权，只改对应风味的分支，
selfcheck 自动跟随，不会再次分叉。
"""
from app.mysql_compat import F_MARIADB, F_MYSQL8
from app.mariadb_matrix import HA_PRIVS, REPL_PRIVS, APP_PRIVS

# ---- MariaDB 语义特权集（唯一来源 = mariadb_matrix，运行时按能力过滤）----
MARIADB_ACCOUNT_GRANTS = {
    "ha_monitor": list(HA_PRIVS),
    "replication": list(REPL_PRIVS),
    "app": list(APP_PRIVS),
    "root": ["ALL PRIVILEGES"],
}

# ---- MySQL 8 语义特权集（与 ensure_ha_creds 的 MySQL 分支一致，单独维护）----
# 注意：不可含 MariaDB 专有特权（REPLICATION SLAVE ADMIN 在 MySQL 上不存在；
#       反之上列表中的 SYSTEM_VARIABLES_ADMIN 为 MySQL 8 专有）。
MYSQL_ACCOUNT_GRANTS = {
    "ha_monitor": ["REPLICATION SLAVE", "REPLICATION CLIENT", "REPLICATION_SLAVE_ADMIN",
                   "SYSTEM_VARIABLES_ADMIN", "PROCESS", "SELECT", "RELOAD", "SUPER", "DELETE"],
    "replication": ["REPLICATION SLAVE"],
    "app": list(APP_PRIVS),   # 业务账号授权 MariaDB 与 MySQL 一致
    "root": ["ALL PRIVILEGES"],
}

# 节点 init 脚本 / ensure_ha_creds 必须生成的账号
REQUIRED_ACCOUNTS = ("ha_monitor", "replication", "app")


def _grants_for(flavour):
    """按风味返回账号→特权集映射；未知风味按 MariaDB 处理（默认主线）。"""
    return MYSQL_ACCOUNT_GRANTS if flavour == F_MYSQL8 else MARIADB_ACCOUNT_GRANTS


def required_privileges(account, flavour=F_MARIADB):
    """返回账号在指定风味下的语义特权集（单一真相源）。

    flavour 必须是 F_MARIADB / F_MYSQL8（来自 mysql_compat），确保 MariaDB 与
    MySQL 用各自正确的集核验，不交叉误报。
    """
    return list(_grants_for(flavour).get(account, ["SELECT"]))


# 供 selfcheck 解析器派生「多词特权表」：MariaDB + MySQL 并集，避免解析规则与
# 任一风味的授权定义分叉（MR3）。
ALL_PRIVS_UNION = sorted(
    {p for d in (MARIADB_ACCOUNT_GRANTS, MYSQL_ACCOUNT_GRANTS) for ps in d.values()
     for p in ps}
)


def filter_by_capability(cur, privs):
    """按服务器真实支持的特权过滤（db_ops.has_privilege），返回实际可授予的特权列表。

    风味无关：SHOW PRIVILEGES 在 MariaDB / MySQL 8 上均可用。
    """
    from app.ha_client.db_ops import has_privilege
    out = []
    for p in privs:
        if p == "ALL PRIVILEGES":
            out.append(p)
            continue
        if has_privilege(cur, p):
            out.append(p)
    return out


def grant_diff(account, actual_privs, flavour=F_MARIADB):
    """比对实际授予与单一真相源，返回缺失的特权列表（用于启动期一致性自检）。"""
    expected = set(required_privileges(account, flavour))
    actual = set(actual_privs or [])
    return sorted(expected - actual)
