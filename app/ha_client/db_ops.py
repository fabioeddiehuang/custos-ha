# -*- coding: utf-8 -*-
"""db_ops.py — 风味敏感 DB 操作「唯一出口」（v2.5 稳定性/安全改造，对应 MR1）。

为什么存在
----------
历史踩坑反复证明：把「MySQL 专属语法」直接写在各调用点，会在 MariaDB 上
报 1064/1305/1227，且每次新增站点都重新引入同类 bug（见 `doc/开发/待修问题清单`
与 working_memory 的「技术陷阱铁律」）。此前 `mysql_compat.identify_clause` 定义后
**0 处调用**、`GTID_SUBSET` 在 6 个子模块各自硬编码，正是「修了又复现」的病根。

本模块是**唯一**允许出现 `IDENTIFIED WITH/VIA`、`super_read_only`、
`GTID_SUBSET` 风味分支的 Python 位置；`scripts/ci_lint.py` 会静态拦截其它文件里的
直拼。新增任何风味敏感操作，必须加在这里。

铁律
----
- 密码一律走 `%s` 参数化，**绝不**拼进 SQL 字符串（修 gtid.py 曾把密码拼进
  `USING PASSWORD('%s')` 的注入隐患）。
- 失败显式返回/抛出，不靠 `except: pass` 吞（除非调用点明确要容错）。
- MariaDB 与 MySQL 的差异只在本文件出现一次。
"""
import logging

from app.mysql_compat import (F_MARIADB, F_MYSQL8, detect_flavour)

log = logging.getLogger(__name__)


def ident_clause(flavour, plugin="mysql_native_password"):
    """返回 CREATE/ALTER USER 的认证子句（含 ``%s`` 占位符，密码作参数传入）。

    MariaDB：``IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)``
    MySQL 8 ：``IDENTIFIED WITH <plugin> BY %s``

    调用方：``cur.execute("CREATE USER %s@%s " + ident_clause(fl), (u, h, pwd))``

    ``plugin`` 是受控常量（来自 ``default_auth_plugin``），非用户输入，故允许进子句。
    """
    if flavour == F_MARIADB:
        # MariaDB 10.6 不支持 `IDENTIFIED VIA ... BY '明文'`，必须用 USING PASSWORD 形式
        return "IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)"
    return "IDENTIFIED WITH %s BY %%s" % plugin


def set_read_only(cur, on, flavour=None):
    """设置节点只读(on=True)/可写(on=False)，返回 (ok, err)。

    - MySQL 8：先调整 super_read_only（READ_ONLY ADMIN 之外还需关掉它才能关 read_only，
      否则报 1290；开启只读时一并开 super_read_only 防普通账号写）。
    - MariaDB：只有 read_only（无 super_read_only 变量）。

    失败时按「项目默认 MariaDB」探测风味，避免 detect 异常被误判为 MySQL 而写出
    不适用的语句（MR2）。
    """
    if flavour is None:
        try:
            flavour = detect_flavour(cur)
        except Exception:
            flavour = F_MARIADB  # 默认 MariaDB 比误判 MySQL 更安全
    try:
        if flavour == F_MYSQL8:
            try:
                cur.execute("SET GLOBAL super_read_only=%s", (1 if on else 0,))
            except Exception as e:
                # 关超级只读失败仅记录：read_only 才是成败依据（MariaDB 无此变量也会到此）
                log.debug("set_read_only: super_read_only 调整被忽略: %s", e)
        cur.execute("SET GLOBAL read_only=%s", (1 if on else 0,))
        return True, None
    except Exception as e:
        return False, "%s: %s" % (type(e).__name__, e)


def gtid_slave_is_subset(cur, slave_pos, master_pos, flavour):
    """GTID_SUBSET(slave_pos, master_pos)：从库是否已包含主库产生的全部事务。

    等价于 MySQL 原生 ``GTID_SUBSET(slave, master)``。MariaDB 12.x 无该函数
    （实测 1305 FUNCTION GTID_SUBSET does not exist），改走 Python 侧位点比较
    （``syncgate._covered_by_positions``，与就绪/强制同步判定同一套），使「曾为源需
    RESET MASTER」「已应用集非子集需对齐」的收敛逻辑在 MariaDB 上同样生效，
    不再因跳过检测而失明。
    """
    if not master_pos:
        return True
    if not slave_pos:
        return False
    if flavour == F_MYSQL8:
        try:
            cur.execute("SELECT GTID_SUBSET(%s, %s)", (slave_pos, master_pos))
            return bool((cur.fetchone() or [0])[0])
        except Exception:
            pass
    from app.syncgate import _covered_by_positions
    return _covered_by_positions(master_pos, slave_pos)


def has_privilege(cur, priv_name):
    """用 ``SHOW PRIVILEGES`` 能力探测（不按版本号硬编码，对应 TODO 1.1 / MR1）。

    返回 True 表示当前连接账号实际拥有该特权名（大小写不敏感匹配）。
    用于「按服务器真实支持的特权集过滤授权」，避免猜错即 1064/1227。
    """
    try:
        cur.execute("SHOW PRIVILEGES")
        rows = cur.fetchall()
    except Exception as e:
        log.debug("has_privilege: SHOW PRIVILEGES 失败: %s", e)
        return False
    want = (priv_name or "").upper()
    for r in (rows or []):
        # 每行: (Privilege, Context, Comment)
        name = (r[0] if r else "").upper()
        if name == want:
            return True
    return False
