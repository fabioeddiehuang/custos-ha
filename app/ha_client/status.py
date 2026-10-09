"""
与 MySQL 节点交互的客户端工具。

重要设计：所有操作都支持「root 回退」。
- 业务/监控使用 ha_monitor 账号，但其密码可能因改 HA_PASS 而漂移。
- 节点 MySQL 的 root 密码在容器创建时固定（compose 的 MYSQL_ROOT_PASSWORD），
  不随 HA_PASS 漂移。因此当 ha_monitor 连不上时，回退用 root 执行，
  保证只读探活/切换/复制搭建/账号自愈都能成功，VIP 不再因 ha_monitor
  密码漂移而卡在某一个节点。
"""
import re
import time
import hashlib
import pymysql
from pymysql import OperationalError, ProgrammingError, DatabaseError

from app.config import ACCOUNT_HOST_MODE, segment_of, get_manager_public_ip, is_semi_sync_enabled
from app import mariadb_matrix
from .db_ops import ident_clause
from . import db_ops  # db_ops.set_read_only 避免与本模块 set_read_only 重名冲突


import os as _os
import time as _time
_ALTER_MEMO_TTL = 3600


def _ensure_account(cur, user, host, passwd, idn_expr, grant_sql=None,
                    conn_host=None, conn_port=None, plugin=None):
    """幂等确保账号存在、密码正确、权限正确；无变化时完全不执行写操作（不产生 binlog）。

    这是根治「主库每周期被 ALTER/GRANT 刷写、副本经复制持续写盘」的关键：
    稳定态下本函数只做 SELECT 比对，0 条写语句 -> 主库 binlog 静默 -> 副本空闲即静默。

    plugin 为目标认证插件；当它非 mysql_native_password（如 caching_sha2_password）时，
    本地无法复算哈希，改为「插件一致 + 实连校验通过」即判定密码正确、跳过 ALTER。
    """
    try:
        cur.execute("SELECT plugin, authentication_string FROM mysql.user "
                    "WHERE user=%s AND host=%s", (user, host))
        row = cur.fetchone()
    except Exception:
        # 极旧版本 mysql.user 无 plugin 列：退回只查 authentication_string
        cur.execute("SELECT NULL, authentication_string FROM mysql.user "
                    "WHERE user=%s AND host=%s", (user, host))
        row = cur.fetchone()
    want = _native_password_hash(passwd)

    need_alter = False
    if row is None:
        # 账号不存在：创建（CREATE 同时设密码），随后跳过 ALTER
        cur.execute("CREATE USER IF NOT EXISTS %s@%s " + idn_expr, (user, host, passwd))
    else:
        cur_plugin, cur_hash = row[0], row[1]
        if plugin and cur_plugin and cur_plugin != plugin:
            need_alter = True                      # 插件迁移（native -> caching_sha2）
        elif plugin and plugin != "mysql_native_password":
            need_alter = not _can_login(conn_host, conn_port, user, passwd)
        else:
            need_alter = (cur_hash != want)

    if need_alter:
        memo_key = (conn_host, conn_port, user, host, passwd)
        now = time.time()
        last = _alter_memo.get(memo_key, 0)
        if plugin and plugin != "mysql_native_password" and now - last < _ALTER_MEMO_TTL:
            need_alter = False                     # 刚改过仍连不上，本周期不再重复写 binlog
        else:
            _alter_memo[memo_key] = now
    if need_alter:
        cur.execute("ALTER USER %s@%s " + idn_expr, (user, host, passwd))
    if grant_sql:
        want_privs = _parse_grant_privs(grant_sql)
        have = _granted_privs(cur, user, host)
        if want_privs and not want_privs.issubset(have):
            # ALL PRIVILEGES 需单独判定：MySQL 8 的 SHOW GRANTS 会把它**展开**成一长串
            # 具体权限（不含 "ALL PRIVILEGES" 字面量），直接做子集判断会误判为「权限缺失」，
            # 导致每周期重复 GRANT -> 重新引入「主库空闲也持续写 binlog、副本跟着写盘」。
            # MariaDB 则直接给出 "ALL PRIVILEGES" 字面量，两种都必须覆盖。
            if "ALL PRIVILEGES" in want_privs and _has_all_privileges(cur, user, host):
                return
            cur.execute(grant_sql)


_BINLOG_RETENTION_WARNED = set()

def ensure_ha_creds(host, port, root_user, root_pass,
                    ha_user, ha_pass, repl_user, repl_pass,
                    app_user="", app_pass="", manager_ip="", manager_subnet=None,
                    old_ha_user="", old_repl_user="", old_app_user=""):
    """
    将节点上的 ha_monitor / replication / app / root@'<管理器IP>' 账号**完整自愈**为当前设定值。

    与旧版（仅 ALTER USER、且必须用 root 连）不同：
    - 优先用 **ha_monitor**（init 创建、密码已知正确、且已授予 CREATE/ALTER/DROP USER）
      连接并修正账号；root 仅作为最后回退。
    - 这会打破「root 密码漂移 -> 用错误 root 连不上 -> 自愈不跑 -> 永远连不上 root」
      的死循环：只要 ha_monitor 仍能连上（节点在线），就能把 root@'<管理器IP>' 密码
      校正回管理器库存值，后续 root 连接自动恢复、复制随后建立。
    - 若账号不存在（裸机未跑 init.sql / 被面板清空 / 密码漂移）也能自动 CREATE + GRANT，
      避免从库连主库报 1130「Host ... not allowed」。

    返回 (ok, msg)。
    """
    # 优先 ha_monitor（已知正确），失败再回退 root。
    conn = None
    used_for_connect = None
    last_err = None
    for (cu, cp, tag) in ((ha_user, ha_pass, "ha_monitor"),
                          (root_user, root_pass, "root")):
        if not cp:
            continue
        try:
            conn = connect(host, port, cu, cp, timeout=5)
            used_for_connect = tag
            break
        except Exception as e:
            last_err = e
    if conn is None:
        return False, "连接失败(ha_monitor 与 root 均不可达): %s" % last_err
    local_state_ok = [False]

    def _sync(cur):
        # 判定风味：MariaDB 与 MySQL 的账号/权限语法【不兼容】，必须分派。
        # MariaDB 无 SYSTEM_VARIABLES_ADMIN 等 MySQL 8.0 专有特权（直接 GRANT 会 1064）；
        # 但 MariaDB 10.5+ 提供 REPLICATION SLAVE ADMIN，12.x 执行 CHANGE MASTER/START SLAVE
        # 必须有该特权，故 MariaDB 分支需授予 REPLICATION SLAVE ADMIN（不可省略）。
        try:
            flavour = detect_flavour(cur)
        except Exception:
            flavour = F_MYSQL8

        # 认证插件：MySQL >= 8.0.34 用 caching_sha2_password，消除
        # "[MY-013360] mysql_native_password is deprecated" 告警（8.4 起该插件默认不可用）。
        # MariaDB 始终用自带的 mysql_native_password（无废弃问题）。
        version_str = ""
        try:
            cur.execute("SELECT VERSION()")
            _vr = cur.fetchone()
            version_str = (_vr[0] if _vr else "") or ""
        except Exception:
            version_str = ""
        try:
            from app.db import get_setting            # 局部导入，避免模块级循环依赖
            _prefer = (get_setting("AUTH_PLUGIN") or "auto")
        except Exception:
            _prefer = "auto"
        auth_plugin = default_auth_plugin(version_str, None, _prefer)
        if flavour == F_MARIADB:
            # MariaDB：认证插件显式 mysql_native_password（兼容管理器/老驱动）。
            # MariaDB 10.6 不支持 `IDENTIFIED VIA ... BY '明文'`，须用
            # `IDENTIFIED VIA mysql_native_password USING PASSWORD('明文')`
            # （PASSWORD() 函数对明文再哈希，与 init_mariadb.sql 一致，已验证可用）。
            idn = ident_clause(F_MARIADB, "mysql_native_password")
            idn_root = ident_clause(F_MARIADB, "mysql_native_password")
            # F13（2026-09-16 真机）：MariaDB 分支此前**漏定义 idn_repl**，而下方 accts 循环
            # 在 u==repl_user 时引用它 → NameError → ensure_ha_creds 对**所有 MariaDB 节点**
            # 恒失败（返回 False），凭证漂移自愈整条链路静默失效（表现为日志恒
            # "凭证巡检完成：0 个节点已对齐"、漂移永远自愈不了）。MariaDB 复制账号同样用
            # native 插件 + USING PASSWORD 语法，直接复用 idn 即可。
            idn_repl = idn
            # 注：MariaDB 10.5+（含 11/12.x）提供 REPLICATION SLAVE ADMIN / SLAVE MONITOR 特权：
            #  - 12.x 执行 CHANGE MASTER TO / START SLAVE 必须有 REPLICATION SLAVE ADMIN（仅 REPLICATION SLAVE 不够，报 1227）；
            #  - 12.x 执行 SHOW REPLICA STATUS / 读取复制状态必须有 SLAVE MONITOR（监控探活读复制状态用），否则报 1227；
            #  10.x 对这两项不强制，但显式授予对 10.5+ 兼容无害。
            # BINLOG ADMIN：MariaDB 10.5+ 把 `SET SESSION/LOCAL sql_log_bin` 从 SUPER 中拆出，
            # 缺它时 manager 写 ha_local.state（必须禁 binlog，否则从库 1062）会报 1227，
            # 结果主库标记一直写不进去、keepalived 读不到 current_master 而退化判定。
            # 与 node_gen 的 render_mariadb_init_sql / fix-node-creds.sql 保持一致。
            # v2.3.0：特权集由 mariadb_matrix 单一来源提供，并**按服务器真实支持的
            # 特权名过滤**（SHOW PRIVILEGES）—— 不再按版本号猜，也不再靠 except 吞
            # 1064/1227。此前 ha_client 这份比 node_gen 少了 REPLICATION MASTER
            # ADMIN，导致「init 建号有、自愈补号无」的不一致，一并消除。
            _avail = mariadb_matrix.server_privileges(cur)
            ha_grant = ("GRANT %s ON *.* TO '%s'@'%%'"
                        % (mariadb_matrix.ha_privs(_avail), _esc_sql(ha_user)))
            repl_grant = ("GRANT %s ON *.* TO '%s'@'%%'"
                          % (mariadb_matrix.repl_privs(_avail), _esc_sql(repl_user)))
        else:
            idn = ident_clause(flavour, auth_plugin)
            idn_root = ident_clause(flavour, auth_plugin)
            # replication 账号强制 mysql_native_password：MySQL 8.0 下非 SSL 复制 IO 线程
            # 对 caching_sha2_password 的握手与客户端 JDBC/驱动不同，常报 Access Denied，
            # 改为 native 后非 SSL 复制即可稳定连通（与节点 init 配方一致）。
            idn_repl = ident_clause(flavour, "mysql_native_password")
            ha_grant = ("GRANT REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
                        "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE ON *.* "
                        f"TO '{_esc_sql(ha_user)}'@'%'")
            repl_grant = f"GRANT REPLICATION SLAVE ON *.* TO '{_esc_sql(repl_user)}'@'%'"
        accts = [(ha_user, ha_pass, ha_grant), (repl_user, repl_pass, repl_grant)]
        if app_user and app_pass:
            app_grant = (f"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, "
                         f"REFERENCES, EXECUTE, SHOW VIEW, CREATE TEMPORARY TABLES ON *.* "
                         f"TO '{_esc_sql(app_user)}'@'%'")
            accts.append((app_user, app_pass, app_grant))
        # 幂等：账号/密码/权限无变化则不写任何语句（见 _ensure_account），
        # 彻底消除「主库每周期 ALTER/GRANT -> binlog -> 副本重放再写盘」的空闲写盘。
        # 账号 host 策略（开关 ACCOUNT_HOST_MODE，默认 legacy 仅 @'%'，行为完全不变）。
        # 开启后 MySQL 会按 host 精确度优先匹配：网段账号比 @'%' 更具体、优先生效。
        _acct_hosts = _account_hosts(manager_ip)
        for u, p, grant_sql in accts:
            for _h in _acct_hosts:
                _g = grant_sql if _h == "%" else grant_sql.replace("@'%'", "@'%s'" % _h)
                _acct_idn = idn_repl if u == repl_user else idn
                _acct_plugin = "mysql_native_password" if u == repl_user else auth_plugin
                _ensure_account(cur, u, _h, p, _acct_idn, _g,
                                conn_host=host, conn_port=port, plugin=_acct_plugin)
        # 关键：把 root@'<管理器IP>' 与 root@'%' 密码校正为管理器库存 root_pass。
        #  - manager_ip 的 root 用于管理器自愈回退；
        #  - 127.0.0.1 的 root 供 keepalived 的 check_mysql_master.sh 本机 TCP 探活使用
        #    （安装脚本按当时密码建过它，节点 root 口令一旦变更就会漂移 → 探活 1045
        #     → VIP 不上浮，故自愈必须把它刷成节点当前 root 口令）；
        #  - root@'%' 用于内网数据库客户端/其它主机远程连接（用户要求可远程登录）。
        # manager_ip 为空时跳过该条，但 root@'%' 始终创建（便于内网运维）。
        root_targets = []
        if manager_ip:
            root_targets.append(manager_ip)
        root_targets.append("127.0.0.1")
        if manager_subnet:
            # c/s 同机：管理器容器连【同机】节点时源 IP 是 docker 网关/容器网段，
            # 并非 manager_ip（真实宿主机 IP），故追加 docker 子网通配符白名单。
            root_targets.append(manager_subnet)
        root_targets.append("%")  # 内网远程访问
        for rh in root_targets:
            # 必须与 node_gen 生成的 fix-node-creds.sql 保持一致：
            # root 远程账号必须是 ALL PRIVILEGES + WITH GRANT OPTION。
            # 否则 root 只有 USAGE（CREATE USER 默认仅 USAGE）：一旦 ha_monitor 权限退化
            # （自身无 GRANT OPTION，无法自行补齐），root 回退也救不了，
            # 账号自愈就彻底锁死成死循环。
            root_grant = ("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION"
                          % (_esc_sql(root_user), rh))
            _ensure_account(cur, root_user, rh, root_pass, idn_root, root_grant,
                            conn_host=host, conn_port=port, plugin=auth_plugin)
        # 补授 ha_monitor 的额外特权（已授权时跳过，避免每周期重复 GRANT 写 binlog）。
        #  - CREATE USER：MySQL 8.0 与 MariaDB 均需要（MariaDB 中 CREATE USER 特权即隐含
        #    覆盖 ALTER USER / DROP USER，无需也不能单独 GRANT ALTER/DROP USER——后者在
        #    MariaDB 不是可授权权限，GRANT 会报 1064）。
        #  - READ_ONLY ADMIN：MariaDB 10.11+ / MySQL 8.0 需要，提升主库时关 read_only 用。
        #  - BINLOG ADMIN：MariaDB 10.5+ 需要，写 ha_local.state 前关闭会话 binlog 用。
        #    （对 MySQL 8.0 不存在该权限，GRANT 会失败但已被 except 吞掉，无副作用。）
        # 账号 DCL（CREATE USER / GRANT）绝不参与复制：节点级账号由各节点独立授予
        # （管理器逐节点直连写入），若经 binlog 复制到从库，从库上被 GRANT 的精确 host 账号
        # （如 ha_monitor@<manager_ip>）往往不存在，重放即报 1133
        # 「Can't find any matching row in the user table」→ SQL 线程卡死。
        # 故此处关会话 binlog，避免把该 GRANT 复制下去（从库由管理器直连单独授予）。
        _binlog_off = False
        try:
            cur.execute("SET SESSION sql_log_bin=0")
            _binlog_off = True
        except Exception:
            pass  # 缺 BINLOG ADMIN/SUPER（如 ha_monitor 首跑尚未获得）时跳过，由复制自愈兜底
        # v2.3.0：补授列表同样按服务器能力过滤（仅 MariaDB 分支）；MySQL 分支保持
        # 原列表与原有行为不变（不存在的特权依旧被 except 吞掉，无副作用）。
        if flavour == F_MARIADB:
            _extra_privs = mariadb_matrix.optional_privs(
                mariadb_matrix.server_privileges(cur))
        else:
            _extra_privs = ("CREATE USER", "READ_ONLY ADMIN", "BINLOG ADMIN",
                            "REPLICATION MASTER ADMIN")
        for extra in _extra_privs:
            # 同时授予 @'%' 与管理器实际连入用的 @manager_ip 账号（精确 host 优先匹配，
            # 管理器大多以 ha_monitor@<manager_ip> 连入，缺 REPLICATION MASTER ADMIN 会导致
            # enable_semisync 的 SET GLOBAL rpl_semi_sync_master_* 报 Access Denied）。
            for _h in ("%", manager_ip):
                if not _h:
                    continue
                try:
                    if extra.upper() not in _granted_privs(cur, ha_user, _h):
                        cur.execute("GRANT %s ON *.* TO '%s'@'%s'" % (extra, _esc_sql(ha_user), _h))
                except Exception:
                    pass
        # ---- F11：消除「精确 host 影子账号」造成的能力落差（2026-09-16 真机）----
        # 现象：节点上残留 ha_monitor@'<管理器IP>'（历史 fix 脚本交叉污染 / 旧版本遗留），
        #   MariaDB / MySQL 按 host **精确度优先**匹配 —— 管理器从管理器主机 IP 连入时
        #   命中该「影子账号」。若它只有 USAGE（或缺少 REPLICATION SLAVE ADMIN /
        #   SLAVE MONITOR），则表现为「连得上、却没权限」：CHANGE REPLICATION SOURCE
        #   报 1227、读复制状态报 1227 → 复制永久起不来、故障切换中止，而错误信息
        #   完全看不出是精确 host 账号在作祟（这正是 177/33 实测中「复制无法自愈」的根因）。
        # 修复：对 ha_monitor **每一个已存在的 host 变体**（并集 管理器IP ∪ '%'）执行全集
        #   授权，彻底抹平影子账号的能力落差。幂等：已具备全集者跳过，不写 binlog。
        #   刻意只补 ha_monitor：repl/app 不参与管理器连入，无需 per-host 变体。
        try:
            cur.execute("SELECT DISTINCT Host FROM mysql.user WHERE User=%s", (ha_user,))
            _ha_hosts = [r[0] for r in (cur.fetchall() or [])]
        except Exception:
            _ha_hosts = []
        _target_privs = _parse_grant_privs(ha_grant)
        _shadow_hosts = set([h for h in _ha_hosts if h] + ["%"]
                            + ([manager_ip] if manager_ip else []))
        for _h in _shadow_hosts:
            try:
                if _target_privs and _target_privs.issubset(_granted_privs(cur, ha_user, _h)):
                    continue
                _g = (ha_grant if _h == "%"
                      else ha_grant.replace("@'%'", "@'%s'" % _h))
                cur.execute(_g)
                print("[ha-shadow] 已为 ha_monitor@%s 补齐全集特权（消除精确 host 影子账号）"
                      % _h, flush=True)
            except Exception as _e:
                print("[ha-shadow] 修复 ha_monitor@%s 权限失败(可忽略): %s" % (_h, _e))
        # 账号改名：删除节点上残留的旧账号（旧账号可能以多个 host 存在）。
        # 必须在 sql_log_bin=0 作用域内执行，避免 DROP USER 经复制下放到从库。
        def _drop_old(cur, old_user, new_user):
            if not old_user or old_user == new_user:
                return
            try:
                cur.execute("SELECT DISTINCT Host FROM mysql.user WHERE User=%s", (old_user,))
                _hs = [r[0] for r in cur.fetchall()]
            except Exception:
                return
            for _h in _hs:
                try:
                    cur.execute("DROP USER IF EXISTS `%s`@`%s`" % (
                        old_user.replace("`", "``"), _h.replace("`", "``")))
                except Exception as _e:
                    print("[drop-old] 删除旧账号 %s@%s 失败: %s" % (old_user, _h, _e))
        _drop_old(cur, old_ha_user, ha_user)
        _drop_old(cur, old_repl_user, repl_user)
        _drop_old(cur, old_app_user, app_user)

        if _binlog_off:
            try:
                cur.execute("SET SESSION sql_log_bin=1")
            except Exception:
                pass
        # 节点本地状态库 ha_local 的读写授权（见 _ensure_local_state_grant 说明）
        local_state_ok[0] = _ensure_local_state_grant(cur, ha_user)

        # 半同步：rpl_semi_sync_*_enabled 不能写进静态 my.cnf——mysqld --initialize
        # 期间插件未加载，会报 unknown variable 并中止初始化（容器崩溃循环）。
        # 故改在运行时 SET GLOBAL 开启，随管理器自愈周期持久化（节点重启后自动重开）。
        if flavour == F_MYSQL8 and is_semi_sync_enabled():
            try:
                cur.execute("SET GLOBAL rpl_semi_sync_source_enabled=1")
                cur.execute("SET GLOBAL rpl_semi_sync_replica_enabled=1")
            except Exception as _se:
                import logging as _lg
                _lg.getLogger(__name__).warning("运行时开启半同步失败: %s", _se)

        # binlog 清理参数在线兜底（2026-09-22）：节点 my.cnf 已写入保留参数，但已部署且在运行的
        # 节点不会因 my.cnf 变更自动重读 ⇒ 每周期 SET GLOBAL 兜底，避免「复制 binlog 无限累积
        # 撑爆 /data」（历史版本未设导致磁盘告警）。
        # ⚠ 兜底语义：只在「永不过期(0)」时才介入 —— 0 才是本次磁盘告警的根因；
        #   已配置保留期的节点（实测 12.3.3 多为 10 天）保持原值，不擅自缩短其 PITR 窗口。
        # ⚠ 必须按 flavor 分流：MySQL 8.0 默认 binlog_expire_logs_seconds=2592000(30 天)，
        #   二者运行时互斥（不能同时非 0），直接 SET GLOBAL expire_logs_days 会被 3683 拒绝；
        #   MariaDB 则相反，两个变量互为别名（改一个另一个自动跟随）。
        # ⚠ 失败绝不能静默：一旦静默，节点重启后即退回「永不过期」，根治措施失效且无人知晓。
        try:
            if flavour == F_MYSQL8:
                cur.execute("SELECT @@GLOBAL.binlog_expire_logs_seconds")
                _sec = (cur.fetchone() or (None,))[0]
                # MySQL 默认已是 30 天(2592000)，只有被显式清零(0=永不过期)才需要兜底；
                # 已配置保留期的节点保持原值，不擅自缩短其 PITR 窗口。
                if _as_int(_sec) == 0:
                    # MySQL 8.0：两者互斥，必须先把 expire_logs_days 归零才能设 seconds
                    cur.execute("SET GLOBAL expire_logs_days = 0")
                    cur.execute("SET GLOBAL binlog_expire_logs_seconds = %d" % _RETENTION_SEC)
            else:
                cur.execute("SELECT @@GLOBAL.expire_logs_days")
                _eld = (cur.fetchone() or (None,))[0]
                # 只兜底「永不过期(0)」：实测在跑的 12.3.3 节点多为 10 天，强行改成 7 天
                # 等于静默缩短既有的 PITR 窗口 —— 0 才是本次磁盘告警的根因，只修它。
                if _as_int(_eld) == 0:
                    cur.execute("SET GLOBAL expire_logs_days = %d" % _RETENTION_DAYS)
            cur.execute("SELECT @@GLOBAL.max_binlog_size")
            _mbs = (cur.fetchone() or (None,))[0]
            if (_as_int(_mbs) or 0) > _MAX_BINLOG_SIZE:
                cur.execute("SET GLOBAL max_binlog_size = %d" % _MAX_BINLOG_SIZE)
            # MariaDB >= 11.4：给 binlog 总体积设硬上限 —— 按天保留只保证「时间」有界，
            # 写入量突增时仍可能撑爆 /data，这才是真正兜住磁盘的那道闸。
            # 实测环境同时存在 10.11/11.0 节点，那些版本无此变量（报 1193）属预期，静默跳过；
            # >= 11.4 上则由 slave_connections_needed_for_purge(默认 1) 保证不会删掉
            # 已连从库尚未应用的日志。
            if flavour != F_MYSQL8:
                try:
                    cur.execute("SELECT @@GLOBAL.max_binlog_total_size")
                    _cap = _as_int((cur.fetchone() or (None,))[0])
                    if _cap is not None and _cap != _BINLOG_TOTAL_SIZE_CAP:
                        cur.execute(
                            "SET GLOBAL max_binlog_total_size = %d" % _BINLOG_TOTAL_SIZE_CAP)
                except Exception:
                    # < 11.4 无此变量（已实测），非故障，不刷 warning
                    pass
        except Exception as _be:
            _k = (host, port)
            if _k not in _BINLOG_RETENTION_WARNED:
                _BINLOG_RETENTION_WARNED.add(_k)
                import logging as _blg
                _blg.getLogger(__name__).warning(
                    "节点 %s:%s 设置 binlog 保留参数失败，复制 binlog 可能无限累积撑爆磁盘: %s",
                    host, port, _be)

    cur = conn.cursor()
    used = used_for_connect
    try:
        _sync(cur)
    except Exception as e:
        # ha_monitor 连上了但缺 CREATE/ALTER USER 权限（典型：init 未授予、仅 USAGE），
        # 改不动 replication 密码。此时回退 root 重做整段账号同步（root 必然有权限）。
        # 这是「复制 Access denied」能被管理器自愈的关键路径。
        if used_for_connect == "ha_monitor" and root_pass:
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = connect(host, port, root_user, root_pass, timeout=5)
                cur = conn.cursor()
                _sync(cur)
                used = "root(回退)"
            except Exception as e2:
                return False, ("校正失败(经 ha_monitor: %s；root 回退也失败: %s)"
                               % (e, e2))
        else:
            return False, "校正失败(经 %s): %s" % (used_for_connect, e)
    # ha_local 的库级 GRANT 需要 GRANT OPTION。_sync 若走的是 ha_monitor（通常没有
    # GRANT OPTION），上面那步会静默失败，这里再用 root 补一次，确保 VIP 状态可写。
    if not local_state_ok[0] and used_for_connect == "ha_monitor" and root_pass:
        try:
            _rc = connect(host, port, root_user, root_pass, timeout=5)
            _ensure_local_state_grant(_rc.cursor(), ha_user)
            _rc.close()
        except Exception:
            pass
    try:
        cur.execute("FLUSH PRIVILEGES")
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    return True, "ok (via %s)" % used


def repair_ha_shadow_grants(host, port, ha_user, ha_pass, root_user="root", root_pass="",
                            manager_ip=""):
    """修复 ha_monitor「精确 host 影子账号」的权限落差（F11，**可安全用于只读副本**）。

    背景（2026-09-16 真机 177/33）：节点上可能残留 ha_monitor@'<管理器IP>' 等
    「精确 host」账号（历史 fix 脚本交叉污染 / 旧版本遗留 / ACCOUNT_HOST_MODE 切换）。
    MySQL / MariaDB 按 host **精确度优先**匹配 —— 管理器从管理器主机 IP 连入时命中该
    影子账号。若它只有 USAGE，则表现为「连得上、却没权限」：CHANGE REPLICATION SOURCE、
    SHOW REPLICA STATUS、写 ha_local 一律报 1227 → **复制永久无法自愈、故障切换中止**，
    而错误信息完全看不出是精确 host 账号在作祟。实测 33 号节点的复制正是被此卡死。

    与 ensure_ha_creds 的关键区别：本函数**只做 GRANT**（不 CREATE / ALTER USER、不改
    密码），因此可在**只读副本**上安全执行。副本上严禁 CREATE/ALTER USER（会与复制
    线程争用 mysql.* 元数据锁，导致 'Waiting for dependent transaction to commit'、
    复制卡死），但「为既有账号补齐授权」是低风险且**必要**的：否则该副本永远无法被
    管理器管理。GRANT 全程 ``SET SESSION sql_log_bin=0``，不复制下去、不污染主库。

    连接：优先 ha_monitor；若它自身即无权影子账号（连也白连），回退 root
    （root@'<管理器IP>' / root@'%' 恒为 ALL PRIVILEGES）。幂等：账号已具全集时不写库。

    返回 (repaired_count, msg)。
    """
    import logging as _logging
    _logger = _logging.getLogger(__name__)
    conn = None
    used_root = False
    try:
        # GRANT 需要 **GRANT OPTION**，而 ha_monitor 刻意不带该权限（最小权限原则）。
        # 故本修复**优先用 root**（节点上 root@'<管理器IP>' / root@'%' 均为
        # ALL PRIVILEGES WITH GRANT OPTION）；root 不可达时才退回 ha_monitor
        # （仅当它自身已带 GRANT OPTION 时才有意义，否则会 1044/1045 —— 不影响其它逻辑）。
        if root_pass:
            try:
                conn = connect(host, int(port), root_user, root_pass, timeout=5)
                used_root = True
            except Exception:
                conn = None
        if conn is None:
            conn = connect(host, int(port), ha_user, ha_pass, timeout=5)
        if conn is None:
            return 0, "无法连接节点(root/ha_monitor 均失败)"
        cur = conn.cursor()
        flavour = detect_flavour(cur)
        if flavour == F_MARIADB:
            target = mariadb_matrix.ha_privs(mariadb_matrix.server_privileges(cur))
        else:
            target = ("REPLICATION SLAVE, REPLICATION CLIENT, REPLICATION_SLAVE_ADMIN, "
                      "SYSTEM_VARIABLES_ADMIN, PROCESS, SELECT, RELOAD, SUPER, DELETE, EXECUTE")
        target_set = {p.strip().upper() for p in target.split(",") if p.strip()}
        try:
            cur.execute("SELECT DISTINCT Host FROM mysql.user WHERE User=%s", (ha_user,))
            _hosts = [r[0] for r in (cur.fetchall() or [])]
        except Exception:
            _hosts = []
        cand = set([h for h in _hosts if h] + ["%"]
                   + ([manager_ip] if manager_ip else []))
        _binlog_off = False
        try:
            cur.execute("SET SESSION sql_log_bin=0")
            _binlog_off = True
        except Exception:
            pass
        repaired = 0
        missing = []  # 未能补齐授权的 host（缺口），不再「可忽略」
        for h in cand:
            try:
                if target_set and target_set.issubset(_granted_privs(cur, ha_user, h)):
                    continue
                cur.execute("GRANT %s ON *.* TO '%s'@'%s'"
                            % (target, _esc_sql(ha_user), h))
                repaired += 1
                print("[ha-shadow] %s:%s 的 ha_monitor@%s 已补齐全集特权"
                      % (host, port, h), flush=True)
            except Exception as e:
                missing.append((h, str(e)))
                # 缺口会导致 1227（CHANGE REPLICATION / SET read_only 失败），必须露出
                print("[ha-shadow] %s:%s ha_monitor@%s 授权失败: %s"
                      % (host, port, h, e), flush=True)
                _logger.warning("[ha-shadow] %s:%s ha_monitor@%s 授权失败(缺口): %s",
                                host, port, h, e)
        if _binlog_off:
            try:
                cur.execute("SET SESSION sql_log_bin=1")
            except Exception:
                pass
        # 治本告警：存在授权缺口时明确暴露根因，避免「连得上却没权限」长期潜伏。
        # - root 不可达(used_root=False)是缺口主因：回退 ha_monitor 无 GRANT OPTION 无法补齐
        #   → 提示运维检查管理器 root_pass，否则手动/自动切换可能 1227 失败。
        # - root 可达仍失败属罕见（权限模型异常），同样需人工介入。
        if missing:
            _detail = "；".join("%s(%s)" % (h, e) for h, e in missing)
            if used_root:
                _subj = "ha_monitor 授权自愈未完全成功"
                _body = ("%s:%s 的 ha_monitor 精确 host 影子账号存在授权缺口"
                         "（root 可达但仍授权失败，疑似权限模型异常）：%s。"
                         "复制/切换可能因 1227 权限不足失败，请人工核查。"
                         % (host, port, _detail))
            else:
                _subj = "ha_monitor 授权自愈失败(根因: 管理器 root 不可达)"
                _body = ("%s:%s 的 ha_monitor 精确 host 影子账号授权不完整"
                         "（管理器 root 口令不可达，已回退 ha_monitor 但无 GRANT OPTION 无法补齐）。"
                         "缺授权 host: %s。"
                         "请检查管理器 root_pass 配置；否则手动/自动切换可能因 1227 权限不足失败。"
                         % (host, port, _detail))
            try:
                from app.alerts import send_alert
                send_alert(_subj, _body)
            except Exception as _ae:
                _logger.warning("send_alert 失败(授权缺口告警未发出): %s", _ae)
        return repaired, ("已修复 %d 个影子账号" % repaired if repaired else "无需修复")
    except Exception as e:
        return 0, "异常: %s" % e
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

"""节点状态读取：只读/可写、复制位点、binlog 用量、提主等。"""

from ._const import *
from ..mysql_compat import (detect_syntax, detect_flavour, F_MARIADB, F_MYSQL8, default_auth_plugin, reset_replica_sql, show_replica_status_sql, stop_replication_sql)


from .connection import (connect, connect_robust, _esc_sql)

from .connection import connect

# —— 拆包后的补齐导入（必读）——
# 本文件与 status/gtid/local_state/replication 共用同一段「账号/权限」代码（拆包时按职责
# 复制到了各子模块），这些辅助函数实体在 credentials 与 mysql_compat 中。缺少它们时
# py_compile 查不出来，只会在运行到那一行时抛 NameError —— 实测正是因此导致
# 「提升主库为可写」失败（name 'detect_syntax' is not defined）、账号建不出来、探活全失败。

from .credentials import (_native_password_hash, _parse_grant_privs, _granted_privs,
                          _account_hosts, _ensure_local_state_grant, _as_int,
                          _can_login, _ensure_account, _has_all_privileges)


def db_uptime(host, port, user, password, root_user="root", root_pass="", timeout=5):
    from .connection import (connect_robust)
    """返回数据库进程已运行时长（秒）；失败返回 None。

    用于「配置待生效」自动核验：recreate / 重建 / 重启数据库后，进程 Uptime 会重置为
    很小的值；若 Uptime < 待办产生后的经过秒数，说明数据库在 regen 之后被重建或重启过，
    该待办已落实，可自动清除。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    except Exception:
        return None
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SHOW STATUS LIKE 'Uptime'")
        row = cur.fetchone()
        if row:
            try:
                return int(row[1])
            except (TypeError, ValueError):
                return None
        return None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass



def is_read_only(host, port, ha_user, ha_pass, root_user="root", root_pass="", timeout=5):
    from .connection import (connect_robust)
    from .credentials import (ensure_ha_creds)
    """判断节点是否只读（副本）。

    用于自愈时跳过只读副本上的「直接账号 DDL / 改全局参数 DDL」：副本的账号与
    gtid_mode/server_id 均来自主库经复制同步，管理器在副本上直接执行这些 DDL 会与其
    复制来的同名语句争用元数据锁并漂权限（表现为 1045/1227 告警）。副本账号由主库
    ensure_ha_creds 周期性 binlog 出去自动纠正。
    """
    try:
        conn = connect_robust(host, port, ha_user, ha_pass, root_user, root_pass, timeout)
    except Exception:
        return False
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT @@global.read_only")
        row = cur.fetchone()
        # MariaDB 返回字符串 'OFF'/'ON'，MySQL 返回 0/1 整型。
        # 原 `bool(row[0])` 对 'OFF' 得 True（非空字符串皆为真），把**可写主库**
        # 误判成"只读副本"——于是主库上的账号 DDL / 全局变量修复被整体跳过，
        # 漂移永远得不到自愈。这里按两种形态统一判"只读"。
        _v = row[0] if row else None
        if isinstance(_v, (int, float)):
            return bool(int(_v))
        return str(_v).strip().upper() in ("1", "ON", "TRUE")
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass



def get_status(host, port, user, password, root_user="root", root_pass="", timeout=3):
    from .connection import (connect_robust)
    """探活并返回节点状态字典（供 monitor/failover 使用）。

    返回字段：alive, mode(master/slave/unknown), ro(0/1/None), role,
    seconds_behind, slave_io, slave_sql, master_host, master_port, error。
    """
    result = dict(
        alive=False, mode="unknown", ro=None, role="unknown",
        seconds_behind=None, slave_io=None, slave_sql=None,
        slave_io_err="", slave_sql_err="",
        master_host=None, master_port=None, error=None,
        connections=None, threads_running=None, qps=None,
        # 压力代理指标（宿主机资源在远程节点上拿不到，改用 MySQL 侧压力信号）
        row_lock_waits=None, slow_queries=None,
        # 半同步复制健康指标（SEMI_SYNC=0 时变量恒为 OFF，不影响核心探活）
        semi_master_status=None, semi_slave_status=None,
        semi_master_clients=None, semi_no_times=None,
        semi_no_tx=None, semi_yes_tx=None,
        # 复制位点（GTID 执行集合）：用于 monitor 区分「真在追平」与「SQL 线程卡死」
        exec_gtid="",
        # 数据交互状态：主库视角下的下游从库列表（SHOW REPLICAS / SHOW SLAVE HOSTS）
        downstream=[],
    )
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    except Exception as e:
        result["error"] = str(e)
        return result
    cur = conn.cursor()
    try:
        syntax = detect_syntax(cur)
        try:
            cur.execute("SELECT @@read_only")
            result["ro"] = cur.fetchone()[0]
        except Exception:
            result["ro"] = None
        try:
            cur.execute(show_replica_status_sql(syntax))
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                d = dict(zip(cols, row))
                result["mode"] = "slave"
                result["role"] = "slave"
                result["slave_io"] = d.get("Slave_IO_Running") or d.get("Replica_IO_Running")
                result["slave_sql"] = d.get("Slave_SQL_Running") or d.get("Replica_SQL_Running")
                result["slave_io_err"] = (d.get("Last_IO_Error") or d.get("Last_IO_Error") or "").strip()
                result["slave_sql_err"] = (d.get("Last_SQL_Error") or d.get("Last_SQL_Error") or "").strip()
                # 注意：复制延迟为 0（健康、已追平）时不能用 `or` 回退——
                # `0 or Seconds_Behind_Source(None)` 会把健康的 0 误判为 None，
                # 导致仪表盘「复制延迟」全部显示「—」。必须用 None 哨兵判断。
                sb = d.get("Seconds_Behind_Master")
                if sb is None:
                    sb = d.get("Seconds_Behind_Source")
                result["seconds_behind"] = sb
                result["master_host"] = d.get("Master_Host") or d.get("Source_Host")
                result["master_port"] = d.get("Master_Port") or d.get("Source_Port")
                # 复制位点（GTID 执行集合）：断电位点不前进 = SQL 线程卡死，而非真落后。
                # MySQL/MariaDB GTID 模式均返回 Executed_Gtid_Set；老 MariaDB 退回 Gtid_Slave_Pos。
                eg = (d.get("Executed_Gtid_Set") or d.get("Gtid_Slave_Pos") or "").strip()
                result["exec_gtid"] = eg
            else:
                # 无复制通道：主库或尚未建立复制。slave_io/sql 保持 None，避免被误判为 No/No 故障
                result["mode"] = result.get("mode") or "master"
                result["role"] = result.get("role") or "master"
                result["slave_io"] = None
                result["slave_sql"] = None
                # 主库视角：列出下游从库（数据推送给谁）。SHOW REPLICAS(>=8.0.22/10.5)
                # / SHOW SLAVE HOSTS 的 Host 取自从库 report_host，可能为空（故仅保留非空项）。
                try:
                    cur.execute("SHOW REPLICAS" if syntax == "new" else "SHOW SLAVE HOSTS")
                    for _r in cur.fetchall():
                        _d = dict(zip([c[0] for c in cur.description], _r))
                        _dh = (_d.get("Host") or "").strip()
                        _dp = _d.get("Port")
                        if _dh:
                            result["downstream"].append(
                                {"host": _dh, "port": int(_dp) if _dp else None})
                except Exception:
                    pass
        except Exception:
            pass
        # 半同步复制健康指标（与复制探测同级；SEMI_SYNC 关闭时变量恒为 OFF，不告警）
        try:
            cur.execute("SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync%'")
            ss = dict(cur.fetchall())
            def _si(k):
                try:
                    return int(ss.get(k) or 0)
                except Exception:
                    return None
            # MySQL 8.0.26+ 将半同步状态变量重命名（master→source, slave→replica），
            # 故同时兼容新旧两套命名，确保 MySQL 8.0 与 MariaDB 都能正确采集半同步健康度。
            def _semi_key(old, new):
                return old if old in ss else new
            result["semi_master_status"] = ss.get(_semi_key("Rpl_semi_sync_master_status", "Rpl_semi_sync_source_status"))
            result["semi_slave_status"] = ss.get(_semi_key("Rpl_semi_sync_slave_status", "Rpl_semi_sync_replica_status"))
            result["semi_master_clients"] = _si(_semi_key("Rpl_semi_sync_master_clients", "Rpl_semi_sync_source_clients"))
            result["semi_no_times"] = _si(_semi_key("Rpl_semi_sync_master_no_times", "Rpl_semi_sync_source_no_times"))
            result["semi_no_tx"] = _si(_semi_key("Rpl_semi_sync_master_no_tx", "Rpl_semi_sync_source_no_tx"))
            result["semi_yes_tx"] = _si(_semi_key("Rpl_semi_sync_master_yes_tx", "Rpl_semi_sync_source_yes_tx"))
        except Exception:
            pass
        # 轻量采集实时运行指标（供观测中心趋势图使用）。
        # 失败不影响核心探活结果，故整体包 try 吞掉。
        try:
            cur.execute("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            rc = cur.fetchone()
            result["connections"] = int(rc[1]) if rc else None
            cur.execute("SHOW GLOBAL STATUS LIKE 'Threads_running'")
            rr = cur.fetchone()
            result["threads_running"] = int(rr[1]) if rr else None
            # QPS：用两次 Questions 差值估算（此处仅取当前瞬时值，由调用方做差更准；
            # 采集线程已在 metrics.py 用相邻样本做差，这里提供原始值即可）。
            cur.execute("SHOW GLOBAL STATUS LIKE 'Queries'")
            rq = cur.fetchone()
            result["qps"] = int(rq[1]) if rq else None
            # 压力代理指标：均为累计计数器，由调用方按相邻样本做差得到「周期增量」
            cur.execute("SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'")
            rlk = cur.fetchone()
            result["row_lock_waits"] = int(rlk[1]) if rlk else None
            cur.execute("SHOW GLOBAL STATUS LIKE 'Slow_queries'")
            rsq = cur.fetchone()
            result["slow_queries"] = int(rsq[1]) if rsq else None
        except Exception:
            pass
        result["alive"] = True
        return result
    except Exception as e:
        result["error"] = str(e)
        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass



def set_read_only(host, port, user, password, on=True, root_user="root", root_pass=""):
    from .connection import (connect, connect_robust)
    # 写入隔离需要 READ_ONLY ADMIN 等特权；root 恒为 ALL PRIVILEGES，故**优先用 root**
    # （root 不可达时回退 ha_monitor）。与 promote_to_master 一致——否则 ha_monitor 精确 host
    # 影子账号缺特权时，SET GLOBAL read_only 报 1227 → 隔离失败 → 切换被交叉验证(witness)误判
    # 为脑裂风险而中止（见 do_failover_to 1.5）。这是「手动切换第一次成功、第二次失败」的根因：
    # 第一次旧主权限全、隔离成功；第二次旧主（从库节点）缺特权、隔离失败被 witness 中止。
    conn = None
    if root_pass:
        try:
            conn = connect(host, port, root_user, root_pass, timeout=5)
        except Exception:
            conn = None
    if conn is None:
        conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        cur = conn.cursor()
        ok_ro, err_ro = db_ops.set_read_only(cur, on)
        if not ok_ro:
            return False, err_ro
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def get_gtid_executed(host, port, user, password, root_user="root", root_pass="", timeout=5):
    from .connection import (connect_robust)
    """读取节点 @@GLOBAL.gtid_executed（主从均适用），用于选主时比较复制进度。

    返回字符串；连接失败或查询异常返回 None。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.gtid_executed")
            row = cur.fetchone()
            return (row[0] if row and row[0] else "") or ""
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass



def promote_to_master(host, port, user, password, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    """提升为可写主库：先清除残留复制通道，再关闭 read_only。

    必须先 STOP SLAVE + RESET SLAVE ALL，否则本节点仍挂着 SHOW SLAVE STATUS（指向旧主），
    monitor 会把它误判为 slave，且可能与新主形成环形复制（多次手动切换后「全变成从库」的根因）。

    【空闲零写入】ensure_topology 每个监控周期都会调用本函数。旧实现对已经是主库的节点
    仍会执行 STOP REPLICA / RESET REPLICA ALL / **FLUSH BINARY LOGS**；其中
    FLUSH BINARY LOGS 会新建并 fsync 一个 binlog 文件（节点模板为 sync_binlog=1），
    是「系统空闲却在节点上持续产生磁盘写入」的主因。
    故先做一次**只读**快检：已「可写且无残留复制通道」时直接返回，不执行任何写操作。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)

        # 只读快检：不产生任何写盘 / 不进 binlog
        # ⚠ 关键：MariaDB 的 @@GLOBAL.read_only 返回**字符串** 'OFF' / 'ON'
        #   （MySQL 才返回 0/1 整型）。原实现 int(_r[0]) 对 'OFF' 抛 ValueError，
        #   被下方 `except Exception: pass` 静默吞掉 → 快检形同虚设，
        #   每个监控周期都对「已经是主库」的节点跑完整 promote 流程，其中的
        #   FLUSH BINARY LOGS 会新建一个空 binlog —— 实测刷出 2700+ 个 433B 空文件、
        #   binlog 文件号疯涨而 GTID 纹丝不动。这里按两种形态统一判「可写」。
        try:
            cur.execute("SELECT @@GLOBAL.read_only")
            _r = cur.fetchone()
            _ro = _r[0] if _r else None
            if isinstance(_ro, (int, float)):
                _writable = int(_ro) == 0
            else:
                _writable = str(_ro).strip().upper() in ("0", "OFF", "FALSE")
            if _writable:
                cur.execute(show_replica_status_sql(syntax))
                if cur.fetchone() is None:
                    # 已是可写且无残留复制通道的主库：无需再做任何写操作
                    return True, None
        except Exception:
            pass
        try:
            cur.execute(stop_replication_sql(syntax))
            cur.execute(reset_replica_sql(syntax))
        except Exception:
            pass
        # MySQL 8.0 专有：super_read_only=ON 会强制 read_only=ON 并拒绝业务写入（报 1290），
        # 且不关闭它就直接关 read_only 会被拒绝/无效。故必须先关 super_read_only。
        # MariaDB 无该变量，语句报错属正常，忽略即可（不影响后一句执行）。
        # 关 read_only 是管理写操作：优先用 root 连接（root 恒为 ALL PRIVILEGES），
        # 避免 ha_monitor 精确 host 影子账号缺 READ_ONLY ADMIN/SUPER 时 SET GLOBAL
        # read_only=0 报 1227（提升主库失败、切换中止、无日志）。与
        # replication._fence_readonly 保持同一策略；root 不可用时回退当前连接。
        _ro_conn = conn
        if root_pass:
            try:
                _rc = connect_robust(host, port, root_user, root_pass, root_user, root_pass)
                if _rc is not None:
                    _ro_conn = _rc
            except Exception:
                _ro_conn = conn
        try:
            ok_ro, err_ro = db_ops.set_read_only(_ro_conn.cursor(), False)
        finally:
            if _ro_conn is not conn:
                try:
                    _ro_conn.close()
                except Exception:
                    pass
        if not ok_ro:
            return False, err_ro
        # 轮转 binlog：若该节点此前以从库/独立主形态运行，其当前 binlog 可能是在
        # gtid_mode=OFF/OFF_PERMISSIVE 时期写入、含匿名事务。提升为主库后，新从库以
        # AUTO_POSITION=1 来拉取时会因该匿名事务报 1236。轮转后新 binlog 仅含 GTID 事务，
        # 从根上避免「提升为主后从库匿名事务 1236」这类偶发故障（heal_anonymous_gtid_io
        # 负责清理仍被从库需要的旧 binlog）。
        try:
            cur.execute("FLUSH BINARY LOGS")
        except Exception:
            pass
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def get_node_utc_time(host, port, user, password, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    """读取节点 UTC 时间（SELECT UTC_TIMESTAMP()），用于时钟漂移检测。

    用 UTC 而非 NOW()：节点与管理器的系统时区可能不同，直接比较本地时间会因
    时区差异产生 8 小时级别的误报；UTC 可完全消除该歧义。

    返回 datetime 或 None。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass)
    except Exception:
        return None
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT UTC_TIMESTAMP()")
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass



def get_binlog_usage(host, port, user, password, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    """读取 binlog 占用总量（无需 SSH / OS 权限，纯 MySQL 侧可得）。

    为什么重要：远程节点没有 agent，OS 级磁盘指标不可得（api.py 注释明确说明），
    而「binlog 堆积撑爆磁盘」是 MySQL 最高频的生产事故之一——复制一断，binlog
    只增不减。这里用 SHOW BINARY LOGS 求和即可拿到占用量。

    返回 (total_bytes, file_count, oldest_file)；失败返回 (None, None, None)。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass)
    except Exception:
        return None, None, None
    if conn is None:
        return None, None, None
    try:
        cur = conn.cursor()
        cur.execute("SHOW BINARY LOGS")
        rows = cur.fetchall()
        if not rows:
            return 0, 0, None
        total = 0
        oldest = None
        for r in rows:
            try:
                total += int(r[1])
            except (TypeError, ValueError, IndexError):
                continue
            if oldest is None:
                oldest = r[0]
        return total, len(rows), oldest
    except Exception:
        return None, None, None
    finally:
        try:
            conn.close()
        except Exception:
            pass



def purge_binlog_before(host, port, user, password, dt_str, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    """PURGE BINARY LOGS BEFORE <dt_str>。

    ⚠ 危险操作：调用方必须已确认**所有从库都已追平**（否则从库需要的 binlog 被删，
    复制会永久报 1236，只能重建从库）。故仅在 monitor 的 binlog 守卫里、
    且 AUTO_PURGE_BINLOG=1 且全部从库无延迟时才会被调用。
    """
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass)
    except Exception as e:
        return False, f"连接失败: {e}"
    if conn is None:
        return False, "无法连接节点(ha_monitor/root 均失败)"
    try:
        cur = conn.cursor()
        cur.execute("PURGE BINARY LOGS BEFORE '%s'" % str(dt_str).replace("'", ""))
        return True, f"已清理 {dt_str} 之前的 binlog"
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


