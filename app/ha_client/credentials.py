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


def connect(host, port, user, password, timeout=5):
    """惰性转发到 connection.connect，避免与 connection<->credentials 的循环导入。"""
    from .connection import connect as _connect
    return _connect(host, port, user, password, timeout=timeout)


def _esc_sql(s):
    """惰性转发到 connection._esc_sql，同上避免循环导入。"""
    from .connection import _esc_sql as _e
    return _e(s)


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
        # 2026-10-12：补授对象必须是「ha_monitor 在 mysql.user 里**实际存在**的 host 变体」，
        # 不能只写死 @'%' 与 @manager_ip —— 实测 188 上 ha_monitor 只有 '%' 与 '192.168.1.%'
        # （根本没有 @'192.168.1.100'），而管理器正是从 192.168.1.100 连入、按**精确度优先**
        # 命中 @'192.168.1.%' ⇒ 补到 @'%'/@manager_ip 上的特权它一个也用不到，
        # SHOW BINARY LOGS 照样 1227（binlog 守卫静默失效）。故取实际变体并集后逐个补授；
        # 探测失败时才回退旧口径，且不存在的账号绝不 GRANT（GRANT 不隐式建号，会报 1133）。
        try:
            cur.execute("SELECT DISTINCT Host FROM mysql.user WHERE User=%s", (ha_user,))
            _extra_hosts = [r[0] for r in (cur.fetchall() or []) if r[0]]
        except Exception:
            _extra_hosts = []
        if not _extra_hosts:
            _extra_hosts = [h for h in ("%", manager_ip) if h]
        for extra in _extra_privs:
            # 对每一个实际存在的 host 变体补授（精确 host 优先匹配，管理器连入命中的
            # 那个账号才必须齐权；缺 REPLICATION MASTER ADMIN 会导致 enable_semisync
            # 的 SET GLOBAL rpl_semi_sync_master_* 报 Access Denied）。
            for _h in _extra_hosts:
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
        # 2026-10-12：只对**实际存在**的 host 变体授权 —— 把 '%' / manager_ip 无脑塞进候选时，
        # 若节点上根本没有 ha_monitor@'%' 或 @'<管理器IP>'，GRANT 会报 ERROR 1133
        # "Can't find any matching row in the user table"（GRANT 不隐式建号），
        # 该错误被下面 except 记进 missing ⇒ 触发「ha_monitor 授权自愈未完全成功」**误告警**。
        # 账号确实不存在时由 ensure_ha_creds 负责 CREATE；本函数只做 GRANT，不越俎代庖。
        cand = [h for h in _hosts if h]
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

"""HA 账号/权限的探测、建立与校正（root 回退、host 去 %% 集合）。"""

from ._const import *
from ..mysql_compat import (detect_flavour, F_MARIADB, F_MYSQL8, default_auth_plugin)


def reset_replication_creds_on_master(master_host, master_port, ha_user, ha_pass,
                                       root_user, root_pass, repl_user, repl_pass,
                                       slave_ip=None):
    from .connection import (connect)
    from .replication import (make_slave_of)
    """在主库上重置复制账号密码，解决从库 IO 报
    ``Access denied for user 'replication'@...`` 的凭证漂移问题。

    根因通常是主库的 ``replication@'%'`` 密码与集群库存 ``repl_pass`` 不一致（多为
    该主库之前未以真实 root 跑过 ``fix-node-creds.sql``，ha_monitor 仅 USAGE 无法自愈）。
    从库侧密码是对的（make_slave_of 已用库存 repl_pass 重指），所以必须把主库账号
    密码改回库存值。

    MySQL 8.0 默认 caching_sha2_password，非 TLS 复制会失败；强制 mysql_native_password
    可彻底规避。同时按从库 IP 建同名同密账号，避免 host 精确匹配把连接导向别的 host
    模式账号导致密码被遮蔽。

    优先 ha_monitor，连上但缺权限（典型 USAGE-only）时回退 root。返回 (ok, msg)。
    """
    def _do(cur):
        from app.mysql_compat import detect_flavour, F_MARIADB
        # MariaDB 不支持 MySQL 8 的 `IDENTIFIED WITH <plugin> BY '明文'`（实测 12.3.3 报
        # 1064 语法错，直接使「重置复制密码」失败、从库卡在 Access denied）。MariaDB 默认
        # 即 mysql_native_password，用 `IDENTIFIED BY` 即达到「强制明文口令」的等价效果；
        # 仅 MySQL 8 需要 `WITH mysql_native_password BY` 以规避 caching_sha2_password
        # 非 TLS 复制失败。
        flavour = detect_flavour(cur)
        _ident = ident_clause(flavour, "mysql_native_password")
        # 用参数化执行，避免 repl_pass 含 ' / \ 时破坏语句（自定义密码场景）。
        hosts = ["%"]
        if slave_ip and slave_ip != master_host:
            hosts.append(slave_ip)
        for h in hosts:
            try:
                cur.execute(
                    "CREATE USER IF NOT EXISTS %s@%s " + _ident,
                    (repl_user, h, repl_pass))
            except Exception:
                # 5.7/MariaDB 不支持 CREATE USER 作 GRANT 项等：忽略，后面用 ALTER 兜底
                pass
            cur.execute(
                "ALTER USER %s@%s " + _ident,
                (repl_user, h, repl_pass))
            cur.execute(
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO %s@%s",
                (repl_user, h))
        cur.execute("FLUSH PRIVILEGES")

    who = None
    conn = None
    try:
        conn = connect(master_host, master_port, ha_user, ha_pass, timeout=8)
        who = "ha_monitor"
    except Exception as e:
        if not root_pass:
            return False, "主库 ha_monitor 连接失败: %s" % e
        try:
            conn = connect(master_host, master_port, root_user, root_pass, timeout=8)
            who = "root(%s)" % root_user
        except Exception as e2:
            return False, ("主库 ha_monitor 连接失败: %s；root 回退也失败: %s" % (e, e2))
    cur = conn.cursor()
    try:
        _do(cur)
        return True, ("已在主库 %s:%s（用 %s）重置复制账号 %s 密码为 mysql_native_password"
                      % (master_host, master_port, who, repl_user))
    except Exception as e:
        # ha_monitor 连上了但缺 CREATE/ALTER USER 权限（典型：init 未授予、仅 USAGE），
        # 改不动 replication 密码。此时回退 root 重做（root 必然有权限）。
        if who == "ha_monitor" and root_pass:
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = connect(master_host, master_port, root_user, root_pass, timeout=8)
                cur = conn.cursor()
                _do(cur)
                who = "root(%s)(回退)" % root_user
                return True, ("已在主库 %s:%s（用 %s）重置复制账号 %s 密码为 mysql_native_password"
                              % (master_host, master_port, who, repl_user))
            except Exception as e2:
                return False, ("主库重置复制密码失败(ha_monitor: %s；root 回退也失败: %s)"
                               % (e, e2))
        return False, "主库重置复制密码失败(%s): %s" % (who, e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def alter_user_pass(host, port, user, password, target_user, target_pass,
                    root_user="root", root_pass=""):
    from .connection import (connect_robust)
    """
    用 (user/password) 连接后，将 target_user@'%' 的密码改为 target_pass。
    回退：若 user 连不上，则用 root 连接执行。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    try:
        cur = conn.cursor()
        # MariaDB 不支持 MySQL 8 的 `IDENTIFIED WITH <plugin> BY '明文'`（实测 12.3.3 报
        # 1064 语法错）；MariaDB 默认即 mysql_native_password，用 `IDENTIFIED BY` 等价。
        from app.mysql_compat import detect_flavour, F_MARIADB
        _fl = detect_flavour(cur)
        _idn = ident_clause(_fl, "mysql_native_password")
        cur.execute(
            "ALTER USER %s@'%%' " + _idn,
            (target_user, target_pass),
        )
        cur.execute("FLUSH PRIVILEGES")
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def _native_password_hash(password):
    """计算 MySQL mysql_native_password 存储的 authentication_string（*+SHA1(SHA1(pwd))）。

    用于自愈时比对「当前密码哈希」与「期望密码哈希」：仅当不一致才 ALTER USER，
    避免每周期无条件重写密码 -> 进 binlog -> 副本重放+再写自身 binlog -> 空闲也持续写盘。
    """
    if password is None:
        return None
    s1 = hashlib.sha1(password.encode("utf-8")).digest()
    # 注意：二次哈希要取 hexdigest（str），不能用 digest() 后再 .hexdigest()
    # —— bytes 没有 .hexdigest()，会抛 'bytes' object has no attribute 'hexdigest'，
    # 导致整个账号自愈链路失效。
    s2 = hashlib.sha1(s1).hexdigest()
    return "*" + s2.upper()



def _parse_grant_privs(grant_sql):
    """从 GRANT 语句里解析出权限集合（用于与账号已有权限比对，判断是否需重授权）。"""
    if not grant_sql:
        return set()
    m = re.match(r"\s*GRANT\s+(.+?)\s+ON\s", grant_sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return set()
    return {p.strip().upper() for p in m.group(1).split(",") if p.strip()}



def _granted_privs(cur, user, host):
    """读取账号当前已授予的全局权限集合（用于幂等判断，避免重复 GRANT 写 binlog）。"""
    try:
        cur.execute("SHOW GRANTS FOR %s@%s", (user, host))
    except Exception:
        return set()
    privs = set()
    for (g,) in cur.fetchall():
        privs |= _parse_grant_privs(g)
    return privs



def _can_login(host, port, user, password, timeout=3):
    from .connection import (connect)
    """实连校验账号密码。

    caching_sha2_password 的 authentication_string 形如 ``$A$005$<随机盐><摘要>``，
    盐每次 ALTER 都变，无法在管理端本地复算比对，故改用「真的连一次」判断是否一致。
    """
    if not host or not port:
        return False
    conn = None
    try:
        conn = connect(host, int(port), user, password, timeout=timeout)
        return True
    except Exception:
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass



def _has_all_privileges(cur, user, host):
    """判断账号是否已拥有全部权限（兼容 MySQL / MariaDB 两种 SHOW GRANTS 形式）。"""
    try:
        cur.execute("SHOW GRANTS FOR %s@%s", (user, host))
        rows = cur.fetchall()
    except Exception:
        return False
    have = set()
    for (g,) in rows:
        if "ALL PRIVILEGES" in (g or "").upper():
            return True
        have |= _parse_grant_privs(g)
    return _ALL_PRIV_SIG.issubset(have)



def _ensure_local_state_grant(cur, ha_user):
    from .local_state import (set_local_master_flag)
    """确保 ha_monitor 能读写节点本地状态库 ha_local（keepalived VIP 检查脚本依赖）。

    根因：MySQL 8.0 分支授予 ha_monitor 的 *.* 权限里没有 INSERT/CREATE，
    于是 set_local_master_flag 的 `REPLACE INTO ha_local.state` 报 1142，
    被上层 `except: pass` 吞掉 —— ha_local.state 永远为空，
    节点上的 keepalived 检查脚本读不到 current_master，VIP 无法归属/漂移。

    这里按**最小权限**只授 ha_local 库（不放大 *.* 权限）。
    返回是否授权成功（失败通常意味着当前连接无 GRANT OPTION）。
    """
    try:
        cur.execute("CREATE DATABASE IF NOT EXISTS ha_local")
    except Exception:
        pass   # 无 CREATE 权限时，库通常已在节点初始化阶段建好
    try:
        cur.execute(
            "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON ha_local.* TO '%s'@'%%'" % ha_user)
        return True
    except Exception:
        return False



def _account_hosts(manager_ip=None):
    """按 ACCOUNT_HOST_MODE 返回要为 ha_monitor / replication / app 创建的 host 列表。

    legacy（默认）: ['%']            仅 @'%'，与历史行为完全一致
    additive      : ['%', '<段>.%']  追加网段账号，**保留 @'%' 兜底**（灰度第一步，最安全）
    segment       : ['<段>.%']       仅网段（灰度终态；不自动 DROP @'%'，可随时改回回滚）

    网段推导失败（manager_ip 为空 / 非 IPv4）时一律回退 ['%']：
    宁可维持现状，也绝不赌网段 —— 猜错会让管理器连不上所有节点，
    这比保留 % 危险得多（参考 config.get_manager_public_ip 关于 docker 桥接
    探测错位的说明）。
    """
    if ACCOUNT_HOST_MODE not in ("additive", "segment"):
        return ["%"]
    seg = segment_of(manager_ip or "")
    if not seg:
        return ["%"]
    return ["%", seg] if ACCOUNT_HOST_MODE == "additive" else [seg]



def ensure_semisync_runtime(host, port, ha_user, ha_pass, root_user=None,
                            root_pass=None, timeout=5):
    from .connection import (connect)
    """按【节点】开启半同步（运行期 SET GLOBAL）——补齐**副本**侧开关。

    为什么必须单独做这件事（根因）：
        ensure_ha_creds 只在主库执行。账号/权限会经复制同步到副本，
        但 **SET GLOBAL 不会复制**，所以副本的 rpl_semi_sync_*_enabled 永远停在 OFF；
        主库等不到任何半同步副本 -> 主库 source_status 与副本 replica_status 恒为 OFF，
        表现为「半同步明明配了却一直 OFF」。
    本函数对**每个节点**单独执行（含副本）。MySQL 要求修改
    rpl_semi_sync_replica_enabled 后重启复制 IO 线程才会以半同步方式重连，
    故副本上检测到开关刚被打开时会自动重启 IO 线程。

    幂等：开关已 ON 时直接返回，不产生任何写操作/不复制。
    返回 (changed: bool, msg: str)
    """
    if not is_semi_sync_enabled():
        return False, "SEMI_SYNC 未开启，跳过"
    conn = None
    last_err = None
    for cu, cp in ((ha_user, ha_pass), (root_user, root_pass)):
        if not cu:
            continue
        try:
            conn = connect(host, port, cu, cp, timeout=timeout)
            break
        except Exception as e:
            last_err = e
    if conn is None:
        return False, "不可达: %s" % last_err
    try:
        cur = conn.cursor()
        try:
            flavour = detect_flavour(cur)
        except Exception:
            return False, "无法判定数据库类型"
        if flavour == F_MARIADB:
            # 幂等：已开启则直接返回，不产生写操作、也【不刷自愈日志】。
            # monitor 仅在 changed=True 时打印自愈信息；若每周期都返回 True，
            # 每个 MariaDB 节点每个间隔都会刷一行「半同步自愈」（MySQL 分支用 already-on 规避）。
            def _on_m(x):
                return str(x or "").strip().upper() in ("ON", "1")

            try:
                cur.execute("SHOW VARIABLES LIKE 'rpl_semi_sync%_enabled'")
                _pre = {r[0]: r[1] for r in (cur.fetchall() or [])}
            except Exception:
                _pre = {}
            if _on_m(_pre.get("rpl_semi_sync_master_enabled")) \
                    and _on_m(_pre.get("rpl_semi_sync_slave_enabled")):
                return False, "already-on"
            # MariaDB 半同步：插件内置，通常由 node_gen 注入的 my.cnf（plugin_load_add +
            # rpl_semi_sync_*_enabled=1）随启动启用。但若节点生成时未注入该块（早于该修复，
            # 或 SEMI_SYNC 后开未 regen），插件可能未加载、变量恒为 OFF —— 这里运行时补启用，
            # 使「半同步配了却一直 OFF」对 my.cnf 缺块也 resilient，无需重装节点。
            def _mset(var):
                try:
                    cur.execute("SET GLOBAL %s=1" % var)
                    return True, ""
                except Exception as e:
                    return False, str(e)

            m_ok, m_err = _mset("rpl_semi_sync_master_enabled")
            s_ok, s_err = _mset("rpl_semi_sync_slave_enabled")

            def _semi_unk(s):
                return "unknown system variable" in s.lower() or "does not exist" in s.lower()

            if (not m_ok or not s_ok) and _semi_unk(m_err + " " + s_err):
                # 插件未加载：运行时 INSTALL PLUGIN（MariaDB 半同步 .so 内置）
                for _sn, _pn in (("semisync_master.so", "rpl_semi_sync_master"),
                                 ("semisync_slave.so", "rpl_semi_sync_slave")):
                    try:
                        cur.execute("INSTALL PLUGIN %s SONAME '%s'" % (_pn, _sn))
                    except Exception as e:
                        if "already loaded" not in str(e).lower():
                            print("[semi] MariaDB 加载插件 %s 失败: %s" % (_pn, e))
                m_ok, m_err = _mset("rpl_semi_sync_master_enabled")
                s_ok, s_err = _mset("rpl_semi_sync_slave_enabled")
            # 最佳努力：对齐 AFTER_SYNC 等待点 + 超时（与 my.cnf 意图一致）
            try:
                cur.execute("SET GLOBAL rpl_semi_sync_master_wait_point='AFTER_SYNC'")
            except Exception:
                pass
            try:
                cur.execute("SET GLOBAL rpl_semi_sync_master_timeout=10000")
            except Exception:
                pass
            restarted = False
            try:
                cur.execute("SHOW SLAVE STATUS")
                _row = cur.fetchone()
                if _row:
                    _cols = [d[0] for d in cur.description]
                    _d = dict(zip(_cols, _row))
                    if _d.get("Slave_IO_Running") == "Yes":
                        cur.execute("STOP SLAVE")
                        cur.execute("START SLAVE")
                        restarted = True
            except Exception:
                pass
            try:
                conn.commit()
            except Exception:
                pass
            try:
                cur.execute("SHOW VARIABLES LIKE 'rpl_semi_sync%_enabled'")
                _now = {r[0]: r[1] for r in (cur.fetchall() or [])}
            except Exception:
                _now = {}
            _m_on = str(_now.get("rpl_semi_sync_master_enabled", "")).strip().upper() in ("ON", "1")
            _s_on = str(_now.get("rpl_semi_sync_slave_enabled", "")).strip().upper() in ("ON", "1")
            if not (_m_on and _s_on):
                _why = (m_err + " " + s_err).strip() or "SET GLOBAL 未生效"
                return False, "MariaDB 半同步开启失败（复核未 ON）：%s" % _why
            return True, "MariaDB 已启用半同步" + ("并重启复制 IO 线程" if restarted else "")
        # 以下为 MySQL 8 原逻辑
        cur.execute("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync%_enabled'")
        vals = dict(cur.fetchall())

        def _v(new, old):
            return vals.get(new, vals.get(old))

        def _on(x):
            return str(x or "").strip().upper() in ("ON", "1")

        if _on(_v("rpl_semi_sync_source_enabled", "rpl_semi_sync_master_enabled")) \
                and _on(_v("rpl_semi_sync_replica_enabled", "rpl_semi_sync_slave_enabled")):
            return False, "already-on"
        # 两套命名都试：MySQL 8.0.26+ 用 source/replica，MariaDB（含 11.4 及以下）只有
        # master/slave（source/replica 在 11.5+ 才是别名）。此前只 SET source/replica，
        # 在 MariaDB 上必然失败却被 except: pass 吞掉 → 每轮都报「已开启半同步」的假成功。
        failed = []
        for var in ("rpl_semi_sync_source_enabled", "rpl_semi_sync_replica_enabled",
                    "rpl_semi_sync_master_enabled", "rpl_semi_sync_slave_enabled"):
            try:
                cur.execute("SET GLOBAL %s=1" % var)
            except Exception as e:
                failed.append("%s(%s)" % (var, (str(e) or "").strip()[:60]))
        # 副本：改完开关需重启 IO 线程才会真正以半同步方式重连主库
        restarted = False
        try:
            cur.execute("SHOW REPLICA STATUS")
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            if row:
                d = dict(zip(cols, row))
                if d.get("Replica_IO_Running") == "Yes" or d.get("Slave_IO_Running") == "Yes":
                    cur.execute("STOP REPLICA")
                    cur.execute("START REPLICA")
                    restarted = True
        except Exception:
            pass
        try:
            conn.commit()
        except Exception:
            pass

        # 复核：只有变量真的变成 ON 才算成功。此前无论 SET 是否成功都 return True，
        # 1227 权限不足/变量不存在被吞掉，导致 monitor 每 60s 记一次「自愈成功」，
        # 自愈统计一片大好、半同步却从未真正开启。
        try:
            cur.execute("SHOW VARIABLES LIKE 'rpl_semi_sync%_enabled'")
            now = {r[0]: r[1] for r in (cur.fetchall() or [])}
        except Exception:
            now = {}

        def _now(new, old):
            return now.get(new, now.get(old))

        src_on = _on(_now("rpl_semi_sync_source_enabled", "rpl_semi_sync_master_enabled"))
        rep_on = _on(_now("rpl_semi_sync_replica_enabled", "rpl_semi_sync_slave_enabled"))
        if not (src_on and rep_on):
            why = "；".join(failed) or "SET GLOBAL 未生效（多为 1227 权限不足，需 SUPER/SYSTEM_VARIABLES_ADMIN）"
            return False, "半同步开启失败（复核仍未 ON）：%s" % why
        return True, "已开启半同步" + ("并重启复制 IO 线程" if restarted else "")
    except Exception as e:
        return False, "异常: %s" % e
    finally:
        try:
            conn.close()
        except Exception:
            pass



def ensure_connect_creds(host, port, ha_user, ha_pass, root_user, root_pass,
                         manager_ip=None, manager_subnet=None, timeout=5):
    from .connection import (_esc_sql, connect)
    """仅校正【本节点】的 ha_monitor 与 root 登录凭证，使管理器能连上该节点。

    与 ensure_ha_creds 的区别：
      - 只处理 ha_monitor@'%'（+ @manager_ip）与 root@<各 host>，不动 replication/app 账号
        （replication 账号是主库权威、由主库经复制下发；app 同），故对副本完全安全；
      - 全程在 SET SESSION sql_log_bin=0 下执行，**不写 binlog、不回灌主库**，
        可在副本上直连校正，杜绝账号 DCL 经复制放大 / 元数据锁争用。

    解决：副本重装后 init SQL 写入的 ha_monitor/root 与管理器库存不一致（节点包生成后
    改过口令 / 重加密等），导致管理器连不上副本 -> 副本永远 "down" -> 不被重指向、
    不自动加入集群。优先用 root 连接（具 SUPER 可先关 binlog），不通再回退 ha_monitor。
    返回 (ok, msg)。
    """
    conn = None
    used = None
    for (cu, cp, tag) in ((root_user, root_pass, "root"), (ha_user, ha_pass, "ha_monitor")):
        if not cu:
            continue
        try:
            conn = connect(host, port, cu, cp, timeout=timeout)
            used = tag
            break
        except Exception:
            pass
    if conn is None:
        return False, "不可达（root/ha_monitor 均失败）"
    try:
        cur = conn.cursor()
        try:
            _flavour = detect_flavour(cur)
        except Exception:
            _flavour = F_MYSQL8
        version_str = ""
        try:
            cur.execute("SELECT VERSION()")
            _vr = cur.fetchone()
            version_str = (_vr[0] if _vr else "") or ""
        except Exception:
            pass
        try:
            from app.db import get_setting
            _prefer = (get_setting("AUTH_PLUGIN") or "auto")
        except Exception:
            _prefer = "auto"
        _auth = default_auth_plugin(version_str, None, _prefer)
        if _flavour == F_MARIADB:
            idn = ident_clause(F_MARIADB, "mysql_native_password")
            idn_root = ident_clause(F_MARIADB, "mysql_native_password")
        else:
            idn = ident_clause(_flavour, _auth)
            idn_root = ident_clause(_flavour, _auth)
        # 关 binlog：本节点本地校正，不复制到主库（避免 DCL 回灌 / 放大）
        try:
            cur.execute("SET SESSION sql_log_bin=0")
        except Exception:
            pass
        for _h in ("%", manager_ip):
            if not _h:
                continue
            _ensure_account(cur, ha_user, _h, ha_pass, idn, None,
                           conn_host=host, conn_port=port, plugin=_auth)
        root_targets = []
        if manager_ip:
            root_targets.append(manager_ip)
        root_targets.append("127.0.0.1")
        if manager_subnet:
            root_targets.append(manager_subnet)
        root_targets.append("%")
        for rh in root_targets:
            _g = ("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s' WITH GRANT OPTION"
                  % (_esc_sql(root_user), rh))
            _ensure_account(cur, root_user, rh, root_pass, idn_root, _g,
                           conn_host=host, conn_port=port, plugin=_auth)
        return True, ("已校正本节点连接凭证(ha_monitor/root，binlog 安全，以 %s 连接)" % used)
    except Exception as e:
        return False, "异常: %s" % e
    finally:
        try:
            conn.close()
        except Exception:
            pass



def _as_int(v):
    """把 MySQL 返回值安全转 int。

    MariaDB 的 expire_logs_days 形如 '10.000000'，直接 int() 会 ValueError ——
    一旦发生在 try/except 里就会变成又一次静默失败，故统一走 int(float())。
    """
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None
