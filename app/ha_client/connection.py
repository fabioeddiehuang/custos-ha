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
                except Exception as _e:
                    import logging as _lg
                    # 补授失败→后续 enable_semisync 的 SET GLOBAL 会 Access Denied，必须露出
                    _lg.getLogger(__name__).warning("补授半同步额外权限 %s 失败(ha_monitor@%s): %s", extra, _h, _e)
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
            except Exception as _e:
                import logging as _lg
                _lg.getLogger(__name__).warning("恢复会话 binlog 失败(SET SESSION sql_log_bin=1): %s", _e)
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
            except Exception as _e:
                import logging as _lg
                _lg.getLogger(__name__).debug("关闭连接异常: %s", _e)
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
        except Exception as _e:
            import logging as _lg
            # 此处失败会导致 ha_local 授权未补全 → keepalived 读不到 current_master → VIP 无法归属
            _lg.getLogger(__name__).warning("root 回退补全 ha_local 授权失败: %s", _e)
    try:
        cur.execute("FLUSH PRIVILEGES")
    except Exception as _e:
        import logging as _lg
        _lg.getLogger(__name__).warning("FLUSH PRIVILEGES 失败: %s", _e)
    try:
        conn.close()
    except Exception as _e:
        import logging as _lg
        _lg.getLogger(__name__).debug("关闭连接异常: %s", _e)
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
        except Exception as _e:
            # 关 binlog 失败则后续 GRANT 会进 binlog（被复制到从库），值得告警
            _logger.warning("关闭会话 binlog 失败(SET SESSION sql_log_bin=0): %s", _e)
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
            except Exception as _e:
                _logger.warning("恢复会话 binlog 失败(SET SESSION sql_log_bin=1): %s", _e)
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
        except Exception as _e:
            import logging as _lg
            _lg.getLogger(__name__).debug("关闭连接异常: %s", _e)

"""底层连接与游标工具（与节点建连、root 回退、元组/Dict 游标辅助）。"""

from ._const import *
from .credentials import (_granted_privs, _account_hosts, _parse_grant_privs, _ensure_local_state_grant, _native_password_hash, _can_login, _ensure_account, _has_all_privileges, _as_int)


def _pymysql_at_least(major, minor):
    try:
        parts = [int(x) for x in pymysql.__version__.split(".")[:2]]
    except Exception:
        return False
    return tuple(parts) >= (major, minor)


def _tiny_sleep(seconds=0.3):
    """自愈循环里重启复制后给 SQL 线程一点时间应用/再次停下，便于下一轮检测冲突。"""
    try:
        time.sleep(seconds)
    except Exception as _e:
        import logging as _lg
        _lg.getLogger(__name__).debug("tiny_sleep 被打断: %s", _e)



def _one_dict(cur):
    """把当前结果集的第一行转成 dict（按列名索引）。

    关键：connect() 默认使用元组游标，直接用 row[key] 会失败；
    务必先按 cur.description 转 dict，否则读不到 Slave_SQL_Running 等字段。
    """
    if not cur.description:
        return None
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None



def _esc_sql(s):
    """转义 SQL 字符串字面量（反斜杠 + 单引号），供所有把凭证/辨识名拼进 SQL 的语句使用。

    集群账号名/密码允许管理员自定义，含 `'` 或 `\\` 时若不转义，整条语句语法错误 ->
    账号静默建不出来（表现为「自定义用户名密码登不上」）。参数化 execute 已安全，此处
    仅用于少数无法参数化、必须用字符串拼装的授权/降级语句（用户名无特殊字符但为稳妥也转义）。
    """
    return str(s if s is not None else "").replace("\\", "\\\\").replace("'", "''")



def connect(host, port, user, password, timeout=5):
    """建立连接；失败时抛 RuntimeError（带原始异常信息）。

    注意：get_server_public_key 仅 PyMySQL >= 1.0.0 支持。但实测某些容器内的
    PyMySQL「版本号 ≥1.0.0 却实际不支持该参数」（镜像源/缓存调包或安装不干净），
    光靠版本号判断不可靠。故：先按版本守卫尝试携带该参数；若仍抛 TypeError
    （无论版本号怎么报），去掉该参数重试，保证原生密码等常规连接不受影响。
    """
    base = dict(
        host=host, port=int(port), user=user, password=password,
        connect_timeout=timeout, autocommit=True,
        charset="utf8mb4",
    )
    attempts = [base]
    if _PYMYSQL_SUPPORTS_PUBKEY:
        attempts.insert(0, {**base, "get_server_public_key": True})
    last_err = None
    for kwargs in attempts:
        try:
            return pymysql.connect(**kwargs)
        except TypeError as e:
            # 老版本 / 被调包的 PyMySQL 不识别 get_server_public_key：去掉后重试。
            if "get_server_public_key" in str(e):
                last_err = e
                continue
            raise
        except Exception as e:
            raise RuntimeError(str(e))
    raise RuntimeError(str(last_err))



def connect_robust(host, port, user, password, root_user="root", root_pass="", timeout=5):
    """
    优先用 ha_monitor(user/password) 连接；
    失败（账号漂移/拒绝）时回退用 root(root_user/root_pass) 连接。

    两者都失败时合并错误信息——既指出 ha_monitor（首选账号，缺失/密码漂移才是根因），
    也保留 root 回退结果，避免只显示 root 的 1045、掩盖「init 未跑/ha_monitor 缺失」真相。
    """
    ha_err = None
    try:
        return connect(host, port, user, password, timeout=timeout)
    except Exception as e:
        ha_err = e
    if root_pass:
        try:
            return connect(host, port, root_user, root_pass, timeout=timeout)
        except Exception as e2:
            raise RuntimeError(
                "ha_monitor(%s) 连接失败: %s；root(%s) 回退也失败: %s"
                % (user, ha_err, root_user, e2)
            )
    raise ha_err



_PYMYSQL_SUPPORTS_PUBKEY = _pymysql_at_least(1, 0)
from app.mysql_compat import (detect_syntax, detect_flavour, flavour_from_node_type,
                           change_replication_sql, change_replication_filter_sql,
                           start_replication_sql,
                           stop_replication_sql, reset_replica_sql, show_replica_status_sql,
                           default_auth_plugin, F_MYSQL8, F_MARIADB)
