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
            idn = "IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)"
            idn_root = "IDENTIFIED VIA mysql_native_password USING PASSWORD(%s)"
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
            idn = "IDENTIFIED WITH " + auth_plugin + " BY %s"
            idn_root = "IDENTIFIED WITH " + auth_plugin + " BY %s"
            # replication 账号强制 mysql_native_password：MySQL 8.0 下非 SSL 复制 IO 线程
            # 对 caching_sha2_password 的握手与客户端 JDBC/驱动不同，常报 Access Denied，
            # 改为 native 后非 SSL 复制即可稳定连通（与节点 init 配方一致）。
            idn_repl = "IDENTIFIED WITH mysql_native_password BY %s"
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
    conn = None
    try:
        # GRANT 需要 **GRANT OPTION**，而 ha_monitor 刻意不带该权限（最小权限原则）。
        # 故本修复**优先用 root**（节点上 root@'<管理器IP>' / root@'%' 均为
        # ALL PRIVILEGES WITH GRANT OPTION）；root 不可达时才退回 ha_monitor
        # （仅当它自身已带 GRANT OPTION 时才有意义，否则会 1044/1045 —— 不影响其它逻辑）。
        if root_pass:
            try:
                conn = connect(host, int(port), root_user, root_pass, timeout=5)
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
                print("[ha-shadow] %s:%s ha_monitor@%s 授权失败(可忽略): %s"
                      % (host, port, h, e))
        if _binlog_off:
            try:
                cur.execute("SET SESSION sql_log_bin=1")
            except Exception:
                pass
        return repaired, ("已修复 %d 个影子账号" % repaired if repaired else "无需修复")
    except Exception as e:
        return 0, "异常: %s" % e
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

"""GTID 解析与复制修复（IO/SQL 线程异常、匿名事务、位点缺口自愈）。"""

from ._const import *
from ..mysql_compat import (detect_syntax, detect_flavour, F_MARIADB, F_MYSQL8, default_auth_plugin, show_replica_status_sql, stop_replication_sql, start_replication_sql, change_replication_sql)


from .connection import (connect, connect_robust, _esc_sql)

# —— 拆包后的补齐导入（必读）——
# 本文件与 status/gtid/local_state/replication 共用同一段「账号/权限」代码（拆包时按职责
# 复制到了各子模块），这些辅助函数实体在 credentials 与 mysql_compat 中。缺少它们时
# py_compile 查不出来，只会在运行到那一行时抛 NameError —— 实测正是因此导致
# 「提升主库为可写」失败（name 'detect_syntax' is not defined）、账号建不出来、探活全失败。

from .credentials import (_native_password_hash, _parse_grant_privs, _granted_privs,
                          _account_hosts, _ensure_local_state_grant, _as_int,
                          _can_login, _ensure_account, _has_all_privileges)


def _derive_stuck_gtid(cur, syntax):
    from .connection import (_one_dict)
    """从 SHOW REPLICA STATUS 的 Retrieved/Executed GTID 集合推导当前卡住的事务 GTID。

    仅当存在「已接收但未执行」的事务时有效，返回 MariaDB GTID 格式 'domain-server-seq'。
    MySQL 8.0 的 uuid:seq 格式不在本函数范围（其错误文本通常已含 GTID，由上层正则捕获）。
    """
    cur.execute(show_replica_status_sql(syntax))
    row = _one_dict(cur)
    if not row:
        return None
    r = row.get("Retrieved_Gtid_Set") or ""
    e = row.get("Executed_Gtid_Set") or ""

    def _parse(s):
        d = {}
        for part in s.split(","):
            part = part.strip()
            if not part or "-" not in part:
                continue
            dom, rest = part.split("-", 1)
            sid, seqs = rest.split("-", 1)
            try:
                seq = max(int(x) for x in seqs.split(":"))
                d[int(dom)] = (int(sid), seq)
            except (ValueError, TypeError):
                continue
        return d

    rd, ed = _parse(r), _parse(e)
    cands = []
    for dom, (sid, rseq) in rd.items():
        eseq = ed.get(dom, (None, 0))[1]
        if rseq > eseq:
            cands.append((dom, sid, eseq + 1))
    if not cands:
        return None
    cands.sort()
    dom, sid, nxt = cands[0]
    return "%d-%d-%d" % (dom, sid, nxt)



def heal_replication(host, port, user, password, root_user="root", root_pass="",
                     allow_data_conflict=False):
    from .connection import (_esc_sql, _one_dict, _tiny_sleep, connect_robust)
    """自愈复制：当 Slave_SQL_Running=No 且错误为幂等冲突时，注入空事务跳过该 GTID。

    allow_data_conflict：是否允许跳过「数据冲突类」错误（1062/1032/1146）。
    默认 False —— 这类错误意味着主从数据已经不一致，跳过会静默放大分歧，
    应人工核对或重建从库。仅在运维明确接受风险时由 SKIP_DATA_CONFLICT 开启。


    典型场景：旧主重入 / 切换后，某节点物理数据已包含某事务（如 CREATE DATABASE）
    但其 GTID 执行集合未记录该事务，新主重放时触发 1007 等「已存在」错误导致
    SQL 线程中断。此时用 SET GTID_NEXT='<冲突GTID>'; BEGIN; COMMIT; 显式标记该
    GTID 已执行，再 START SLAVE 即可继续，无需 RESET MASTER（RESET MASTER 会清空
    GTID 集合、反而让同名事务被再次重放）。

    返回 (ok, msg)。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        # 循环跳过所有冲突事务：SQL 线程可能因多个事务连续冲突而反复停止（如 911 跳过后又
        # 在 912 处卡住）。仅处理首个冲突会导致从库 GTID 集合始终追不上主库，进而使
        # heal_anonymous_gtid_io 的安全校验失败、主库含匿名事务的 binlog 永不被清理，
        # 从库 IO 线程持续报 1236。故这里反复跳过，直到 SQL 线程持续运行或无可跳过的冲突。
        skipped = []
        for _ in range(200):
            cur.execute(show_replica_status_sql(syntax))
            row = _one_dict(cur)
            if not row:
                return False, "无复制通道（该节点非从库）"

            def g(key):
                return row.get(key)

            slave_sql = g("Slave_SQL_Running") or g("Replica_SQL_Running") or ""
            if slave_sql == "Yes":
                if skipped:
                    return True, f"已跳过冲突事务 {', '.join(skipped)}，复制恢复"
                return True, "复制正常，无需自愈"
            last_err = (g("Last_SQL_Error") or g("Replica_SQL_Error") or "") or ""
            last_code = g("Last_SQL_Errno") or g("Replica_SQL_Errno") or 0
            try:
                last_code = int(last_code)
            except (TypeError, ValueError):
                last_code = 0
            # 复制是否使用 GTID 自动定位（SHOW REPLICA STATUS 的 Auto_Position 列）。
            #  - AUTO_POSITION=1：用「SET GTID_NEXT='<冲突GTID>'+BEGIN/COMMIT」注入空事务跳过，
            #    这是最精确的方式，且不破坏 GTID 连续性。
            #  - AUTO_POSITION=0（传统 binlog 坐标复制 / 部分 MariaDB GTID 场景）：
            #    SET GTID_NEXT 不生效（本环境正是此情况——monitor 自愈静默失败、复制永久停，
            #    只能靠运维手动 `SET GLOBAL sql_slave_skip_counter=1` 才恢复）。此场景必须用
            #    sql_slave_skip_counter 跳过，否则复制永远卡在 Slave_SQL_Running=No。
            auto_pos = g("Auto_Position")
            try:
                auto_pos = int(auto_pos) if auto_pos is not None else 1
            except (TypeError, ValueError):
                auto_pos = 1
            if not last_err:
                # SQL 线程停了但没有错误信息，尝试直接重启后继续检查
                try:
                    cur.execute(start_replication_sql(syntax))
                except Exception as e:
                    return False, str(e)
                _tiny_sleep()
                continue
            # 若错误文本已点名冲突事务 GTID，则按「跳过该 GTID 空事务」处理——即使错误码不在
            # 幂等集合（如 1827：coordinator 仅转述 worker 失败，错误文本已写明
            # 'failed executing transaction <uuid>:<n>'）。这样可自愈 203 主库场景下
            # 'Worker 1 failed executing transaction 9da5e304:4' 这类复制中断。
            m = re.search(r"executing transaction '([0-9a-fA-F-]+:\d+)'", last_err)
            # GTID 自动定位 + 错误点名具体 GTID：走空事务注入（最精确，不丢 GTID 连续性）。
            if m and auto_pos == 1:
                gtid = m.group(1)
                try:
                    cur.execute(stop_replication_sql(syntax))
                    cur.execute(f"SET GTID_NEXT='{gtid}'")
                    cur.execute("BEGIN")
                    cur.execute("COMMIT")
                    cur.execute("SET GTID_NEXT='AUTOMATIC'")
                    cur.execute(start_replication_sql(syntax))
                    skipped.append(gtid)
                except Exception as e:
                    return False, str(e)
                _tiny_sleep()
                continue
            # ---- 1133：复制重放 DCL（GRANT/REVOKE）时目标账号在从库不存在 ----
            # MariaDB 多域 GTID 下 sql_slave_skip_counter 被禁（错误 1966），且错误文本不含事务
            # GTID，无法走空事务注入。最稳妥：在从库补建该账号，重放即成功、复制继续，零数据丢失；
            # 账号后续由 ensure_ha_creds 校正密码/权限。
            if last_code == 1133:
                uh = re.search(r"TO\s+'?([^'@\s]+)'?\s*@\s*'?'?([^'\s,)]+)", last_err) \
                     or re.search(r"FROM\s+'?([^'@\s]+)'?\s*@\s*'?'?([^'\s,)]+)", last_err)
                if uh:
                    cu, ch = uh.group(1), uh.group(2)
                    try:
                        cur.execute(stop_replication_sql(syntax))
                        cur.execute("CREATE USER IF NOT EXISTS '%s'@'%s'" % (_esc_sql(cu), _esc_sql(ch)))
                        cur.execute(start_replication_sql(syntax))
                        skipped.append("create_user:%s@%s" % (cu, ch))
                        _tiny_sleep()
                        continue
                    except Exception as e:
                        return False, f"1133 补建账号失败({cu}@{ch}): {e}"
                # 解析不到账号则落到下方 1966 兜底（空事务跳过该事务）
            # 数据冲突（1062/1032/1146）：跳过等于静默接受主从数据永久不一致，
            # 默认拒绝自动自愈；仅在运维显式开启 SKIP_DATA_CONFLICT 时才放行。
            if last_code in _DATA_CONFLICT_SQL_ERRNOS and not allow_data_conflict:
                return False, (
                    f"数据冲突(errno {last_code})：跳过会使本节点与主库数据永久不一致，"
                    f"已拒绝自动跳过，需人工核对或从主库重建本节点。{last_err}"
                    f"（若确认可接受风险，请在「系统设置 → 高级切换/监控」开启「允许跳过数据冲突」）")
            if last_code and last_code not in _SKIPPABLE_SQL_ERRNOS and not m:
                return False, f"复制错误码 {last_code} 非幂等冲突，需人工处理：{last_err}"
            # 非 GTID 自动定位，或无法解析具体 GTID：用 sql_slave_skip_counter 跳过。
            # 兼容传统 binlog 坐标复制，避免 SET GTID_NEXT 不生效导致自愈失败、复制卡死。
            try:
                cur.execute(stop_replication_sql(syntax))
                cur.execute("SET GLOBAL sql_slave_skip_counter = 1")
                cur.execute(start_replication_sql(syntax))
                skipped.append("skip_counter")
            except Exception as e:
                em = str(e)
                # MariaDB 多域 GTID：sql_slave_skip_counter 被禁（错误 1966）——改用「空事务注入」
                # 跳过卡住的事务：从 SHOW REPLICA STATUS 的 Retrieved/Executed GTID 集合推导其 GTID。
                if "1966" in em and auto_pos == 1:
                    try:
                        gx = _derive_stuck_gtid(cur, syntax)
                    except Exception:
                        gx = None
                    if gx:
                        try:
                            cur.execute("SET GTID_NEXT='%s'" % gx)
                            cur.execute("BEGIN")
                            cur.execute("COMMIT")
                            cur.execute("SET GTID_NEXT='AUTOMATIC'")
                            cur.execute(start_replication_sql(syntax))
                            skipped.append("gtid:%s" % gx)
                            _tiny_sleep()
                            continue
                        except Exception as e2:
                            return False, f"空事务跳过失败({gx}): {e2}"
                    return False, f"多域GTID无法跳过(1966)且无可用GTID: {em}"
                return False, f"跳过冲突事务失败(sql_slave_skip_counter): {em}"
            _tiny_sleep()
        return False, (f"跳过冲突事务次数过多（>200，含 {', '.join(skipped)}），"
                       f"可能存在结构性冲突，需人工处理")
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def parse_gtid_set(s):
    """解析 GTID 集合字符串为 {uuid: set([ints])}。

    支持形如 'uuid1:1-33,uuid2:1-19' 或 'uuid:1-5:7:9-11' 的写法。
    """
    res = {}
    if not s:
        return res
    for part in s.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        uuid, ranges = part.split(":", 1)
        nums = set()
        for r in ranges.split(":"):
            r = r.strip()
            if not r:
                continue
            if "-" in r:
                a, b = r.split("-", 1)
                try:
                    nums.update(range(int(a), int(b) + 1))
                except ValueError:
                    pass
            else:
                try:
                    nums.add(int(r))
                except ValueError:
                    pass
        res[uuid] = nums
    return res



def is_gtid_purged_error(err):
    """复制 IO 错误是否为「主库 binlog 已清理 / 从库状态过旧」类 1236。

    典型文本：
      Got fatal error 1236 from master when reading data from binary log:
      'Could not find GTID state requested by slave in any binlog files.
       Probably the slave state is too old and required binlog files have been purged.'

    这类故障【无法自愈】：重挂复制（RESET SLAVE + CHANGE MASTER）只会重新请求同一批
    已被清理的 GTID，注入空事务（heal_io_gtid_gap）解决的是相反方向（从库比主库多
    GTID）。唯一解法是从主库重新做一份数据——即「重建从库」。
    """
    e = (err or "").lower()
    if not e:
        return False
    return (
        "could not find gtid state requested by slave" in e
        or "required binlog files have been purged" in e
        or "slave state is too old" in e
        or ("1236" in e and ("purged" in e or "too old" in e))
    )



def is_gtid_missing_error(err):
    """复制 IO 错误是否为「主库 binlog 缺失从库所请求的 GTID」类 1236（主库 GTID 序列有空洞）。

    典型文本（MariaDB）：
      Got fatal error 1236 from master when reading data from binary log:
      'The binlog on the master is missing the GTID 1-1-12 requested by the slave
       (even though both a prior and a subsequent sequence number does exist),
       and GTID strict mode is enabled'

    与 is_gtid_purged_error（binlog 被清理 / 从库状态过旧）**不是一回事**：
    此处主库前后序列号都在、只缺中间一个 ⇒ 主库 GTID 序列存在「空洞」。
    典型成因：多节点 server_id / gtid_domain_id 撞车，各自产生同编号但内容不同的事务；
    或源端曾跳过事务后再被提升为主库。

    ⚠ 这类空洞补不回 binlog，且 MySQL 语义的 heal_io_gtid_gap（依赖 Source_UUID /
      Executed_Gtid_Set / SET GTID_NEXT）在 MariaDB 上取不到 source_uuid、会直接
      「无法确定源端标识，跳过」，故不能指望它自愈。唯一稳妥解法是从主库全量重建从库。
    """
    e = (err or "").lower()
    if not e:
        return False
    return ("missing the gtid" in e and "requested by the slave" in e) or \
           ("binlog on the master is missing" in e) or \
           ("missing the gtid" in e and "strict mode" in e)



def heal_io_gtid_gap(slave_host, slave_port, ha_user, ha_pass,
                     slave_root_user="root", slave_root_pass="",
                     master_host=None, master_port=None,
                     master_root_user="root", master_root_pass=""):
    from .connection import (_one_dict, connect_robust)
    """自愈复制 IO 线程 13114/1236：从库报「Replica has more GTIDs than the source has」。

    根因：复制源（主库）自身丢失了本应存在的 GTID（binlog 被截断/回滚，或源端 RESET
    MASTER），而从库已执行过这些 GTID。修复须在【源端/主库】为这些缺失 GTID 注入空事务，
    从库 IO 线程即可恢复重连。与 SQL 线程冲突跳过（作用在从库）不同，此修复作用在「主库」。

    返回 (acted, msg)：acted=True 表示已执行注入/恢复；False 表示无需处理或失败。
    """
    sconn = connect_robust(slave_host, slave_port, ha_user, ha_pass,
                           slave_root_user, slave_root_pass, timeout=5)
    if sconn is None:
        return False, "无法连接从库(ha_monitor/root均失败)"
    try:
        scur = sconn.cursor()
        ssyntax = detect_syntax(scur)
        scur.execute(show_replica_status_sql(ssyntax))
        row = _one_dict(scur)
        if not row:
            return False, "从库无复制通道，无需处理"
        def g(key):
            return row.get(key)
        io_err = (g("Last_IO_Error") or "") or ""
        # 仅处理「从库比源多 GTID」这一类 IO 错误
        if ("more GTIDs than the source" not in io_err
                and "13114" not in io_err and "1236" not in io_err):
            # 透出真实 IO 错误文本，便于从库侧直接看到连不上主库的真实原因
            return False, ("IO 错误非 GTID 缺失类，跳过" + (("：" + io_err) if io_err else ""))
        # 待修复的源端 = 从库实际连接的源（优先从复制状态读取，回退到传入的主库）
        target_host = g("Source_Host") or g("Master_Host") or master_host
        try:
            target_port = int(g("Source_Port") or g("Master_Port") or master_port or 0)
        except (TypeError, ValueError):
            target_port = 0
        source_uuid = g("Source_UUID") or g("Master_UUID") or ""
        executed = g("Executed_Gtid_Set") or ""
        if not source_uuid or not target_host or not target_port:
            return False, "无法确定源端标识，跳过"
        gtids = parse_gtid_set(executed)
        if source_uuid not in gtids or not gtids[source_uuid]:
            return False, "从库未执行源端 GTID，无需注入"
        # 连源端（主库），读其 gtid_executed，仅注入主库缺失的部分
        mconn = connect_robust(target_host, target_port, ha_user, ha_pass,
                               master_root_user, master_root_pass, timeout=5)
        if mconn is None:
            return False, f"无法连接源端主库 {target_host}:{target_port}(ha_monitor/root均失败)"
        try:
            mcur = mconn.cursor()
            mcur.execute("SELECT @@GLOBAL.gtid_executed")
            master_executed = (mcur.fetchone() or (None,))[0] or ""
            master_has = parse_gtid_set(master_executed).get(source_uuid, set())
            missing = sorted(n for n in gtids[source_uuid] if n not in master_has)
            if missing:
                try:
                    for n in missing:
                        mcur.execute(f"SET GTID_NEXT='{source_uuid}:{n}'")
                        mcur.execute("BEGIN")
                        mcur.execute("COMMIT")
                    mcur.execute("SET GTID_NEXT='AUTOMATIC'")
                except Exception as e:
                    try:
                        mcur.execute("SET GTID_NEXT='AUTOMATIC'")
                    except Exception:
                        pass
                    return False, f"在源端注入空事务失败: {e}"
            # 重启从库 IO 线程，立即清除报错（Connect_Retry 默认 60s，主动重启更快）
            try:
                scur.execute("STOP REPLICA IO_THREAD")
                scur.execute("START REPLICA IO_THREAD")
            except Exception:
                try:
                    scur.execute("STOP SLAVE IO_THREAD")
                    scur.execute("START SLAVE IO_THREAD")
                except Exception:
                    pass
            return True, (f"已在源端 {target_host}:{target_port} 为 {source_uuid} "
                          f"补齐 {len(missing)} 个缺失 GTID，并重启从库 IO 线程")
        finally:
            try:
                mconn.close()
            except Exception:
                pass
    except Exception as e:
        return False, str(e)
    finally:
        try:
            sconn.close()
        except Exception:
            pass



def _io_error_is_auth(io_err: str) -> bool:
    s = (io_err or "").lower()
    return any(m in s for m in _AUTH_IO_MARKERS)



def _start_io_thread(cur):
    """启动 IO 线程（MySQL 8.0 / 5.7 两种语法都试）。"""
    for s in ("START REPLICA IO_THREAD", "START SLAVE IO_THREAD"):
        try:
            cur.execute(s)
            return True
        except Exception:
            continue
    return False



def _stop_io_thread(cur):
    """停止 IO 线程（失败可忽略：未运行时本就会报错）。"""
    for s in ("STOP REPLICA IO_THREAD", "STOP SLAVE IO_THREAD"):
        try:
            cur.execute(s)
            return True
        except Exception:
            continue
    return False



def _stop_replica(cur):
    """完整停掉复制（IO + SQL 两个线程）。

    ⚠ 执行 CHANGE MASTER TO / CHANGE REPLICATION SOURCE TO 之前必须用它，
    只停 IO 线程是不够的：SQL 线程仍在跑时会直接报
        1198: This operation cannot be performed as you have a running slave '';
              run STOP SLAVE '' first
    「一键修复 / 重建复制通道」曾因此整体失败（修复动作看起来毫无效果）。
    """
    for s in ("STOP REPLICA", "STOP SLAVE"):
        try:
            cur.execute(s)
            return True
        except Exception:
            continue
    return False



def heal_io_auth(slave_host, slave_port, ha_user, ha_pass,
                 slave_root_user="root", slave_root_pass="",
                 repl_user=None, repl_pass=None,
                 master_host=None, master_port=None,
                 master_root_user="root", master_root_pass=""):
    from .connection import (_esc_sql, _one_dict, _tiny_sleep, connect_robust)
    """自愈复制 IO 线程的【认证类】错误。

    典型报错（MySQL 8.0 + caching_sha2_password + 非 TLS 复制）：
        Authentication plugin 'caching_sha2_password' reported error:
        Authentication requires secure connection.

    根因：从库以明文 TCP 连主库，caching_sha2_password 在没有 TLS、也没有索取
    服务端 RSA 公钥的情况下无法完成密码交换。原先这类错误会被
    heal_io_gtid_gap 判为「非 GTID 缺失类」直接跳过，导致一键修复无效。

    修复分两级：
      1) 重建复制通道并带 SOURCE_PUBLIC_KEY=1（官方推荐：向服务端索取 RSA 公钥）；
      2) 若仍失败（服务端未开启 RSA 自动密钥 / 不支持该选项），到【源端】把复制
         账号改为 mysql_native_password，再重启 IO 线程。

    返回 (acted, msg)。
    """
    sconn = connect_robust(slave_host, slave_port, ha_user, ha_pass,
                           slave_root_user, slave_root_pass, timeout=5)
    if sconn is None:
        return False, "无法连接从库(ha_monitor/root均失败)"
    try:
        scur = sconn.cursor()
        ssyntax = detect_syntax(scur)
        scur.execute(show_replica_status_sql(ssyntax))
        row = _one_dict(scur)
        if not row:
            return False, "从库无复制通道，无需处理"

        def g(k):
            return row.get(k)

        io_err = (g("Last_IO_Error") or "") or ""
        if not _io_error_is_auth(io_err):
            return False, "IO 错误非认证类，跳过" + (("：" + io_err) if io_err else "")

        target_host = g("Source_Host") or g("Master_Host") or master_host
        try:
            target_port = int(g("Source_Port") or g("Master_Port") or master_port or 0)
        except (TypeError, ValueError):
            target_port = 0
        if not target_host or not target_port or not repl_user:
            return False, "缺少源端地址或复制账号，无法重建复制通道"

        flavour = detect_flavour(scur)
        notes = []

        # ---------- 第一级：带 SOURCE_PUBLIC_KEY=1 重建通道 ----------
        # 必须完整 STOP SLAVE（含 SQL 线程），否则 CHANGE MASTER TO 报 1198。
        _stop_replica(scur)
        try:
            scur.execute(change_replication_sql(
                flavour, target_host, target_port, repl_user, repl_pass or "",
                public_key=True, assign_anon_gtids=(flavour == F_MYSQL8)))
            notes.append("已带 SOURCE_PUBLIC_KEY=1 重建复制通道")
        except Exception as e_pub:
            notes.append(f"带公钥重建失败({e_pub})，改为不带公钥重建")
            try:
                scur.execute(change_replication_sql(
                    flavour, target_host, target_port, repl_user, repl_pass or "",
                    public_key=False))
            except Exception as e2:
                return False, f"重建复制通道失败: {e2}"
        _start_io_thread(scur)

        _tiny_sleep(1.5)
        scur.execute(show_replica_status_sql(ssyntax))
        row2 = _one_dict(scur) or {}
        if row2.get("Slave_IO_Running") == "Yes" or row2.get("Replica_IO_Running") == "Yes":
            return True, "；".join(notes) + f"；IO 线程已恢复（源 {target_host}:{target_port}）"

        io_err2 = (row2.get("Last_IO_Error") or "") or ""

        # ---------- 第二级：到源端把复制账号降级为 mysql_native_password ----------
        mconn = connect_robust(target_host, target_port, ha_user, ha_pass,
                               master_root_user, master_root_pass, timeout=5)
        if mconn is None:
            return False, (f"重建通道后 IO 仍未恢复，且无法连接源端 {target_host}:{target_port} "
                           f"做降级处理（当前错误：{io_err2 or '未知'}）")
        try:
            mcur = mconn.cursor()
            quser = _esc_sql(repl_user)
            qpass = _esc_sql(repl_pass or "")
            last_err = None
            # 常见两种 host 形态：'%' 与从库 IP
            for host_part in ("'%'", f"'{slave_host}'"):
                try:
                    mcur.execute(
                        f"ALTER USER '{quser}'@{host_part} "
                        f"IDENTIFIED WITH mysql_native_password BY '{qpass}'")
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
            if last_err is not None:
                # 账号可能根本不存在：直接按 native 插件创建并授权
                try:
                    mcur.execute(
                        f"CREATE USER '{quser}'@'%' "
                        f"IDENTIFIED WITH mysql_native_password BY '{qpass}'")
                    mcur.execute(f"GRANT REPLICATION SLAVE ON *.* TO '{quser}'@'%'")
                    last_err = None
                except Exception as e2:
                    return False, (f"源端改用 mysql_native_password 失败: {last_err} / {e2}"
                                   f"（当前错误：{io_err2 or '未知'}）")
            notes.append(f"已在源端把复制账号 {repl_user} 改为 mysql_native_password")
        finally:
            try:
                mconn.close()
            except Exception:
                pass

        # 源端改完后重建通道（此时不再需要公钥）并重启 IO 线程
        _stop_replica(scur)
        try:
            scur.execute(change_replication_sql(
                flavour, target_host, target_port, repl_user, repl_pass or "",
                public_key=False))
        except Exception as e:
            notes.append(f"重建通道失败({e})")
        _start_io_thread(scur)

        _tiny_sleep(1.5)
        scur.execute(show_replica_status_sql(ssyntax))
        row3 = _one_dict(scur) or {}
        if row3.get("Slave_IO_Running") == "Yes" or row3.get("Replica_IO_Running") == "Yes":
            return True, "；".join(notes) + f"；IO 线程已恢复（源 {target_host}:{target_port}）"
        return False, ("；".join(notes) + "；IO 仍未恢复："
                       + (row3.get("Last_IO_Error") or "未知错误"))
    except Exception as e:
        return False, str(e)
    finally:
        try:
            sconn.close()
        except Exception:
            pass



def heal_anonymous_gtid_io(slave_host, slave_port, ha_user, ha_pass,
                           slave_root_user="root", slave_root_pass="",
                           master_host=None, master_port=None,
                           master_root_user="root", master_root_pass=""):
    from .connection import (_one_dict, connect_robust)
    """自愈 IO 错误 1236 'Cannot replicate anonymous transaction when AUTO_POSITION=1'。

    根因：源端(主库) binlog 含【匿名事务】(gtid_mode 曾为 OFF 时写入，或经 OFF_PERMISSIVE
    过渡写入)，而 AUTO_POSITION=1 无法复制匿名事务 → 从库 IO 线程每次重连都从 binlog 头部
    读到该匿名事务而报 1236。该匿名事务通常已被从库执行过（位于从库 Executed_Gtid_Set 内），
    故可安全清除，无需 RESET MASTER（会丢 GTID 集合）。

    安全步骤：
      1) 先让从库 SQL 线程把已接收的 GTID 全部应用（heal_replication 跳过冲突事务），
         确保从库 Executed ⊇ 主库即将清理的 binlog 区间；
      2) 主库 FLUSH BINARY LOGS 生成全新(纯 GTID)的 binlog；
      3) 主库 PURGE BINARY LOGS TO 当前 binlog（删掉含匿名事务的旧 binlog；
         安全前提：步骤1已保证从库执行过其中全部 GTID，purge 前再做一次集合校验）；
      4) 从库 STOP REPLICA + START REPLICA，AUTO_POSITION 从新 binlog 续传，不再遇匿名事务。

    返回 (acted, msg)。
    """
    sconn = connect_robust(slave_host, slave_port, ha_user, ha_pass,
                           slave_root_user, slave_root_pass, timeout=5)
    if sconn is None:
        return False, "无法连接从库(ha_monitor/root均失败)"
    try:
        scur = sconn.cursor()
        ssyntax = detect_syntax(scur)
        scur.execute(show_replica_status_sql(ssyntax))
        row = _one_dict(scur)
        if not row:
            return False, "从库无复制通道，无需处理"
        io_err = (row.get("Last_IO_Error") or row.get("Last_IO_Error") or "") or ""
        if "anonymous transaction" not in io_err:
            return False, "非匿名事务类 IO 错误，跳过"
        target_host = row.get("Source_Host") or row.get("Master_Host") or master_host
        try:
            target_port = int(row.get("Source_Port") or row.get("Master_Port")
                              or master_port or 0)
        except (TypeError, ValueError):
            target_port = 0
        if not target_host or not target_port:
            return False, "无法确定源端(主库)地址，跳过"
        # 步骤1：让从库应用已接收的 GTID（修复 SQL 冲突，如 1827 点名的事务）
        try:
            heal_replication(slave_host, slave_port, ha_user, ha_pass,
                             slave_root_user, slave_root_pass)
        except Exception as e:
            return False, f"从库 SQL 自愈失败: {e}"
        # 取从库执行集合，用于 purge 前安全校验
        scur.execute("SELECT @@GLOBAL.gtid_executed")
        slave_executed = (scur.fetchone() or (None,))[0] or ""
        # 步骤2/3：主库轮转 + 清理含匿名事务的旧 binlog
        mconn = connect_robust(target_host, target_port, ha_user, ha_pass,
                               master_root_user, master_root_pass, timeout=5)
        if mconn is None:
            return False, (f"无法连接源端主库 {target_host}:{target_port}"
                           f"(ha_monitor/root均失败)")
        try:
            mcur = mconn.cursor()
            mcur.execute("SELECT @@GLOBAL.gtid_executed")
            master_executed = (mcur.fetchone() or (None,))[0] or ""
            # 安全校验：从库必须已执行主库全部 GTID，否则 purge 会丢数据
            slave_g = parse_gtid_set(slave_executed)
            master_g = parse_gtid_set(master_executed)
            for uuid, nums in master_g.items():
                if not nums <= slave_g.get(uuid, set()):
                    return False, (f"从库尚未执行主库全部 GTID（{uuid} 缺失），"
                                   f"不安全清理主库 binlog，请先让复制追平后再试")
            mcur.execute("FLUSH BINARY LOGS")
            mcur.execute("SHOW BINARY LOGS")
            logs = mcur.fetchall()
            cur_bin = None
            for lg in logs or []:
                if isinstance(lg, dict):
                    cur_bin = lg.get("Log_name") or cur_bin
                elif isinstance(lg, (list, tuple)):
                    cur_bin = lg[0] if lg else cur_bin
            if not cur_bin:
                return False, "无法确定主库当前 binlog"
            mcur.execute(f"PURGE BINARY LOGS TO '{cur_bin}'")
        finally:
            try:
                mconn.close()
            except Exception:
                pass
        # 步骤4：从库重启复制线程，从新(干净的) binlog 续传
        try:
            scur.execute(stop_replication_sql(ssyntax))
            scur.execute(start_replication_sql(ssyntax))
            return True, (f"已在主库 {target_host}:{target_port} 轮转并清理含匿名事务的 binlog，"
                          f"从库 {slave_host}:{slave_port} 已重连续传")
        except Exception as e:
            return False, f"主库 binlog 已清理，但从库重启复制失败: {e}"
    except Exception as e:
        return False, str(e)
    finally:
        try:
            sconn.close()
        except Exception:
            pass


