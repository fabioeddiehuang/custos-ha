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

"""复制搭建与切换核心：make_slave_of / 起停从库 / 提主 / VIP 守卫。"""

from ._const import *
from ..mysql_compat import (detect_syntax, detect_flavour, F_MARIADB, F_MYSQL8, default_auth_plugin, change_replication_filter_sql, reset_replica_sql, stop_replication_sql, start_replication_sql, change_replication_sql)


from .connection import (connect, connect_robust, _esc_sql)

# —— 拆包后的补齐导入（必读）——
# 本文件与 status/gtid/local_state/replication 共用同一段「账号/权限」代码（拆包时按职责
# 复制到了各子模块），这些辅助函数实体在 credentials 与 mysql_compat 中。缺少它们时
# py_compile 查不出来，只会在运行到那一行时抛 NameError —— 实测正是因此导致
# 「提升主库为可写」失败（name 'detect_syntax' is not defined）、账号建不出来、探活全失败。

from .credentials import (_native_password_hash, _parse_grant_privs, _granted_privs,
                          _account_hosts, _ensure_local_state_grant, _as_int,
                          _can_login, _ensure_account, _has_all_privileges)


def restart_slave_io(host, port, user, password, root_user="root", root_pass="", timeout=8):
    from .connection import (connect_robust)
    """重启从库 IO 线程，使其用新密码立即重连主库（无需等 60s 重试窗口）。"""
    try:
        conn = connect_robust(host, port, user, password, root_user, root_pass, timeout=timeout)
    except Exception as e:
        return False, "重启从库 IO 线程：连接失败: %s" % e
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(stop_replication_sql(syntax) + " IO_THREAD")
        cur.execute(start_replication_sql(syntax) + " IO_THREAD")
        return True, "已重启从库 %s:%s IO 线程" % (host, port)
    except Exception as e:
        return False, "重启从库 IO 线程失败: %s" % e
    finally:
        try:
            conn.close()
        except Exception:
            pass



def _fence_readonly(host, port, user, password, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    """分裂态封写：SET GLOBAL read_only=ON（MySQL 同步 super_read_only）。

    返回 (ok, err)。优先用 root 连接（避免 ha_monitor 缺 READ_ONLY ADMIN）。
    仅在「节点自认主但非当前主、且就地纠正失败」的分裂态调用——正常主库/从库不会触发。
    """
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root 均失败)"
    try:
        cur = conn.cursor()
        flavour = detect_flavour(cur)
        try:
            if flavour == F_MYSQL8:
                cur.execute("SET GLOBAL super_read_only=ON")
            cur.execute("SET GLOBAL read_only=ON")
            return True, None
        except Exception as e:
            # MariaDB 可能需 READ_ONLY ADMIN；MySQL 需 SUPER / SYSTEM_VARIABLES_ADMIN。
            # 若 root 都失败，说明 root 口令失效或账号权限不足，必须告警暴露。
            return False, "%s: %s" % (type(e).__name__, e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def make_slave_of(host, port, user, password, master_host, master_port,
                  repl_user, repl_pass, root_user="root", root_pass="",
                  repl_mode=None, log_file=None, log_pos=None):
    from .connection import (connect_robust)
    from .local_state import (_addr_eq, _local_state_self_master, set_local_master_flag_ex)
    """把本节点改造为 master_host 的从库（GTID 自动定位）。

    重指向前先 RESET SLAVE ALL 清掉残留复制通道，再以 MASTER_AUTO_POSITION=1
    重新挂载到新主库。GTID 自动定位会跳过本节点「已经执行过」的事务，
    因此    【严禁 RESET MASTER】：一旦清空本机 GTID 执行集合，新主库会把它
    曾发过的同名事务（如 CREATE DATABASE）当作新事务重放，从而触发
    「database exists / duplicate entry」等 1007/1062 错误，导致
    Slave_SQL_Running=No。依赖 GTID 自动定位即可干净重入，无需 RESET MASTER。

    唯一例外：若本节点「曾为主库」（其 gtid_binlog_pos 非空，含自有 GTID 身份），
    重指向其他主库时该自有 GTID 与新主库同 domain-server 序列在 gtid_strict_mode
    下必然冲突（out-of-order）。此情形必须 RESET MASTER 清空自有 binlog，
    再按已应用集/主库位点重设 gtid_slave_pos——否则干净从库无法建立（见下方 _own_gt 分支）。

    返回 (ok, msg)。
    """
    # ---- 凭证解密失败守卫（必须在连库前拦截）----
    # crypto.decrypt() 在密钥不匹配时会【原样返回密文】（刻意设计：绝不返回空串，
    # 否则空口令会在下次写库时被永久落库、覆盖真实凭证）。但密文一旦被当明文口令
    # 拼进 CHANGE MASTER，MySQL/MariaDB 会报：
    #   1470 - String 'ENC1:...' is too long for MASTER_PASSWORD (should be no longer than 96)
    # 该报错完全看不出「密钥不匹配」这一真实根因，且会把密文写进复制配置，
    # 表现为「复制永久起不来、源库不可达」，极难定位。这里显式拦截并给出可操作结论。
    if isinstance(repl_pass, str) and repl_pass.startswith("ENC1:"):
        return False, (
            f"{host}:{port} 复制口令仍是 ENC1 密文（解密失败），已中止 CHANGE MASTER 以避免"
            f"把密文写进复制配置。根因：凭证密钥与加密时不一致 —— SECRET_KEY / CRED_KEY 变动，"
            f"或 data/secret_key 丢失被重新生成。处理二选一：① 恢复原 SECRET_KEY 后重启管理器"
            f"（凭证自动恢复）；② 在「集群设置」重新录入复制口令，使其按当前密钥重新加密。")
    if isinstance(password, str) and password.startswith("ENC1:"):
        # 不阻断：ha_monitor 失败时 connect_robust 还能回退 root，贸然中断会扩大故障面。
        print(f"[cred] 警告：{host}:{port} 的 ha_monitor 口令仍是 ENC1 密文（解密失败），"
              f"当前连接将回退 root。请检查凭证密钥是否与加密时一致。")
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        flavour = detect_flavour(cur)
        # file/pos 接管路径（v2.4.1）：非 GTID 存量老集群切主/回挂用传统位点。
        # 此时跳过 GTID 专属检查（gtid_mode / 不一致检测），仅按给定位点挂载。
        _file_pos = bool(repl_mode == "file_pos" or (log_file and log_pos is not None))

        # ---- VIP 守卫（必须先于任何复制改动执行）----
        # ha_local.state 是 keepalived 检查脚本判定 VIP 归属的**唯一**依据。若本节点该表
        # 记录的 current_master 指向自己，说明 VIP 正绑在本机、业务正在经 VIP 写它。
        # 此时若继续把它挂成从库并 SET GLOBAL read_only=1，业务写入会立刻报
        #   1290 - The MariaDB server is running with the --read-only=ON option
        # 这正是「管理器视角的 current_master」与「keepalived 视角」分裂的典型后果：
        # 例如 ha_local.state 写入失败（缺 BINLOG ADMIN / ha_local 库权限 → 1227/1142），
        # 节点便一直自认主、VIP 不释放，而管理器已选了别的主，于是把它当从库压成只读。
        # 分裂时保持其可写并告警，远好于静默压只读导致业务整体不可写。
        _self_m, _rec = _local_state_self_master(cur, host, port)
        _target = "%s:%s" % (master_host, int(master_port))
        if _self_m and not _addr_eq(_target, host, port):
            # ---- 先自愈，再决定是否拦截（F16，2026-09-16）----
            # 本节点的 ha_local.state **只有管理器会写**。既然管理器此刻已认定当前主是
            # _target（另一节点），那么本机这份「我是主」的记录就是**过期错误数据**。
            # 旧实现只做「拒绝降级」，形成死锁：
            #   状态推送失败 → 本机记录过期 → 守卫拒绝降级 → 记录永远得不到纠正；
            # 后果有两层，且互相放大：
            #   ① 回归的旧主既不重入集群、又保留自己的 binlog 继续接受写入 → 无人察觉的双写不一致；
            #   ② 真主（_target）在 keepalived 那侧也可能因同类过期记录而拒绝持有 VIP
            #      → 两节点同时放弃 → VIP 全网消失（实测现象）。
            # 故此处就地把本机 current_master 纠正为 _target，再复读确认；纠正成功即放行。
            try:
                cur.execute("SELECT @@GLOBAL.read_only")
                _ro = cur.fetchone()
                _v = "" if _ro is None else str(_ro[0]).strip().upper()
                _writable = _v in ("0", "OFF", "")
            except Exception:
                _writable = True
            if not _writable:
                # 已被写入隔离置为只读（read_only=ON）：改造为从库不会让情况更糟，
                # 反而能让它重新入集群并恢复复制，故放行（保持原语义）。
                print(f"[vip-guard] {host}:{port} 自认主(ha_local={_rec!r})但已为只读，"
                      f"放行重挂载为 {_target} 的从库以恢复复制。")
                _self_m = False
            else:
                # ⚠ 就地纠正复用 set_local_master_flag_ex（2026-09-23 修复）：
                #   它内部会先自检 `SET SESSION sql_log_bin=0`，ha_monitor 缺 BINLOG ADMIN
                #   时自动回退 root 连接（root 恒为 ALL PRIVILEGES）—— 与状态推送器行为对齐。
                #   此前这里只用当前连接（ha_monitor）手写 REPLACE，而 ha_monitor 按
                #   _ensure_local_state_grant 仅授 ha_local.* 的 SELECT/INSERT/UPDATE/
                #   DELETE/CREATE，**没有 BINLOG ADMIN** ⇒ MariaDB >= 10.5.2 上
                #   `SET SESSION sql_log_bin=0` 必被 1227 拒绝 ⇒ 自愈必然失败（已实测）。
                #   后果：守卫永久拒绝降级，回归的旧主既不入集群又保持可写，且 keepalived
                #   按其过期记录仍把 VIP 绑在它身上 ⇒ 业务写入造成无人察觉的双写不一致；只能等推送器
                #   最长 PUSH_MAX_INTERVAL=300s 才解开，那时不一致已成事实。
                _ok_fix, _ferr = set_local_master_flag_ex(
                    host, port, user, password, _target, root_user, root_pass)
                _fixed = bool(_ok_fix)
                if _fixed:
                    print(f"[vip-guard] {host}:{port} 本地 current_master 已由 {_rec!r} "
                          f"就地纠正为 {_target}（自愈），继续挂载为从库。")
                    _FENCE_FIRST_SEEN.pop("%s:%s" % (host, port), None)
                    _self_m = False
                else:
                    print(f"[vip-guard] {host}:{port} 就地纠正本地 current_master 失败：{_ferr}")
                    # ---- 宽限期后 写入隔离（2026-09-23 新增）----
                    # 就地纠正失败 => 节点处于「自认主但非当前主、且无法自愈」的分裂态，
                    # 保持可写会继续产生无人察觉的双写不一致。策略：首次发现仅高优告警（宽限期吸收抖动），
                    # 超宽限仍未自愈则 read_only=ON 封写（管不住就别让它写）。
                    _fk = "%s:%s" % (host, port)
                    _now = _time.time()
                    _FENCE_FIRST_SEEN.setdefault(_fk, _now)
                    _fenced, _ferr_fence = False, ""
                    if _now - _FENCE_FIRST_SEEN[_fk] >= _VIPGUARD_FENCE_GRACE:
                        _fenced, _ferr_fence = _fence_readonly(
                            host, port, user, password, root_user, root_pass)
                        if not _fenced:
                            print(f"[vip-guard] {_fk} 写入隔离失败：{_ferr_fence}")
        if _self_m and not _addr_eq(_target, host, port):
            msg = (f"[vip-guard] 拒绝把 {host}:{port} 改造为从库：其本机 ha_local.state 仍记录 "
                   f"current_master={_rec!r}（keepalived 据此把 VIP 绑在该节点），与本次要挂载的"
                   f"主库 {_target} 不一致，且就地纠正该记录也未成功。已跳过本次重指向并保留其"
                   f"可写状态，避免业务经 VIP 写入报 1290。"
                   f"【就地纠正的真实错误】{_ferr or '（未提供）'}。"
                   f"请检查：① 该节点 ha_local 库权限；② ha_monitor 是否缺 BINLOG ADMIN"
                   f"（MariaDB >= 10.5.2 执行 SET SESSION sql_log_bin 必需）；"
                   f"③ 管理端记录的该节点 root 口令是否有效（缺 BINLOG ADMIN 时靠它回退）；"
                   f"④ 到该节点的网络连通性。"
                   f"{(chr(91) + '已触发写入隔离：已将该节点 read_only=ON 封写，避免继续无人察觉的双写不一致' + chr(93)) if _fenced else '[宽限期内：尚未封写，将在超限后自动写入隔离]'}"
                   f"{(chr(32) + '写入隔离失败：' + _ferr_fence) if (_fenced is False and _ferr_fence) else ''}。")
            print(msg)
            try:
                from app.alerts import send_alert   # 局部导入，避免模块级循环依赖
                send_alert("VIP 与主库视角分裂", msg)
            except Exception:
                pass
            return False, msg
        # MariaDB 与 MySQL 双路径完全独立：
        #  - MySQL 8.0：用 MASTER_AUTO_POSITION=1，要求 gtid_mode=ON；匿名事务用
        #    ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=LOCAL 根治 1236。
        #  - MariaDB：用 MASTER_USE_GTID=slave_pos（基于 gtid_domain_id 的序列 GTID，
        #    自动定位、天然无匿名事务 1236 问题），**不检查 gtid_mode**（MariaDB 无此变量）。
        syntax = "new" if flavour == F_MYSQL8 else "old"
        if flavour == F_MARIADB:
            # MariaDB：无需 gtid_mode 检查，GTID 由 my.cnf 的 gtid_domain_id 保证
            pass
        else:
            # GTID 自动定位（MASTER_AUTO_POSITION=1）要求本节点 gtid_mode=ON；
            # 若节点未开启 GTID，CHANGE REPLICATION SOURCE 会直接失败并每周期刷屏。
            try:
                cur.execute("SELECT @@gtid_mode")
                row = cur.fetchone()
                gm = row[0] if row else None
            except Exception:
                gm = None
            if not _file_pos and gm is not None and gm != "ON":
                return False, (f"本节点 gtid_mode={gm}（应为 ON），无法使用 GTID 自动定位。"
                               f"请重新运行 Install-ha-mariadb-Node.sh 开启 GTID 后重入集群。")
        # ---- GTID 不一致感知（升版重启/历史切换后从库执行集可能超前或偏离主库）----
        # 正常重指向：RESET SLAVE ALL + MASTER_USE_GTID=slave_pos 即可（GTID 自动定位跳过已执行事务）。
        # 但若从库「自有 binlog(gtid_binlog_pos) 或 已应用集(gtid_slave_pos)」不是主库
        # gtid_binlog_pos 的子集（如本例 100 曾为主、自有 binlog 含 3-3-17737，主库却发 3-3-1），
        # slave_pos 自动定位失败并触发「out-of-order sequence」(gtid_strict_mode)，复制卡死。
        # 此时 RESET MASTER 清空从库自有 binlog 解除乱序冲突；gtid_slave_pos 默认保留（让复制从
        # 已应用位点续传、重放主库其余 domain 最终一致）。仅当 gtid_slave_pos 本身也非子集
        # （极少见：从库应用过主库没有的事务）才把 gtid_slave_pos 对齐到主库当前位置跳过重放。
        try:
            # MariaDB 不支持 GTID_SUBSET / GTID_SUBTRACT（12.3.3 实测报
            # 1305 FUNCTION GTID_SUBSET does not exist）。下面这套不一致检测一执行到
            # GTID_SUBSET 就抛异常、被 except 静默吞掉 ⇒ 在 MariaDB 上**从未真正生效**。
            # 但「gtid_slave_pos 为空」时不会调用 GTID_SUBSET，反而会走到 RESET MASTER +
            # 设置起点的分支 ⇒ 原主库（100）被跳过历史、数据永久不一致。
            # 2.2.20 的实际效果恒为「不做任何特殊处理」的最简路径，这正是它稳定的原因。
            # 与其依赖「抛异常被吞」这种偶然行为，不如显式跳过：MariaDB 的 GTID 自动定位
            # (MASTER_USE_GTID=slave_pos) 本身即可正确定位位点，无需这套 MySQL 语义逻辑。
            if flavour == F_MARIADB or _file_pos:
                raise RuntimeError("MariaDB 不支持 GTID_SUBSET / file_pos 路径，显式跳过不一致检测")
            cur.execute("SELECT @@gtid_slave_pos, @@gtid_binlog_pos")
            _sr = cur.fetchone() or [None, None]
            _slave_gt = _sr[0] or ""
            _own_gt = _sr[1] or ""
            _mconn = connect_robust(master_host, int(master_port), user, password, root_user, root_pass)
            if _mconn is not None:
                _mcur = _mconn.cursor()
                _mcur.execute("SELECT @@gtid_binlog_pos")
                _master_gt = (_mcur.fetchone() or [None])[0] or ""
                _mconn.close()
                if _master_gt and (_slave_gt or _own_gt):
                    _pos_ok = False
                    if _slave_gt:
                        cur.execute("SELECT GTID_SUBSET(%s, %s)", (_slave_gt, _master_gt))
                        _pos_ok = bool((cur.fetchone() or [0])[0])
                    # 从库「自有 binlog GTID」(gtid_binlog_pos) 非空 = 它曾为源（曾是主库）。
                    # 重指向其他主库时，其自有 GTID 与该主库同 domain-server 序列在
                    # gtid_strict_mode 下必然冲突（out-of-order）；而 GTID_SUBSET(自有,主库)
                    # 常判为「子集」会漏掉此场景（同序列、不同源）。故「曾为源」即清空自有
                    # binlog，再按已应用集/主库位点重设 gtid_slave_pos，彻底解除乱序冲突。
                    if _own_gt:
                        print("[gtid] %s:%s 曾为源(自有binlog GTID=%s)，RESET MASTER 解除乱序冲突" % (host, port, _own_gt))
                        try:
                            cur.execute(stop_replication_sql(flavour))
                            cur.execute(reset_replica_sql(flavour))
                            cur.execute("RESET MASTER")
                            # 【回归 2.2.20 行为 2026-09-24】复制起点的设定：
                            # 2.4.x 曾先后改为「对齐主库当前位点」和「用自有 GTID 作起点」，两者在
                            # 已应用集(gtid_slave_pos)为空时都会**跳过本节点缺失的事务** ⇒ 复制
                            # IO/SQL 全 Yes、延迟 0，数据却永久不一致（100 复用旧 data 始终不一致
                            # 的根因）。2.2.20 的做法更简单也更可靠：
                            #   已应用集为空 ⇒ **不设置**起点，保持为空 ⇒ 从库从头完整重放主库
                            #   binlog，数据必然完整。重放不会因建账号冲突，因为安装脚本用的都是
                            #   CREATE USER IF NOT EXISTS / CREATE DATABASE IF NOT EXISTS。
                            # 仅当已应用集非空且确实不一致时，才对齐主库当前位点以避免 1236。
                            # 若主库早期 binlog 已清理，重放会报 1236，由上层自动全量克隆兜底。
                            if _slave_gt and _master_gt:
                                cur.execute("SELECT GTID_SUBSET(%s, %s)", (_slave_gt, _master_gt))
                                if bool((cur.fetchone() or [0])[0]):
                                    # 已应用集为主库子集：保留以免从头重放报 1062
                                    cur.execute("SET GLOBAL gtid_slave_pos=%s", (_slave_gt,))
                                else:
                                    # 已应用集也不一致：对齐到主库当前位置，跳过重放避免 1236（2.2.20 行为）
                                    cur.execute("SET GLOBAL gtid_slave_pos=%s", (_master_gt,))
                                    print("[gtid] %s:%s 已应用集不一致，gtid_slave_pos 已对齐主库当前 %s" % (host, port, _master_gt))
                            elif _slave_gt:
                                cur.execute("SET GLOBAL gtid_slave_pos=%s", (_slave_gt,))
                            elif _master_gt:
                                # 已应用集为空（原主库带旧 data 重入 / 全新安装）：
                                # 保持 gtid_slave_pos 为空 ⇒ 从头完整重放主库 binlog，保证数据完整。
                                print("[gtid] %s:%s 已应用集为空：保持 gtid_slave_pos 为空，"
                                      "从头完整重放主库 binlog（2.2.20 行为）" % (host, port))
                        except Exception as e2:
                            print("[gtid] 不一致重建失败(可忽略，继续常规重链): %s" % e2)
                    elif not _pos_ok:
                        # 纯净从库（无自有 binlog GTID）：仅已应用集非主库子集才对齐（原有逻辑）
                        print("[gtid] %s:%s 本地 GTID 非主库子集(不一致)，RESET MASTER 解除冲突后重链" % (host, port))
                        try:
                            cur.execute(stop_replication_sql(flavour))
                            cur.execute(reset_replica_sql(flavour))
                            cur.execute("RESET MASTER")
                            if _slave_gt and not _pos_ok:
                                # 已应用集也不一致：对齐到主库当前位置，跳过重放避免 1236
                                cur.execute("SET GLOBAL gtid_slave_pos=%s", (_master_gt,))
                                print("[gtid] %s:%s 已应用集不一致，gtid_slave_pos 已对齐主库当前 %s" % (host, port, _master_gt))
                        except Exception as e2:
                            print("[gtid] 不一致重建失败(可忽略，继续常规重链): %s" % e2)
        except Exception as e:
            print("[gtid] 不一致检测跳过(可忽略): %s" % e)

        try:
            cur.execute(stop_replication_sql(flavour))
            cur.execute(reset_replica_sql(flavour))
        except Exception:
            pass
        # 注意：这里不再 RESET MASTER，避免 GTID 执行集合被清空后
        # 旧事务被新主库重放而报「已存在」类错误。
        # 先带 PUBLIC_KEY 选项（MySQL 8.0 caching_sha2 / MariaDB 非 TLS 复制的官方修复）；
        # 若服务端不识别该选项则去掉重试。
        # assign_anon_gtids 仅 MySQL 8.0 生效（根治匿名事务 1236）；MariaDB 无需（走 slave_pos）。
        try:
            cur.execute(
                change_replication_sql(flavour, master_host, int(master_port),
                                       repl_user, repl_pass, public_key=True,
                                       assign_anon_gtids=(flavour == F_MYSQL8),
                                       log_file=log_file, log_pos=log_pos),
            )
        except Exception as e_pub:
            pub_err = str(e_pub)
            try:
                cur.execute(
                    change_replication_sql(flavour, master_host, int(master_port),
                                           repl_user, repl_pass, public_key=False,
                                           assign_anon_gtids=(flavour == F_MYSQL8)),
                )
            except Exception as e_no_pub:
                try:
                    cur.execute(
                        change_replication_sql(flavour, master_host, int(master_port),
                                               repl_user, repl_pass, public_key=False,
                                               assign_anon_gtids=False),
                    )
                except Exception as e2:
                    raise Exception(
                        f"CHANGE REPLICATION SOURCE 失败(pub:{pub_err}; nopub:{e_no_pub}; "
                        f"compat:{e2})")
        # 双保险：ha_local 是节点本地状态表，不参与复制（写时已 SET sql_log_bin=0）。
        # 再从库侧加复制过滤，确保即便有遗漏也不会因 ha_local 事件 1062 卡死复制。
        # 仅 MySQL 支持 CHANGE REPLICATION FILTER；MariaDB 无此语句（返回 None），
        # 直接跳过——否则每次重链都刷一条 1064 语法错误日志。
        _flt_sql = change_replication_filter_sql(["ha_local"], flavour=flavour)
        if _flt_sql:
            try:
                cur.execute(_flt_sql)
            except Exception as e_flt:
                print(f"[repl] 设置 REPLICATE_IGNORE_DB=ha_local 失败(可忽略): {e_flt}")
        cur.execute(start_replication_sql(flavour))
        # 从库设为只读，避免双写脑裂：仅当前主库可写。
        try:
            cur.execute("SET GLOBAL read_only=1")
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




def fix_node_mysql_params(host, port, root_user="root", root_pass="",
                          ha_user=None, ha_pass=None, target_server_id=None):
    from .connection import (connect, connect_robust)
    """自愈单节点复制关键参数（仅 SET PERSIST，不重启、不改 my.cnf）。

    解决的问题：裸机 MySQL 的 my.cnf 片段常被面板/重启覆盖，导致 gtid_mode 反复回退为
    OFF、server_id 退回默认值 1（多节点重复），每次重装都要手动改配置。

    SET PERSIST 会把参数写入 mysqld-auto.cnf，MySQL 启动后（在 my.cnf 之后）自动加载，
    因此【重启依然生效】，且会覆盖被面板改写的 my.cnf 片段，从根上消除反复回退。

    仅处理可在线修改的动态参数：
    - enforce_gtid_consistency=ON（须先于 gtid_mode=ON）
    - gtid_mode 经 OFF_PERMISSIVE -> ON_PERMISSIVE -> ON
    - server_id=<唯一值>

    log_bin=OFF 无法在线开启（需改 my.cnf + 重启），若检测到则只告警不处理。

    返回 (ok, report_dict)。report_dict: {changes:[...], warns:[...], before:{...}, error:...}
    """
    # 自愈是特权操作（SET PERSIST server_id/gtid_mode），需 SYSTEM_VARIABLES_ADMIN/SUPER。
    # 优先用 ha_monitor（节点模板已授予该特权，主库与副本均具备）；仅在 ha_monitor 不可用
    # （账号缺失/漂移）时回退 root（容器 root 密码固定，不随 HA_PASS 漂移）。
    conn = None
    if ha_pass:
        try:
            conn = connect(host, port, ha_user or "root", ha_pass, timeout=5)
        except Exception:
            conn = None
    if conn is None:
        conn = connect_robust(host, port, ha_user or "root", ha_pass or "",
                              root_user, root_pass)
    if conn is None:
        return False, {"error": f"无法以 root/ha_monitor 连入 {host}:{port}"
                              f"（请先确认已创建 root@'<管理器IP>' 且密码正确）"}
    cur = conn.cursor()
    report = {"changes": [], "warns": [], "before": {}}
    try:
        # MariaDB 与 MySQL 双路径完全独立：
        #  - MariaDB 没有 gtid_mode 变量（只有 gtid_domain_id），@@gtid_mode 会直接报错；
        #    故 MariaDB 跳过 gtid_mode/enforce_gtid_consistency 整段，仅校验 server_id 与 log_bin。
        #  - MySQL 才需要 gtid_mode 三阶段过渡 + enforce_gtid_consistency。
        flavour = detect_flavour(cur)
        if flavour == F_MARIADB:
            report["before"] = {"flavour": "mariadb"}
            try:
                cur.execute("SELECT @@server_id,@@log_bin")
                sid, lb = cur.fetchone()
                report["before"]["server_id"] = sid
                report["before"]["log_bin"] = lb
            except Exception as e:
                report["warns"].append(f"读取 server_id/log_bin 失败: {e}")
            # server_id 唯一化（MariaDB 同样靠 server_id + gtid_domain_id 区分 GTID 源）
            if target_server_id and int(sid or 0) != int(target_server_id):
                try:
                    cur.execute(f"SET GLOBAL server_id={int(target_server_id)}")
                    report["changes"].append(f"server_id={target_server_id}")
                except Exception as e:
                    report["warns"].append(f"server_id 设置失败: {e}")
            # log_bin 无法在线开启，仅告警
            if int(lb or 0) != 1:
                report["warns"].append(
                    "log_bin=OFF：复制需要 binlog，需重启 MariaDB（裸机请重跑 Install 脚本或 "
                    "在 my.cnf 增加 log_bin 后重启）")
            conn.commit()
            return True, report
        # ---- 以下为 MySQL 路径 ----
        cur.execute("SELECT @@gtid_mode,@@enforce_gtid_consistency,@@server_id,@@log_bin")
        gm, egc, sid, lb = cur.fetchone()
        report["before"] = {"gtid_mode": gm, "server_id": sid, "log_bin": lb}
        # enforce_gtid_consistency 必须先于 gtid_mode=ON
        if str(egc).upper() != "ON":
            try:
                cur.execute("SET PERSIST enforce_gtid_consistency=ON")
                report["changes"].append("enforce_gtid_consistency=ON")
            except Exception as e:
                report["warns"].append(f"enforce_gtid_consistency 设置失败: {e}")
        # gtid_mode 三阶段过渡（不可一步到 ON）
        if str(gm).upper() != "ON":
            ok_gtid = True
            for stp in ("OFF_PERMISSIVE", "ON_PERMISSIVE", "ON"):
                try:
                    cur.execute(f"SET PERSIST gtid_mode={stp}")
                except Exception as e:
                    ok_gtid = False
                    report["warns"].append(f"gtid_mode={stp} 设置失败: {e}")
                    break
            if ok_gtid:
                report["changes"].append("gtid_mode=ON")
                # 轮转 binlog：gtid_mode 刚从 OFF/OFF_PERMISSIVE 转为 ON，当前 binlog 可能
                # 仍含匿名事务（旧主库切换时会导致从库报 1236「Cannot replicate anonymous
                # transaction」）。轮转后新 binlog 仅含 GTID 事务，从根上避免该错误。
                try:
                    cur.execute("FLUSH BINARY LOGS")
                except Exception as e:
                    report["warns"].append(f"FLUSH BINARY LOGS 失败: {e}")
        # server_id 唯一化
        if target_server_id and int(sid) != int(target_server_id):
            try:
                cur.execute(f"SET PERSIST server_id={int(target_server_id)}")
                report["changes"].append(f"server_id={target_server_id}")
            except Exception as e:
                report["warns"].append(f"server_id 设置失败: {e}")
        # log_bin 无法在线开启，仅告警。
        # 注意：@@log_bin 返回整型 0/1（非 'ON'/'OFF' 字符串），故按真值判断，
        # 避免把正常的 log_bin=1 误报成 log_bin=OFF。
        if int(lb or 0) != 1:
            report["warns"].append(
                "log_bin=OFF：复制需要 binlog，SET PERSIST 无法在线开启；"
                "需在 my.cnf 增加 log_bin 后重启 MySQL（裸机请跑 Install 脚本或手动追加片段后重启）")
        conn.commit()
        return True, report
    except Exception as e:
        return False, {"error": f"{type(e).__name__}: {e}"}
    finally:
        try:
            conn.close()
        except Exception:
            pass



def is_stale_master(status):
    """判断一个节点是否为「残留的独立主库」（非当前主、但自己是主且无复制通道）。

    用于 ensure_topology 决定是否需要对旧主上线做 RESET MASTER 干净重入。
    """
    if not status:
        return False
    return status.get("mode") == "master" and not status.get("master_host")



def stop_slave(host, port, user, password, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(stop_replication_sql(syntax))
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def start_slave(host, port, user, password, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(start_replication_sql(syntax))
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass



def reset_slave(host, port, user, password, root_user="root", root_pass=""):
    from .connection import (connect_robust)
    conn = connect_robust(host, port, user, password, root_user, root_pass)
    if conn is None:
        return False, "无法连接节点(ha_monitor/root均失败)"
    try:
        cur = conn.cursor()
        syntax = detect_syntax(cur)
        cur.execute(stop_replication_sql(syntax))
        cur.execute(reset_replica_sql(syntax))
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


