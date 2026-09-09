"""管理库连接与表结构初始化。

注意：每次 get_conn() 返回「新连接」，避免多线程（监控线程 + Web 线程池）共享
同一 PyMySQL 连接导致协议错乱。调用方负责在使用后关闭连接。
"""
import pymysql
from pymysql.cursors import DictCursor
from .config import MGMT, ROLE_ADMIN, ROLE_VIEWER
from . import crypto as _crypto


def _create():
    return pymysql.connect(
        host=MGMT["host"], port=MGMT["port"], user=MGMT["user"],
        password=MGMT["password"], database=MGMT["db"], charset="utf8mb4",
        cursorclass=DictCursor, connect_timeout=5,
    )


def get_conn():
    """返回一个新的管理库连接。管理库不可用时抛异常，由调用方处理降级。"""
    return _create()


SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS hosts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        host VARCHAR(64) NOT NULL,
        port INT NOT NULL,
        role VARCHAR(16) DEFAULT 'unknown',
        server_id BIGINT,
        status VARCHAR(8) DEFAULT 'unknown',
        is_current_master TINYINT(1) DEFAULT 0,
        initial_role VARCHAR(8) DEFAULT 'slave',
        err TEXT,
        repl_delay INT DEFAULT NULL,
        slave_io VARCHAR(16),
        slave_sql VARCHAR(16),
        last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_host (host, port)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS current_master (
        id TINYINT PRIMARY KEY,
        host VARCHAR(64) NOT NULL,
        port INT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS switch_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        from_host VARCHAR(64),
        from_port INT,
        to_host VARCHAR(64),
        to_port INT,
        reason VARCHAR(255),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS settings (
        `key` VARCHAR(64) PRIMARY KEY,
        value TEXT
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(64) UNIQUE,
        password_hash VARCHAR(128),
        role VARCHAR(16) NOT NULL DEFAULT 'admin',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS nodes (
        ip VARCHAR(64) PRIMARY KEY,
        port INT NOT NULL,
        root_user VARCHAR(64),
        root_pass VARCHAR(255) DEFAULT '',
        server_id INT,
        compose_path VARCHAR(255),
        cluster_id INT DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS clusters (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(64) NOT NULL,
        ha_user VARCHAR(64),
        ha_pass VARCHAR(255) DEFAULT '',
        repl_user VARCHAR(64),
        repl_pass VARCHAR(255) DEFAULT '',
        app_user VARCHAR(64),
        app_pass VARCHAR(255) DEFAULT '',
        vip VARCHAR(45),
        vip_prefix VARCHAR(4) DEFAULT '24',
        interface VARCHAR(32),
        vrid VARCHAR(8),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS alert_emails (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        is_default TINYINT(1) DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_email (email)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS alert_email_map (
        email_id INT NOT NULL,
        cluster_id INT NOT NULL,
        PRIMARY KEY (email_id, cluster_id)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：指标历史快照（monitor 周期落库，供趋势图）----
    """
    CREATE TABLE IF NOT EXISTS metric_history (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        seconds_behind INT, slave_io VARCHAR(16), slave_sql VARCHAR(16),
        ro TINYINT(1), connections INT, repl_delay INT,
        ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_host (cluster_id, host, port, ts)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：慢日志采集（节点 mysql.slow_log 采集后 TRUNCATE，保持节点侧极小）----
    """
    CREATE TABLE IF NOT EXISTS slow_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        query_time_ms INT, lock_time_ms INT, rows_sent INT, rows_examined INT,
        db VARCHAR(64), sql_text MEDIUMTEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_host (cluster_id, host, port, ts)
    ) ENGINE=InnoDB
    """,
    # ---- v2.2：慢日志增量采集游标（每个节点已采集到的最大 start_time）----
    # 有了水位后「清空节点侧 mysql.slow_log」不再必须成功，清空失败也不会重复入库。
    """
    CREATE TABLE IF NOT EXISTS slow_log_cursor (
        cluster_id INT NOT NULL,
        host VARCHAR(64) NOT NULL,
        port INT NOT NULL,
        last_start_time DATETIME(6) NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (cluster_id, host, port)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：跨节点配置一致性巡检结果（定时/手动跑）----
    """
    CREATE TABLE IF NOT EXISTS config_drift (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        var_name VARCHAR(64), expected VARCHAR(255), actual VARCHAR(255),
        status VARCHAR(8),
        ts DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_cid (cluster_id, ts)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：操作审计日志 ----
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        actor VARCHAR(64), action VARCHAR(64), target VARCHAR(128),
        detail TEXT, cluster_id INT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_ts (created_at)
    ) ENGINE=InnoDB
    """,
    # ---- v2.1：自愈/告警状态记录（供告警「已自愈/未恢复」状态标记）----
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        cluster_id INT, host VARCHAR(64), port INT,
        alert_type VARCHAR(32),
        status VARCHAR(16),
        message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_host (cluster_id, host, port)
    ) ENGINE=InnoDB
    """,
]


def init_schema():
    conn = pymysql.connect(
        host=MGMT["host"], port=MGMT["port"], user=MGMT["user"],
        password=MGMT["password"], charset="utf8mb4", cursorclass=DictCursor,
    )
    try:
        with conn.cursor() as c:
            try:
                # 外部管理库账号常无 CREATE DATABASE 权限；库已存在则忽略
                c.execute(f"CREATE DATABASE IF NOT EXISTS `{MGMT['db']}`")
            except Exception:
                pass
        try:
            conn.select_db(MGMT["db"])
        except Exception:
            raise SystemExit(
                f"[init_db] 管理库 `{MGMT['db']}` 不存在且当前账号无法创建，"
                f"请先手动建库或授予建库权限。"
            )
        with conn.cursor() as c:
            for st in SCHEMA_SQL:
                c.execute(st)
            # v2.3.1 密钥镜像表：把 data/cred.key 的副本存入管理库。
            # 价值：①「只备份管理库」即可完整恢复（无需另行保管 key 文件）；
            # ② 文件被误删时启动自动从库恢复，小白零操作即可复原。
            try:
                c.execute("""CREATE TABLE IF NOT EXISTS secrets (
                    k VARCHAR(64) PRIMARY KEY,
                    v MEDIUMTEXT NOT NULL
                )""")
            except Exception:
                pass
            # 兼容旧版 schema：将 port 为 NULL 的历史行补默认，并确保 NOT NULL
            try:
                c.execute("UPDATE hosts SET port=3306 WHERE port IS NULL")
                c.execute("ALTER TABLE hosts MODIFY port INT NOT NULL DEFAULT 3306")
            except Exception:
                pass
            # 兼容目录调整：将已生成节点的 compose_path 刷新为新基路径
            # （generated_nodes 移到项目根目录、仓库改名为 custos-ha 后旧路径已失效）。
            # 仅更新「当前值与目标值不一致」的行：既保留迁移能力，又避免每次启动
            # 都对所有节点跑一遍 UPDATE（无谓写库 + 大集群启动变慢）。
            try:
                from .config import NODES_HOST_BASE
                for r in query("SELECT ip, compose_path FROM nodes"):
                    want = f"{NODES_HOST_BASE}/{r['ip']}"
                    if (r.get("compose_path") or "") != want:
                        execute(
                            "UPDATE nodes SET compose_path=%s WHERE ip=%s",
                            (want, r["ip"]),
                        )
            except Exception:
                pass
            # 空闲优化：为历史表的 ts 列补单列索引。
            # metric_history / config_drift 的保留期清理按 ts 删除，而原索引
            # (cluster_id,host,port,ts) 与 (cluster_id,ts) 的最左前缀都不是 ts，
            # 导致每次 DELETE 都全表扫描，把整表页读进 buffer pool 产生脏页与
            # 后续 checkpoint 写盘。补上索引后清理真正走索引删除。
            for _tbl in ("metric_history", "config_drift"):
                try:
                    c.execute(f"ALTER TABLE {_tbl} ADD INDEX idx_ts (ts)")
                except Exception:
                    pass
            # 兼容旧 schema：新增 initial_role 列（记录节点生成时指定的初始主/从）
            try:
                c.execute(
                    "ALTER TABLE hosts ADD COLUMN initial_role VARCHAR(8) DEFAULT 'slave'"
                )
            except Exception:
                pass
            # 兼容旧 schema：新增 err 列（记录节点探活失败原因，便于前端展示）
            try:
                c.execute("ALTER TABLE hosts ADD COLUMN err TEXT")
            except Exception:
                pass
            # 兼容旧 schema：新增「数据交互状态」字段（v2.2.20 就地修复）
            # repl_state: offline/primary/repl_ok/repl_broken/repl_stuck/orphan
            # upstream:    从库的上游主库 host:port；主库为空
            # downstream:  主库的下游从库列表（JSON 数组字符串）；从库为空
            try:
                c.execute("ALTER TABLE hosts ADD COLUMN repl_state VARCHAR(16)")
            except Exception:
                pass
            try:
                c.execute("ALTER TABLE hosts ADD COLUMN upstream VARCHAR(64)")
            except Exception:
                pass
            try:
                c.execute("ALTER TABLE hosts ADD COLUMN downstream TEXT")
            except Exception:
                pass
            # 兼容旧 schema：alert_emails 新增 enabled（收件开关，默认开启）
            try:
                c.execute(
                    "ALTER TABLE alert_emails ADD COLUMN enabled TINYINT(1) NOT NULL DEFAULT 1"
                )
            except Exception:
                pass
            # 兼容旧 schema：nodes 表新增 root_pass（节点 MySQL root 密码，用于 ha_monitor
            # 凭证自愈与切换的 root 回退；root 密码在容器创建时固定，不随 HA_PASS 漂移）
            try:
                c.execute("ALTER TABLE nodes ADD COLUMN root_pass VARCHAR(255) DEFAULT ''")
            except Exception:
                pass
            # 兼容旧 schema：switch_history 增加 cluster_id（按集群区分切换记录）
            try:
                c.execute("ALTER TABLE switch_history ADD COLUMN cluster_id INT DEFAULT NULL")
            except Exception:
                pass
            # ---- 多集群支持：新增 clusters 表 + hosts/nodes 的 cluster_id ----
            try:
                c.execute(
                    """CREATE TABLE IF NOT EXISTS clusters (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(64) NOT NULL,
                        ha_user VARCHAR(64),
                        ha_pass VARCHAR(255) DEFAULT '',
                        repl_user VARCHAR(64),
                        repl_pass VARCHAR(255) DEFAULT '',
                        app_user VARCHAR(64),
                        app_pass VARCHAR(255) DEFAULT '',
                        vip VARCHAR(45),
                        vip_prefix VARCHAR(4) DEFAULT '24',
                        interface VARCHAR(32),
                        vrid VARCHAR(8),
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB"""
                )
            except Exception:
                pass
            # clusters 增加 vip_port（VIP 对外访问端口，业务连 VIP:VIP_PORT 访问主库）
            try:
                c.execute("ALTER TABLE clusters ADD COLUMN vip_port INT NOT NULL DEFAULT 3307")
            except Exception:
                pass
            # v2.3.0：mysql_version 存 SELECT VERSION() 原文（如 10.6.21-MariaDB-log
            # 达 19 字符），原 varchar(16) 会被截断，扩容到 48。
            try:
                c.execute("ALTER TABLE nodes MODIFY mysql_version varchar(48) DEFAULT NULL")
            except Exception:
                pass
            # ---- v2.3.3：维护模式窗口表 ----
            # scope=cluster 抑制整个集群的自动切换与状态告警；scope=node 只覆盖单个节点。
            # end_at 为 NULL 表示不限时（必须手动结束）；到期由查询时 purge 兜底。
            try:
                c.execute("""CREATE TABLE IF NOT EXISTS maintenance (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cluster_id INT NOT NULL,
                    scope VARCHAR(8) NOT NULL DEFAULT 'cluster',
                    target VARCHAR(64) DEFAULT '',
                    reason VARCHAR(255) DEFAULT '',
                    started_at DATETIME NOT NULL,
                    end_at DATETIME DEFAULT NULL,
                    created_by VARCHAR(64) DEFAULT '',
                    created_at DATETIME NOT NULL,
                    ended_at DATETIME DEFAULT NULL,
                    active TINYINT(1) NOT NULL DEFAULT 1,
                    INDEX idx_cluster_active (cluster_id, active)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            except Exception as e:
                print(f"[init_db] 创建 maintenance 表失败(可忽略): {e}")
            # ---- v2.3.0：节点「配置待生效」提示 ----
            # gen_fingerprint：最近一次生成的部署文件指纹（路径+内容），用于判断配置是否又变了
            # pending_actions：regen 后需在节点执行的动作清单（JSON，空串=无待办）
            # pending_since：待办产生时间（运维执行后点「已执行」清空）
            for _tbl, _col, _def in (
                ("nodes", "gen_fingerprint", "varchar(64) DEFAULT ''"),
                ("nodes", "pending_actions", "TEXT"),
                ("nodes", "pending_since", "DATETIME NULL"),
            ):
                try:
                    c.execute(f"ALTER TABLE {_tbl} ADD COLUMN {_col} {_def}")
                except Exception:
                    pass
            # ---- v2.1：节点/集群支持裸机（node_type）与 MySQL 大版本（mysql_version）----
            # 默认 node_type='docker' 行为完全不变；mysql_version=NULL 表示沿用旧复制语法。
            for _tbl, _col, _def in (
                ("nodes", "node_type", "varchar(16) NOT NULL DEFAULT 'docker'"),
                ("nodes", "mysql_version", "varchar(16) DEFAULT NULL"),
                ("clusters", "node_type", "varchar(16) NOT NULL DEFAULT 'docker'"),
                ("clusters", "mysql_version", "varchar(16) DEFAULT NULL"),
            ):
                try:
                    c.execute(f"ALTER TABLE {_tbl} ADD COLUMN {_col} {_def}")
                except Exception:
                    pass
            # hosts 增加 cluster_id
            try:
                c.execute("ALTER TABLE hosts ADD COLUMN cluster_id INT DEFAULT NULL")
            except Exception:
                pass
            # nodes 增加 cluster_id
            try:
                c.execute("ALTER TABLE nodes ADD COLUMN cluster_id INT DEFAULT NULL")
            except Exception:
                pass
            # hosts 增加半同步复制健康指标（monitor.get_status 采集，仪表盘展示 + 退化告警）。
            # SEMI_SYNC=0 时变量恒为 OFF，这些列保持 NULL，不影响核心探活。
            for _col, _def in (
                ("semi_sync", "varchar(8) DEFAULT NULL"),
                ("semi_clients", "int DEFAULT NULL"),
                ("semi_no_times", "int DEFAULT NULL"),
                ("semi_no_tx", "int DEFAULT NULL"),
                ("semi_yes_tx", "int DEFAULT NULL"),
            ):
                try:
                    c.execute(f"ALTER TABLE hosts ADD COLUMN {_col} {_def}")
                except Exception:
                    pass
            # alerts 增加 acked（告警中心「已读/确认」标记）
            try:
                c.execute("ALTER TABLE alerts ADD COLUMN acked TINYINT(1) NOT NULL DEFAULT 0")
            except Exception:
                pass
            # users 增加 role（角色分级：admin 管理员 / viewer 只读）
            try:
                c.execute("ALTER TABLE users ADD COLUMN role VARCHAR(16) NOT NULL DEFAULT 'admin'")
            except Exception:
                pass
            # current_master 改为按集群（cluster_id 主键），兼容旧版单列(id=1)结构。
            # 仅当 current_master 仍是旧结构（含 id 列、无 cluster_id 列）时迁移一次，
            # 避免每次重启都把当前主数据清空。
            try:
                c.execute("SHOW COLUMNS FROM current_master")
                cm_cols = [r["Field"] for r in c.fetchall()]
            except Exception:
                cm_cols = []
            if "cluster_id" not in cm_cols and "id" in cm_cols:
                try:
                    c.execute(
                        """CREATE TABLE IF NOT EXISTS current_master_new (
                            cluster_id INT PRIMARY KEY,
                            host VARCHAR(64) NOT NULL,
                            port INT NOT NULL,
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB"""
                    )
                    c.execute("SELECT id, host, port FROM current_master")
                    for r in c.fetchall():
                        c.execute(
                            "INSERT IGNORE INTO current_master_new (cluster_id, host, port) VALUES (%s,%s,%s)",
                            (1, r["host"], r["port"]),
                        )
                    c.execute("DROP TABLE IF EXISTS current_master")
                    c.execute("ALTER TABLE current_master_new RENAME TO current_master")
                except Exception as e:
                    print(f"[init_db] 迁移 current_master 失败(可忽略): {e}")
            # 首次迁移：把既有 hosts/nodes 归入默认集群(id=1)，并从旧 settings 迁移凭证
            try:
                c.execute("SELECT COUNT(*) AS n FROM clusters")
                if (c.fetchone() or {}).get("n", 0) == 0:
                    # 从 settings 读取旧凭证
                    def _s(k):
                        c.execute("SELECT value FROM settings WHERE `key`=%s", (k,))
                        row = c.fetchone()
                        return row["value"] if row else None
                    name = _s("CLUSTER_NAME") or "默认集群"
                    ha_user = _s("HA_USER") or "ha_monitor"
                    ha_pass = _s("HA_PASS")
                    repl_user = _s("REPL_USER") or "replication"
                    repl_pass = _s("REPL_PASS")
                    app_user = _s("APP_USER") or "app"
                    app_pass = _s("APP_PASS")
                    vip = _s("VIP")
                    vip_prefix = _s("VIP_PREFIX") or "24"
                    interface = _s("INTERFACE") or "eth0"
                    vrid = _s("VRID") or "51"
                    c.execute(
                        """INSERT INTO clusters
                        (id, name, ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                         vip, vip_prefix, interface, vrid)
                        VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (name, ha_user, ha_pass, repl_user, repl_pass, app_user, app_pass,
                         vip, vip_prefix, interface, vrid),
                    )
                    c.execute("UPDATE hosts SET cluster_id=1 WHERE cluster_id IS NULL")
                    c.execute("UPDATE nodes SET cluster_id=1 WHERE cluster_id IS NULL")
            except Exception as e:
                print(f"[init_db] 迁移默认集群失败(可忽略): {e}")
        # v2.1：保留期默认值（仅首次写入，已存在则不动）
        for _k, _v in (("METRIC_RETENTION_DAYS", "30"), ("SLOWLOG_RETENTION_DAYS", "7")):
            if get_setting(_k) is None:
                upsert_setting(_k, _v)
        conn.commit()
    finally:
        conn.close()
    # v2.3.1：密钥自愈 + 镜像。
    # 顺序很关键：先确保密钥就绪（文件缺失则从库恢复，都没有则生成），再把它
    # 镜像回库；只有密钥确定后，下面的明文加密迁移才能用正确的密钥加密。
    try:
        if not _crypto.key_file_exists():
            _restored = _get_secret("cred_key")
            if _restored:
                _crypto.write_key_file(_restored)
                print("[init_db] data/cred.key 缺失，已从管理库自动恢复密钥", flush=True)
        _set_secret("cred_key", _crypto.bootstrap())
    except Exception as _ke:
        print(f"[init_db] 密钥自愈/镜像失败(可忽略，下轮重试): {_ke}")
    # v2.3.1：表结构与默认值就绪后，把存量明文凭证加密（幂等，失败下一轮重试）
    try:
        migrate_encrypt_credentials()
    except Exception as _ce:
        print(f"[init_db] 凭证加密迁移失败(可忽略，下轮启动重试): {_ce}")


# v2.3.1：需要「透明解密」的凭证列。凡是 SELECT 出这些列的地方都会自动拿到明文，
# 无需任何调用方改动（全系统凭证读取都收敛在 db.query 这一个出口）。
_SECRET_COLS = ("root_pass", "ha_pass", "repl_pass", "app_pass")


def query_raw(sql, args=None):
    """与 query() 相同，但**不做凭证解密**——仅供加密迁移使用。

    迁移必须拿到库里的原始值才能判断「是否已加密」；走 query() 会被透明解密，
    结果永远判成明文、每次启动都把密文重写一遍。
    """
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute(sql, args)
            return c.fetchall()
    finally:
        conn.close()


def query(sql, args=None):
    """查询并返回 dict 列表；命中的凭证列**自动解密**（v2.3.1）。

    这是全系统凭证读取的统一出口：13 个模块、数百处调用无需任何改动即可拿到明文，
    也杜绝「某处漏解密、拿着密文去连数据库」这类难以定位的故障。
    """
    rows = query_raw(sql, args)
    if not rows:
        return rows
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k in _SECRET_COLS:
            v = r.get(k)
            if v and _crypto.is_encrypted(v):
                r[k] = _crypto.decrypt(v)
    return rows


def migrate_encrypt_credentials():
    """把存量明文凭证加密（v2.3.1）。幂等：已加密的跳过。

    必要性：升级前入库的凭证全是明文。不迁移就会长期「新写入加密、存量明文」并存，
    管理库备份与 dump 文件里依旧躺着生产库 root 口令 —— 加密形同虚设。
    """
    changed = 0
    for tbl, key_cols, pk in (
        ("clusters", ("ha_pass", "repl_pass", "app_pass"), "id"),
        ("nodes", ("root_pass",), "ip"),
    ):
        try:
            cols = ", ".join((pk,) + key_cols)
            rows = query_raw(f"SELECT {cols} FROM {tbl}")
        except Exception:
            continue
        for r in rows:
            upd, vals = [], []
            for k in key_cols:
                v = r.get(k)
                if v and not _crypto.is_encrypted(v):
                    upd.append(f"{k}=%s")
                    vals.append(_crypto.encrypt(v))
            if not upd:
                continue
            vals.append(r.get(pk))
            try:
                execute(f"UPDATE {tbl} SET {', '.join(upd)} WHERE {pk}=%s", vals)
                changed += 1
            except Exception as e:
                print(f"[init_db] 加密 {tbl}({pk}={r.get(pk)}) 失败: {e}")
    if changed:
        print(f"[init_db] 凭证加密迁移：已加密 {changed} 条存量记录", flush=True)
    return changed


def _get_secret(k):
    """读管理库密钥镜像（secrets 表）。失败返回 None。"""
    try:
        rows = query("SELECT v FROM secrets WHERE k=%s", (k,))
        return rows[0].get("v") if rows else None
    except Exception:
        return None


def _set_secret(k, v):
    """把密钥镜像写入管理库（幂等 upsert）。失败仅告警，不阻断启动。"""
    try:
        execute(
            "INSERT INTO secrets(k, v) VALUES(%s, %s) "
            "ON DUPLICATE KEY UPDATE v=%s",
            (k, v, v),
        )
        return True
    except Exception as e:
        print(f"[init_db] 密钥镜像入库失败(可忽略): {e}")
        return False


def execute(sql, args=None):
    """执行写操作并提交。

    返回本次执行的 lastrowid（自增主键）。注意：get_conn() 每次返回新连接，
    因此调用方**不能**再用另一个连接去查 LAST_INSERT_ID()（该值是连接级的，
    换连接会拿到 0）。需要新主键时请直接使用本函数的返回值。
    """
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute(sql, args)
            lastrowid = c.lastrowid
        conn.commit()
        return lastrowid
    finally:
        conn.close()


def get_user_role(username, default=None):
    """读取用户角色。

    异常/迁移未完成时**回退管理员**而非只读：宁可暂时放宽，也不能把既有管理员
    锁死在系统外（role 列缺失时若回退 viewer，所有人将无法做任何操作）。
    """
    try:
        rows = query("SELECT role FROM users WHERE username=%s", (username,))
        r = rows[0].get("role") if rows else None
        return (str(r).strip().lower() if r else (default or ROLE_ADMIN))
    except Exception:
        return default or ROLE_ADMIN


def list_users():
    try:
        return query("SELECT id, username, role, created_at FROM users ORDER BY id")
    except Exception:
        return query("SELECT id, username, created_at FROM users ORDER BY id")


def get_user(username):
    rows = query("SELECT id, username, role FROM users WHERE username=%s", (username,))
    return rows[0] if rows else None


def create_user(username, password_hash, role=ROLE_ADMIN):
    execute("INSERT INTO users (username, password_hash, role) VALUES (%s,%s,%s)",
            (username, password_hash, role or ROLE_ADMIN))


def set_user_role(username, role):
    execute("UPDATE users SET role=%s WHERE username=%s",
            (ROLE_VIEWER if role == ROLE_VIEWER else ROLE_ADMIN, username))


def delete_user(username):
    execute("DELETE FROM users WHERE username=%s", (username,))


def count_admins():
    """剩余管理员数量（用于阻止删光/降级最后一个管理员）。"""
    try:
        rows = query("SELECT COUNT(*) AS n FROM users WHERE role=%s", (ROLE_ADMIN,))
        return int(rows[0].get("n") or 0)
    except Exception:
        # role 列不存在时无法统计，按「有很多管理员」处理以免误拦截
        return 999


def upsert_setting(key, value):
    execute(
        "INSERT INTO settings (`key`, value) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE value=%s",
        (key, value, value),
    )


def get_setting(key, default=None):
    rows = query("SELECT value FROM settings WHERE `key`=%s", (key,))
    if rows:
        return rows[0]["value"]
    return default
