"""管理库连接与表结构初始化。

注意：每次 get_conn() 返回「新连接」，避免多线程（监控线程 + Web 线程池）共享
同一 PyMySQL 连接导致协议错乱。调用方负责在使用后关闭连接。
"""
import os
import re
import secrets
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
            # clusters.mysql_version 同样可能写入 SELECT VERSION() 原文（迁移 cutover 经
            # create_cluster 写入 tgt_version），原 varchar(16) 会触发 1406 Data too long；
            # 与 nodes 一并扩容到 48（写入侧再兜底截断）。
            try:
                c.execute("ALTER TABLE clusters MODIFY mysql_version varchar(48) DEFAULT NULL")
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
            # ---- v2.3.10：数据迁移向导任务表 ----
            # 既有 MariaDB 集群 → Custos 托管集群 的迁移任务状态（步骤/配置/日志）。
            # config 为加密 JSON（含源/目标凭据）；steps 为各步骤状态 JSON。
            try:
                c.execute("""CREATE TABLE IF NOT EXISTS migration_tasks (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(128) NOT NULL DEFAULT '未命名迁移',
                    channel VARCHAR(16) NOT NULL DEFAULT 'logical',
                    status VARCHAR(16) NOT NULL DEFAULT 'draft',
                    config TEXT,
                    steps TEXT,
                    log TEXT,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            except Exception as e:
                print(f"[init_db] 创建 migration_tasks 表失败(可忽略): {e}")
            # ---- 迁移向导：百分比进度与「当前阶段」文字（后台线程执行 + 前端轮询）----
            # progress = 整任务百分比(0~100)；stage = 正在做什么的人类可读短句。
            # 后台线程直接 UPDATE 这两列，前端轮询 /status 渲染进度条，避免长任务阻塞请求。
            for _tbl, _col, _def in (
                ("migration_tasks", "progress", "TINYINT UNSIGNED NOT NULL DEFAULT 0"),
                ("migration_tasks", "stage", "varchar(255) NOT NULL DEFAULT ''"),
            ):
                try:
                    c.execute(f"ALTER TABLE {_tbl} ADD COLUMN {_col} {_def}")
                except Exception:
                    pass
            # ---- v2.4.1：旧集群接管任务（与 migration_tasks 独立，互不污染）----
            # config 为加密 JSON（含旧集群主库凭据与接管参数）；steps 为各阶段状态 JSON。
            try:
                c.execute("""CREATE TABLE IF NOT EXISTS takeover_tasks (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(128) NOT NULL DEFAULT '未命名接管',
                    mode VARCHAR(8) NOT NULL DEFAULT 'A',
                    status VARCHAR(16) NOT NULL DEFAULT 'draft',
                    config MEDIUMBLOB,
                    steps MEDIUMTEXT,
                    log MEDIUMTEXT,
                    progress TINYINT UNSIGNED NOT NULL DEFAULT 0,
                    stage VARCHAR(255) NOT NULL DEFAULT '',
                    created_at DATETIME,
                    updated_at DATETIME,
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            except Exception as e:
                print(f"[init_db] 创建 takeover_tasks 表失败(可忽略): {e}")
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
            # 默认 node_type='mariadb-docker'（容器化 MariaDB）；mysql_version=NULL 表示沿用旧复制语法。
            for _tbl, _col, _def in (
                ("nodes", "node_type", "varchar(16) NOT NULL DEFAULT 'mariadb-docker'"),
                ("nodes", "mysql_version", "varchar(16) DEFAULT NULL"),
                ("clusters", "node_type", "varchar(16) NOT NULL DEFAULT 'mariadb-docker'"),
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
    # v2.3.1：密钥已由 crypto 从 SECRET_KEY 派生（或旧 cred.key / CRED_KEY 覆盖），
    # 无需镜像进库、无需单独备份。此处仅确保在明文加密迁移前密钥已就绪。
    try:
        _crypto.bootstrap()
    except Exception as _ke:
        print(f"[init_db] 凭证密钥初始化失败(可忽略，下轮重试): {_ke}")
    # v2.3.1：表结构与默认值就绪后，把存量明文凭证加密（幂等，失败下一轮重试）
    try:
        migrate_encrypt_credentials()
    except Exception as _ce:
        print(f"[init_db] 凭证加密迁移失败(可忽略，下轮启动重试): {_ce}")
    # v2.3.10：加密默认关闭 —— 把存量 ENC1 密文还原为明文，彻底解除对密钥的依赖
    try:
        migrate_decrypt_credentials()
    except Exception as _cd:
        print(f"[init_db] 凭证明文还原失败(可忽略，下轮启动重试): {_cd}")


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
    if not _crypto.enabled():
        return 0      # v2.3.10：加密关闭时不再把明文加密入库（否则与反向迁移互相抵消）
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


def migrate_decrypt_credentials(force=False):
    """v2.3.10：凭证加密**关闭**时，把仍能解开的 ENC1 凭证还原为明文（幂等、尽最大努力）。

    ``force=True``：即使加密处于开启也强制执行 —— 「重置密钥」流程必须先把它全部
    转为明文，再换新密钥重新加密，否则换密钥会让原可解的凭证变成永久死数据。


    为什么需要反向迁移：
      仅把 encrypt() 改成不加密，库里历史 ENC1 密文依然存在，其可用性仍绑在密钥上。
      一旦将来密钥丢失，这批凭证就变成永久读不出的死数据（表现为复制口令是密文、
      CHANGE MASTER 报 1470）。故在**当前密钥还有效**时主动还原，彻底解除该依赖。

    安全性：解不开的一律跳过、**绝不改写**（decrypt 失败会原样返回密文，此处据此判定），
    由 crypto.last_error() 与 make_slave_of 的 ENC1 守卫暴露给运维，不会静默损坏凭证。
    """
    if _crypto.enabled() and not force:
        return 0
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
                if not (v and _crypto.is_encrypted(v)):
                    continue
                plain = _crypto.decrypt(v)
                # 仍以 ENC1 开头 == 解密失败：原样保留，绝不覆盖为密文或空值
                if _crypto.is_encrypted(plain):
                    continue
                upd.append(f"{k}=%s")
                vals.append(plain)
            if not upd:
                continue
            vals.append(r.get(pk))
            try:
                execute(f"UPDATE {tbl} SET {', '.join(upd)} WHERE {pk}=%s", vals)
                changed += 1
            except Exception as e:
                print(f"[init_db] 还原 {tbl}({pk}={r.get(pk)}) 凭证明文失败: {e}")
    if changed:
        print(f"[init_db] 凭证加密已关闭：已将 {changed} 条凭证还原为明文存储"
              f"（此后不再依赖任何密钥）", flush=True)
    return changed


def credential_health():
    """审计全部凭证的可解密状态（Web「设置 → 凭证加密」卡片展示用）。

    这是「密钥丢失」场景下的**关键可观测性**：光告诉用户「加密已关闭」毫无意义，
    必须指出**具体哪些口令已经解不开**，因为它们无论如何都必须在「集群设置 / 节点」
    重新录入（关闭加密也无法让密文变回明文）。

    status: ``plain``=明文存储 ｜ ``ok``=密文且可解 ｜ ``broken``=密文但当前所有候选
    密钥均解不开（decrypt 会原样返回密文，据此判定）。
    """
    items = []
    for tbl, scope, key_cols, pk, name_col, cid_col in (
        ("clusters", "集群", ("ha_pass", "repl_pass", "app_pass"), "id", "name", "id"),
        ("nodes", "节点", ("root_pass",), "ip", "ip", "cluster_id"),
    ):
        try:
            cols = ", ".join(dict.fromkeys((pk, name_col, cid_col) + key_cols))
            rows = query_raw("SELECT %s FROM %s" % (cols, tbl))
        except Exception as e:
            print(f"[db] 凭证体检跳过 {tbl}: {e}")
            continue
        for r in rows:
            nm = r.get(name_col) or r.get(pk)
            for k in key_cols:
                v = r.get(k)
                if not v:
                    continue
                if not _crypto.is_encrypted(v):
                    st = "plain"
                elif _crypto.is_encrypted(_crypto.decrypt(v)):
                    st = "broken"          # 解密失败会原样返回密文 → 据此判定
                else:
                    st = "ok"
                items.append({"scope": scope, "name": str(nm), "column": k,
                              "status": st, "cluster_id": r.get(cid_col)})
    broken = [i for i in items if i["status"] == "broken"]
    return {"total": len(items), "broken": len(broken), "items": items}


def recover_creds_from_node_packages(nodes_dir=None):
    """一键从节点生成包取回明文口令并回填（依赖文件，不连节点 DB）。

    适用场景：重装 / 重建管理器导致管理库凭证密钥变动，库内 ENC1 密文解不开、复制中断。
    节点包（generated_nodes/<ip>/）里含**明文**口令 —— init SQL / fix-node-creds.sql 的
    ``... BY '<plaintext>'``（或 MariaDB 的 ``... USING PASSWORD('<plaintext>')``）、
    compose 的 ``MARIADB_ROOT_PASSWORD`` / ``MYSQL_ROOT_PASSWORD`` —— 且这些正是节点 MySQL
    **当前实际使用**的口令。直接回填即可让复制恢复，无需知道旧密钥、也无需重设节点密码。

    回填后凭证以明文存储（加密默认关闭），无需旧密钥即可被读取。

    返回 {ok, msg, applied:[...], skipped:[...]}：
      - 集群级（ha/repl/app 口令）：扫描全部节点包合并（各节点共享同一套集群口令）回填。
      - 节点级（root 口令）：逐节点取其自身包回填 nodes.root_pass。
    """
    from . import config as _cfg
    from .clusters import update_cluster   # 局部导入避免循环（clusters 依赖 db）
    nd = nodes_dir or getattr(_cfg, "NODES_DIR", "/app/generated_nodes")
    if not os.path.isdir(nd):
        return {"ok": False,
                "msg": f"未找到节点生成目录 {nd}（可能已被卸载/清理）。"
                      f"请改用「集群设置 / 节点」手动重新录入口令。",
                "applied": [], "skipped": []}

    # 解析「账号 → 口令(明文)」：覆盖 MySQL 的 `IDENTIFIED WITH ... BY 'pw'`、
    # SET PASSWORD FOR 'u'@'h' = PASSWORD('pw')，以及 MariaDB 的 `USING PASSWORD('pw')`。
    _pw_re = re.compile(
        r"SET PASSWORD FOR\s+'(?P<u1>[^']+)'@'[^']*'\s*=\s*PASSWORD\('(?P<p1>[^']*)'\)"
        r"|"
        r"'(?P<u2>[^']+)'@'[^']*'\s+IDENTIFIED\s+"
        r"(?:VIA\s+\w+\s+USING\s+PASSWORD\('(?P<p2>[^']*)'\)"
        r"|WITH\s+\w+\s+BY\s+'(?P<p3>[^']*)')"
    )

    def parse_sql(path):
        m = {}
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    for mm in _pw_re.finditer(line):
                        u = mm.group("u1") or mm.group("u2")
                        p = mm.group("p1") or mm.group("p2") or mm.group("p3")
                        if u and p:
                            m[u] = p
        except Exception:
            pass
        return m

    cluster_creds = {}                 # 集群级口令（全节点包合并）
    node_root = {}                     # ip -> root 明文口令
    for name in sorted(os.listdir(nd)):
        d = os.path.join(nd, name)
        if not os.path.isdir(d):
            continue
        local = {}
        for root, _dirs, files in os.walk(d):
            for fn in files:
                if fn.endswith(".sql"):
                    local.update(parse_sql(os.path.join(root, fn)))
                if fn in ("docker-compose.yaml", "docker-compose.yml"):
                    try:
                        with open(os.path.join(root, fn), "r",
                                  encoding="utf-8", errors="ignore") as f:
                            txt = f.read()
                        m2 = re.search(
                            r"(MARIADB|MYSQL)_ROOT_PASSWORD:\s*\"?([^\"\n]+)\"?", txt)
                        if m2:
                            local.setdefault("__root__",
                                             m2.group(2).strip().strip('"').strip("'"))
                    except Exception:
                        pass
        cluster_creds.update(local)
        rp = local.get("__root__")
        if not rp:                     # 容器包无 compose root 时回退到 sql 里的 root 口令
            rp = local.get("root")
        if rp:
            node_root[name] = rp

    applied, skipped = [], []

    # 集群级回填：仅对节点包中确实存在的账号口令写入
    for c in query("SELECT id, ha_user, repl_user, app_user FROM clusters"):
        cid = c["id"]
        sets = {}
        for role, col in (("ha_user", "ha_pass"),
                          ("repl_user", "repl_pass"),
                          ("app_user", "app_pass")):
            u = c.get(role) or ""
            if u and cluster_creds.get(u):
                sets[col] = cluster_creds[u]
        if sets:
            try:
                update_cluster(cid, **sets)
                applied.append("集群#%s: 回填 %s" % (cid, ", ".join(sets.keys())))
            except Exception as e:
                skipped.append("集群#%s: 回填失败(%s)" % (cid, e))
        else:
            skipped.append("集群#%s: 节点包未匹配到其账号口令，跳过" % cid)

    # 节点级 root 回填（按 IP 匹配部署单元目录名）
    for n in query("SELECT ip, root_user FROM nodes"):
        ip = n["ip"]
        rp = node_root.get(ip)
        if rp:
            try:
                execute("UPDATE nodes SET root_pass=%s WHERE ip=%s",
                        (_crypto.encrypt(rp), ip))
                applied.append("节点 %s: 回填 root 口令" % ip)
            except Exception as e:
                skipped.append("节点 %s: root 回填失败(%s)" % (ip, e))
        else:
            skipped.append("节点 %s: 节点包未找到 root 口令，跳过" % ip)

    ok = bool(applied)
    msg = ("已从节点包回填 %d 项" % len(applied)
           + (f"；{len(skipped)} 项跳过" if skipped else "")
           + "。回填后以明文存储（加密默认关闭），请到「集群设置 / 节点」确认，并等待复制自愈。")
    return {"ok": ok, "msg": msg, "applied": applied, "skipped": skipped}


def reset_repl_password(cluster_id):
    """批量重置集群复制口令：生成新 repl_pass 落库，并在每个节点重置复制账号密码，
    对从库以新口令重挂复制。适用于复制口令漂移/需要定期轮换。

    ⚠ 依赖 manager 能用 ha/root 连上节点（口令须可用）；若 root 也解不开，请改用
    「从节点包回填明文」或手动重置。

    返回 {ok, msg, applied:[...], skipped:[...]}。
    """
    from . import clusters as _clusters
    from . import ha_client as _ha
    from . import state as _state

    c = _clusters.get_cluster(cluster_id)
    if not c:
        return {"ok": False, "msg": "集群 #%s 不存在" % cluster_id, "applied": [], "skipped": []}
    creds = _clusters.get_cluster_creds(cluster_id)
    new_pass = secrets.token_urlsafe(16)

    nodes = query(
        "SELECT n.ip, n.port, n.root_user, n.root_pass "
        "FROM nodes n WHERE n.cluster_id=%s", (cluster_id,))

    # 当前主库（用于从库重挂）；current_master 缺失时按 read_only 探测可写节点兜底
    cm = _state.get_current_master(cluster_id)
    master_ip = cm[0]["host"] if cm else None
    master_port = cm[0]["port"] if cm else None
    if not master_ip and nodes:
        for n in nodes:
            try:
                ru, rp = (n.get("root_user") or "root"), (n.get("root_pass") or "")
                conn = _ha.connect_robust(n["ip"], n["port"], creds["ha_user"], creds["ha_pass"],
                                          ru, rp, timeout=5)
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT @@read_only")
                    ro = cur.fetchone()
                    conn.close()
                    if ro and int(ro[0]) == 0:
                        master_ip, master_port = n["ip"], n["port"]
                        break
            except Exception:
                pass

    # 先落库新口令（确保 make_slave_of 用新密码重挂）
    _clusters.update_cluster(cluster_id, repl_pass=new_pass)

    applied, skipped = [], []
    for n in nodes:
        ip, port = n["ip"], n["port"]
        ru = n.get("root_user") or "root"
        rp = n.get("root_pass") or ""
        # 1) 该节点重置复制账号密码（任意节点成为主库时口令一致）
        ok, msg = _ha.reset_replication_creds_on_master(
            ip, port, creds["ha_user"], creds["ha_pass"], ru, rp,
            creds["repl_user"], new_pass)
        if ok:
            applied.append("节点 %s:%s: 重置复制账号密码" % (ip, port))
        else:
            skipped.append("节点 %s:%s: 重置失败(%s)" % (ip, port, msg))
        # 2) 从库以新口令重挂复制（主库本身无需）
        if master_ip and not (str(ip) == str(master_ip) and int(port) == int(master_port or 0)):
            ok2, msg2 = _ha.make_slave_of(
                ip, port, creds["ha_user"], creds["ha_pass"],
                master_ip, master_port, creds["repl_user"], new_pass, ru, rp)
            if ok2:
                applied.append("节点 %s:%s: 以新口令重挂复制" % (ip, port))
            else:
                skipped.append("节点 %s:%s: 重挂复制失败(%s)" % (ip, port, msg2))

    ok = bool(applied) and len(skipped) < max(1, len(nodes))
    msg = ("已为集群 #%s 重置复制口令：%d 项成功"
           % (cluster_id, len(applied))
           + (f"，%d 项跳过" % len(skipped) if skipped else "")
           + ("（已落库新口令；从库将以新口令重挂）" if master_ip else
              "（未探测到主库，从库未自动重挂，请手动重指向）"))
    return {"ok": ok, "msg": msg, "applied": applied, "skipped": skipped}


def startup_credential_check():
    """启动自检：存在「解不开」的凭证必须**立刻**暴露到告警中心与启动日志。

    为什么必须在启动时做：这类故障原本是**完全静默**的 —— 管理库读出来的是密文，
    上层拿到就当口令用，只有等到复制建立时把密文拼进 CHANGE MASTER 报
    ``1470 - String 'ENC1:...' is too long for MASTER_PASSWORD`` 才会暴露，
    届时现象是「复制起不来 / 源库不可达」，与「密钥不匹配」毫无字面关联，排查成本极高。

    注意：这里直接写 alerts 表而**不用** alerts.send_alert —— 后者是邮件通道，
    未配置 SMTP 时只会 print，无法保证在 Web 界面可见。
    """
    try:
        h = credential_health()
    except Exception as e:
        print(f"[init_db] 凭证自检跳过: {e}")
        return 0
    broken = [i for i in h.get("items", []) if i.get("status") == "broken"]
    if not broken:
        return 0

    print("=" * 62, flush=True)
    print("[严重] 检测到 %d 项凭证已加密但当前密钥解不开：" % len(broken), flush=True)
    for i in broken:
        print("        - %s %s · %s" % (i["scope"], i["name"], i["column"]), flush=True)
    print("  这些口令无法自动恢复，必须到「集群设置 / 节点」重新录入；", flush=True)
    print("  否则复制会因口令为 ENC1 密文而报 1470 无法建立。", flush=True)
    print("  建议：设置 → 凭证加密 →「关闭加密（保底）」后重录上述口令。", flush=True)
    print("=" * 62, flush=True)

    n = 0
    by_cluster = {}
    for i in broken:
        by_cluster.setdefault(i.get("cluster_id"), []).append(i)
    for cid, items in by_cluster.items():
        names = "、".join("%s %s·%s" % (x["scope"], x["name"], x["column"]) for x in items)
        msg = (f"【凭证密钥不匹配】{len(items)} 项凭证已加密但当前密钥解不开（{names}）。"
               f"密文无法自动恢复，必须到「集群设置 / 节点」重新录入；否则复制会因复制口令为 "
               f"ENC1 密文而报 1470 无法建立。建议：设置 → 凭证加密 →「关闭加密（保底）」后重录。")
        try:
            # 24 小时内同内容不重复写，避免每次重启刷屏
            old = query("SELECT id FROM alerts WHERE alert_type='cred_key' AND message=%s "
                        "AND created_at > NOW() - INTERVAL 24 HOUR LIMIT 1", (msg,))
            if old:
                continue
            execute("INSERT INTO alerts(cluster_id,host,port,alert_type,status,message,"
                    "created_at) VALUES(%s,NULL,NULL,'cred_key','open',%s,NOW())",
                    (cid, msg))
            n += 1
        except Exception as e:
            print(f"[init_db] 写入凭证告警失败(可忽略): {e}")
    return n


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
